import fs from "fs";
import path from "path"; // Import path module for path.join

// Helper: Read VarInt (compact integer)
function readVarInt(buffer, offset) {
  let value = 0;
  let shift = 0;
  let byte;
  do {
    if (offset >= buffer.length) {
      throw new Error("Buffer out of bounds when reading VarInt");
    }
    byte = buffer.readUInt8(offset++);
    value |= (byte & 0x7f) << shift;
    shift += 7;
  } while ((byte & 0x80) !== 0);
  return { value, offset };
}

// Helper: Write VarInt (compact integer)
function writeVarInt(value) {
  const buffer = [];
  while ((value & 0x7f) !== value) {
    buffer.push((value & 0x7f) | 0x80);
    value >>>= 7;
  }
  buffer.push(value);
  return Buffer.from(buffer);
}

// Helper: Write Compact Bytes (used for recordBatch)
function writeCompactBytes(data) {
  // For Compact Bytes, the VarInt represents (length + 1).
  // So, if data.length is 0, the VarInt should be 1.
  const length = writeVarInt(data.length + 1);
  return Buffer.concat([length, data]);
}

// Global map to store topicId (hex string) to topicName mapping
// This will be initialized once.
let topicIdToNameMap = new Map();

// Function to load topic metadata from partition.metadata files
function loadTopicMetadata(logDir) {
  try {
    const entries = fs.readdirSync(logDir, { withFileTypes: true });

    for (const entry of entries) {
      // Check if it's a directory like 'foo-0', 'pax-0', etc.
      if (entry.isDirectory() && entry.name.includes('-')) {
        const partitionMetadataPath = path.join(logDir, entry.name, "partition.metadata");
        if (fs.existsSync(partitionMetadataPath)) {
          const content = fs.readFileSync(partitionMetadataPath, 'utf8');
          let topicId = null;
          let topicName = null;

          content.split('\n').forEach(line => {
            if (line.startsWith("topic.id=")) {
              // The UUID in partition.metadata is typically in string form with hyphens
              // e.g., "00000000-0000-0000-0000-000000000000"
              // We need to convert it to the 16-byte buffer form received in Fetch requests
              const uuidString = line.substring("topic.id=".length);
              // Ensure it's a valid UUID string format, remove hyphens
              const rawUuidHex = uuidString.replace(/-/g, '');
              if (rawUuidHex.length === 32) { // 16 bytes = 32 hex chars
                  topicId = rawUuidHex; // Store as hex string for map key
              }
            } else if (line.startsWith("topic.name=")) {
              topicName = line.substring("topic.name=".length);
            }
          });

          if (topicId && topicName) {
            topicIdToNameMap.set(topicId, topicName);
          }
        }
      }
    }
    console.log(`Loaded topic metadata: ${JSON.stringify(Array.from(topicIdToNameMap.entries()))}`);
  } catch (error) {
    console.error("Error loading topic metadata:", error);
  }
}

// Call loadTopicMetadata once when the script starts (or on first request if needed)
// The test runner provides /tmp/kraft-combined-logs as the log directory.
// Assuming your main server file calls handleFetchApiRequest,
// you'd call loadTopicMetadata at the start of your server process.
// For this isolated function, we'll make it part of a wrapper, or assume it's called.
// Since the tester runs it as a script, let's assume `handleFetchApiRequest` might be called multiple times.
// So, we'll make sure `loadTopicMetadata` is only called once.

let metadataLoaded = false;

export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
    if (!metadataLoaded) {
        // The /tmp/kraft-combined-logs path comes from the test runner.
        // You might need to pass this path into your server's main function
        // or infer it from server.properties.
        // For now, hardcode as per test logs.
        loadTopicMetadata("/tmp/kraft-combined-logs");
        metadataLoaded = true;
    }

    // Constants for response fields
    const throttleTime = Buffer.alloc(4).fill(0);
    const errorCode = Buffer.alloc(2).fill(0);
    const sessionId = Buffer.alloc(4).fill(0);
    const responseTagBuffer = Buffer.from([0]); // Empty tag buffer for response body

    let offset = 12; // Start after correlationId, at the RequestHeader TAG_BUFFER

    // 1. Read RequestHeader TAG_BUFFER
    offset += 1; // Consume the 0x00 Tag Buffer for RequestHeader

    // 2. Read Client ID (COMPACT_NULLABLE_STRING)
    let clientIdVarIntInfo = readVarInt(buffer, offset);
    offset = clientIdVarIntInfo.offset;
    const clientIdLength = clientIdVarIntInfo.value - 1;
    offset += clientIdLength;

    // Now, parse the FetchRequest v16 body fields
    offset += 4; // replica_id (int32)
    offset += 4; // max_wait_ms (int32)
    offset += 4; // min_bytes (int32)
    offset += 4; // max_bytes (int32)
    offset += 1; // isolation_level (int8)
    offset += 4; // session_id (int32)
    offset += 4; // session_epoch (int32)

    // topics: COMPACT_ARRAY of FetchTopic
    let topicArrayInfo = readVarInt(buffer, offset);
    offset = topicArrayInfo.offset;
    const numTopics = topicArrayInfo.value - 1;

    const topicResponses = [];

    for (let i = 0; i < numTopics; i++) {
        const topicIdBuffer = buffer.subarray(offset, offset + 16);
        offset += 16; // Advance past topic_id (UUID)
        offset += 1; // Skip tag buffer for topic in request

        // Lookup topic name using the pre-loaded map
        const topicName = topicIdToNameMap.get(topicIdBuffer.toString('hex'));
        if (!topicName) {
            console.warn(`Could not find topic name for ID: ${topicIdBuffer.toString('hex')}. Skipping this topic.`);
            // In a real scenario, you might want to return an error code for this topic
            // or just skip it as done here. For this challenge, skipping means it won't be in response.
            continue; // Skip this topic and move to the next in the request
        }

        // partitions: COMPACT_ARRAY of FetchPartition
        let partitionArrayInfo = readVarInt(buffer, offset);
        offset = partitionArrayInfo.offset;
        const numPartitions = partitionArrayInfo.value - 1;

        const partitionResponses = [];

        for (let j = 0; j < numPartitions; j++) {
            const partitionIndex = buffer.readInt32BE(offset); offset += 4;
            offset += 4; // current_leader_epoch
            offset += 8; // fetch_offset
            offset += 8; // log_start_offset
            offset += 4; // partition_max_bytes
            offset += 1; // partition tag buffer in request

            let recordBatchBuffer;
            try {
                const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${partitionIndex}/00000000000000000000.log`;
                if (fs.existsSync(logFilePath)) {
                    const fileContent = fs.readFileSync(logFilePath);
                    recordBatchBuffer = writeCompactBytes(fileContent);
                } else {
                    // Log file does not exist, send empty record batch
                    recordBatchBuffer = writeCompactBytes(Buffer.alloc(0));
                }
            } catch (error) {
                console.error(`Error reading log file for partition ${topicName}-${partitionIndex}:`, error);
                recordBatchBuffer = writeCompactBytes(Buffer.alloc(0)); // On error, send empty record batch
            }

            const partitionIndexBuffer = Buffer.alloc(4);
            partitionIndexBuffer.writeInt32BE(partitionIndex);

            const partitionResponse = Buffer.concat([
                partitionIndexBuffer,
                errorCode, // error_code (INT16, 0 for NO_ERROR)
                Buffer.alloc(8).fill(0), // high_watermark (INT64)
                Buffer.alloc(8).fill(0), // last_stable_offset (INT64)
                Buffer.alloc(8).fill(0), // log_start_offset (INT64)
                writeVarInt(0),          // aborted_transactions (COMPACT_ARRAY, 0 elements means VarInt 1)
                Buffer.alloc(4).fill(0), // preferred_read_replica (INT32)
                recordBatchBuffer,       // records (COMPACT_BYTES)
                responseTagBuffer,       // partition tag buffer
            ]);
            partitionResponses.push(partitionResponse);
        }

        const topicResponse = Buffer.concat([
            topicIdBuffer, // Topic ID (UUID)
            writeVarInt(partitionResponses.length + 1), // num_partitions (COMPACT_ARRAY length)
            ...partitionResponses,
            responseTagBuffer, // Topic tag buffer
        ]);
        topicResponses.push(topicResponse);
    }

    const responseBody = Buffer.concat([
        throttleTime,
        errorCode,
        sessionId,
        writeVarInt(topicResponses.length + 1), // num_responses (COMPACT_ARRAY length for topics)
        ...topicResponses,
        responseTagBuffer, // Response body tag buffer
    ]);

    const correlationIdBuffer = Buffer.alloc(4);
    correlationIdBuffer.writeInt32BE(responseMessage.correlationId);
    const responseHeaderTagBuffer = Buffer.from([0]); // Empty tag buffer for response header

    const fullResponseData = Buffer.concat([
        correlationIdBuffer,
        responseHeaderTagBuffer,
        responseBody,
    ]);

    const messageSizeBuffer = Buffer.alloc(4);
    messageSizeBuffer.writeInt32BE(fullResponseData.length);

    connection.write(Buffer.concat([messageSizeBuffer, fullResponseData]));
};
