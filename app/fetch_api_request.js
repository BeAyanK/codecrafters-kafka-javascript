import fs from "fs";
import path from "path";

// Helper: Read VarInt (compact integer)
function readVarInt(buffer, offset) {
  let value = 0;
  let shift = 0;
  let byte;
  do {
    if (offset >= buffer.length) {
      // This error indicates malformed input buffer, should not happen in valid Kafka protocol
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

// Flag to ensure metadata loading happens only once
let metadataLoaded = false;

// Function to load topic metadata from partition.metadata files
function loadTopicMetadata(logDir) {
  try {
    // Attempt to list directories inside logDir
    const entries = fs.readdirSync(logDir, { withFileTypes: true });

    for (const entry of entries) {
      // Check if it's a directory and looks like a topic-partition folder
      // Example: "foo-0", "paz-1". Exclude internal Kafka dirs like "__cluster_metadata-0".
      const isTopicPartitionDir = entry.isDirectory() && 
                                  !entry.name.startsWith('__') && 
                                  /^[a-zA-Z0-9_-]+-\d+$/.test(entry.name); // More specific regex: topic-name-partition-id

      if (isTopicPartitionDir) {
        const partitionMetadataPath = path.join(logDir, entry.name, "partition.metadata");
        
        if (fs.existsSync(partitionMetadataPath)) {
          const content = fs.readFileSync(partitionMetadataPath, 'utf8');
          let currentTopicId = null; // Use different variable names to avoid confusion
          let currentTopicName = null;

          content.split('\n').forEach(line => {
            if (line.startsWith("topic.id=")) {
              const uuidString = line.substring("topic.id=".length);
              const rawUuidHex = uuidString.replace(/-/g, '');
              if (rawUuidHex.length === 32) {
                  currentTopicId = rawUuidHex; // Store as hex string for map key
              }
            } else if (line.startsWith("topic.name=")) {
              currentTopicName = line.substring("topic.name=".length);
            }
          });

          if (currentTopicId && currentTopicName) {
            topicIdToNameMap.set(currentTopicId, currentTopicName);
          }
        }
      }
    }
    
    // Log the outcome - CodeCrafters should pick this up
    if (topicIdToNameMap.size > 0) {
      console.error(`[your_program] Loaded topic metadata: ${JSON.stringify(Array.from(topicIdToNameMap.entries()))}`);
    } else {
      console.error(`[your_program] No topic metadata loaded from ${logDir}. This might be an issue.`);
    }
  } catch (error) {
    console.error(`[your_program] Error loading topic metadata from ${logDir}: ${error.message}`);
  }
}


export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
    // Load metadata only once
    if (!metadataLoaded) {
        // The /tmp/kraft-combined-logs path is provided by the CodeCrafters tester
        loadTopicMetadata("/tmp/kraft-combined-logs");
        metadataLoaded = true;
    }

    // Constants for response fields
    const throttleTime = Buffer.alloc(4).fill(0);
    const errorCode = Buffer.alloc(2).fill(0);
    const sessionId = Buffer.alloc(4).fill(0);
    const responseTagBuffer = Buffer.from([0]); // Empty tag buffer for response body

    // --- Start: Request Parsing ---
    let offset = 12; // Start after correlationId, at the RequestHeader TAG_BUFFER

    // 1. Read RequestHeader TAG_BUFFER (flexible versions have a tag buffer in the header)
    offset += 1; // Consume the 0x00 Tag Buffer for RequestHeader

    // 2. Read Client ID (COMPACT_NULLABLE_STRING)
    let clientIdVarIntInfo = readVarInt(buffer, offset);
    offset = clientIdVarIntInfo.offset;
    const clientIdLength = clientIdVarIntInfo.value - 1; // Actual length of client ID string
    offset += clientIdLength; // Advance past the client_id string data

    // Now, parse the FetchRequest v16 body fields from the correct offset
    offset += 4; // replica_id (int32)
    offset += 4; // max_wait_ms (int32)
    offset += 4; // min_bytes (int32)
    offset += 4; // max_bytes (int32)
    offset += 1; // isolation_level (int8)
    offset += 4; // session_id (int32)
    offset += 4; // session_epoch (int32)
    // --- End: Request Parsing ---

    // topics: COMPACT_ARRAY of FetchTopic
    // The VarInt here indicates the number of elements + 1.
    let topicArrayInfo = readVarInt(buffer, offset);
    offset = topicArrayInfo.offset;
    const numTopics = topicArrayInfo.value - 1; // Correct for the number of topics to process.

    const topicResponses = [];

    for (let i = 0; i < numTopics; i++) {
        const topicIdBuffer = buffer.subarray(offset, offset + 16);
        offset += 16; // Advance past topic_id (UUID)
        offset += 1; // Skip tag buffer for topic in request (COMPACT_ARRAY of TaggedFields)

        // Lookup topic name using the pre-loaded map
        const topicName = topicIdToNameMap.get(topicIdBuffer.toString('hex'));
        
        if (!topicName) {
            // If topic name is not found, log a warning and skip this topic in the response.
            // CodeCrafters logs will capture this if it happens.
            console.error(`[your_program] WARN: Could not find topic name for ID: ${topicIdBuffer.toString('hex')}. Skipping this topic.`);
            continue; 
        }

        // partitions: COMPACT_ARRAY of FetchPartition
        let partitionArrayInfo = readVarInt(buffer, offset);
        offset = partitionArrayInfo.offset;
        const numPartitions = partitionArrayInfo.value - 1; // Actual number of partitions to process.

        const partitionResponses = [];

        for (let j = 0; j < numPartitions; j++) {
            const partitionIndex = buffer.readInt32BE(offset); offset += 4;
            offset += 4; // current_leader_epoch (INT32)
            offset += 8; // fetch_offset (INT64)
            offset += 8; // log_start_offset (INT64)
            offset += 4; // partition_max_bytes (INT32)
            offset += 1; // partition tag buffer in request (COMPACT_ARRAY of TaggedFields)

            let recordBatchBuffer;
            try {
                // Construct the log file path using the found topicName and partitionIndex
                const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${partitionIndex}/00000000000000000000.log`;
                
                if (fs.existsSync(logFilePath)) {
                    const fileContent = fs.readFileSync(logFilePath);
                    recordBatchBuffer = writeCompactBytes(fileContent);
                } else {
                    // If log file does not exist, send an empty record batch
                    recordBatchBuffer = writeCompactBytes(Buffer.alloc(0));
                }
            } catch (error) {
                // Log any errors during file reading
                console.error(`[your_program] ERROR: Reading log file for partition ${topicName}-${partitionIndex}: ${error.message}`);
                recordBatchBuffer = writeCompactBytes(Buffer.alloc(0)); // On error, send empty record batch
            }
            
            const partitionIndexBuffer = Buffer.alloc(4);
            partitionIndexBuffer.writeInt32BE(partitionIndex);

            const partitionResponse = Buffer.concat([
                partitionIndexBuffer,
                errorCode, // error_code (INT16, 0 for NO_ERROR)
                Buffer.alloc(8).fill(0), // high_watermark (INT64) - Placeholder value (0)
                Buffer.alloc(8).fill(0), // last_stable_offset (INT64) - Placeholder value (0)
                Buffer.alloc(8).fill(0), // log_start_offset (INT64) - Placeholder value (0)
                writeVarInt(0),          // aborted_transactions (COMPACT_ARRAY, 0 elements means VarInt 1)
                Buffer.alloc(4).fill(0), // preferred_read_replica (INT32) - Placeholder value (0)
                recordBatchBuffer,       // records (COMPACT_BYTES)
                responseTagBuffer,       // partition tag buffer (COMPACT_ARRAY of TaggedFields)
            ]);
            partitionResponses.push(partitionResponse);
        }
        
        const topicResponse = Buffer.concat([
            topicIdBuffer, // Topic ID (UUID) from request
            writeVarInt(partitionResponses.length + 1), // num_partitions (COMPACT_ARRAY length)
            ...partitionResponses,
            responseTagBuffer, // Topic tag buffer
        ]);
        topicResponses.push(topicResponse);
    }

    // Response Body structure for FetchResponse v16:
    // throttle_time_ms (INT32)
    // error_code (INT16)
    // session_id (INT32)
    // responses (COMPACT_ARRAY of FetchableTopicResponse)
    // tag_buffer (COMPACT_ARRAY of TaggedFields)
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
        responseHeaderTagBuffer, // Header Tag Buffer
        responseBody,
    ]);

    const messageSizeBuffer = Buffer.alloc(4);
    messageSizeBuffer.writeInt32BE(fullResponseData.length);

    connection.write(Buffer.concat([messageSizeBuffer, fullResponseData]));
};
