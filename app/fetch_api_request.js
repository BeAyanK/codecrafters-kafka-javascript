import fs from "fs";
import path from "path";

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
let topicIdToNameMap = new Map();

// Flag to ensure metadata loading happens only once
let metadataLoaded = false;

// Function to load topic metadata from partition.metadata files
function loadTopicMetadata(logDir) {
  // Only attempt to load if not already loaded
  if (metadataLoaded) {
    return;
  }

  try {
    const entries = fs.readdirSync(logDir, { withFileTypes: true });

    for (const entry of entries) {
      // Filter for directories that likely contain topic-partition data
      // We are looking for directories like "foo-0", "paz-1", etc.
      // Exclude special Kafka internal directories that start with "__"
      if (entry.isDirectory() && !entry.name.startsWith('__')) {
        const partitionMetadataPath = path.join(logDir, entry.name, "partition.metadata");
        
        if (fs.existsSync(partitionMetadataPath)) {
          const content = fs.readFileSync(partitionMetadataPath, 'utf8');
          let currentTopicId = null;
          let currentTopicName = null;

          // Parse content line by line
          content.split(/\r?\n/).forEach(line => { // Use regex for robust newline splitting
            const trimmedLine = line.trim(); // Trim whitespace from lines
            if (trimmedLine.startsWith("topic.id=")) {
              const uuidString = trimmedLine.substring("topic.id=".length);
              const rawUuidHex = uuidString.replace(/-/g, '').toLowerCase(); // Ensure lowercase for consistent hex string comparison
              if (rawUuidHex.length === 32) {
                  currentTopicId = rawUuidHex;
              }
            } else if (trimmedLine.startsWith("topic.name=")) {
              currentTopicName = trimmedLine.substring("topic.name=".length);
            }
          });

          if (currentTopicId && currentTopicName) {
            topicIdToNameMap.set(currentTopicId, currentTopicName);
          }
        }
      }
    }
    
    // Set flag that metadata has been loaded (even if empty)
    metadataLoaded = true; 

    // Log the outcome for debugging on CodeCrafters platform
    if (topicIdToNameMap.size > 0) {
      // Use console.error as it might be more visible in CodeCrafters logs
      console.error(`[your_program] Loaded topic metadata: ${JSON.stringify(Array.from(topicIdToNameMap.entries()))}`);
    } else {
      console.error(`[your_program] WARNING: No topic metadata loaded from ${logDir}.`);
    }
  } catch (error) {
    console.error(`[your_program] ERROR: Failed to load topic metadata from ${logDir}: ${error.message}`);
    // Ensure metadataLoaded is true even on error to prevent repeated attempts
    metadataLoaded = true; 
  }
}


export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
    // Ensure metadata is loaded exactly once
    if (!metadataLoaded) {
        loadTopicMetadata("/tmp/kraft-combined-logs");
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

        // Lookup topic name using the pre-loaded map. Convert UUID buffer to lowercase hex string.
        const topicIdHex = topicIdBuffer.toString('hex').toLowerCase();
        const topicName = topicIdToNameMap.get(topicIdHex);
        
        if (!topicName) {
            // Log a warning and skip this topic if its name can't be found
            console.error(`[your_program] WARN: Could not find topic name for ID: ${topicIdHex}. Skipping this topic.`);
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
