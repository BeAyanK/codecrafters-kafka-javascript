import fs from "fs";
import path from "path";

// Helper functions
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

function writeVarInt(value) {
  const buffer = [];
  while ((value & 0x7f) !== value) {
    buffer.push((value & 0x7f) | 0x80);
    value >>>= 7;
  }
  buffer.push(value);
  return Buffer.from(buffer);
}

function writeCompactBytes(data) {
  const length = writeVarInt(data.length + 1); // Length + 1 for compact bytes
  return Buffer.concat([length, data]);
}

// Global map to store topicId (hex string) to topicName mapping
let topicIdToNameMap = new Map();

// Flag to ensure metadata loading happens only once
let metadataLoaded = false;

// Function to load topic metadata from partition.metadata files
function loadTopicMetadata(logDir) {
  if (metadataLoaded) return; // Prevent re-loading

  try {
    const entries = fs.readdirSync(logDir, { withFileTypes: true });
    console.error(`[your_program] DEBUG: Found ${entries.length} entries in ${logDir}`);

    for (const entry of entries) {
      // Filter for directories that likely contain topic-partition data
      // e.g., "foo-0", "paz-1". Exclude internal Kafka dirs like "__cluster_metadata-0".
      // A more robust check for topic-partition directories: must not start with '__'
      // and must contain a hyphen for topic-name-partition-id format.
      // We are *not* extracting topicName from 'entry.name' here, but from 'partition.metadata'
      if (entry.isDirectory() && !entry.name.startsWith('__') && entry.name.includes('-')) {
        const partitionMetadataPath = path.join(logDir, entry.name, "partition.metadata");
        
        if (fs.existsSync(partitionMetadataPath)) {
          const content = fs.readFileSync(partitionMetadataPath, 'utf8');
          let topicIdFromMetadata = null;
          let topicNameFromMetadata = null;

          // Parse content line by line
          content.split(/\r?\n/).forEach(line => { // Use regex for robust newline splitting
            const trimmedLine = line.trim(); // Trim whitespace from lines
            // FIX: Corrected "topic_id=" to "topic.id="
            if (trimmedLine.startsWith("topic.id=")) {
              const uuidString = trimmedLine.substring("topic.id=".length);
              const rawUuidHex = uuidString.replace(/-/g, '').toLowerCase(); // Ensure lowercase for consistent hex string comparison
              if (rawUuidHex.length === 32) { // UUID is 16 bytes = 32 hex characters
                  topicIdFromMetadata = rawUuidHex;
              }
            } else if (trimmedLine.startsWith("topic.name=")) { // This was already correct
              topicNameFromMetadata = trimmedLine.substring("topic.name=".length);
            }
          });

          // Only map if both ID and Name were found in the file
          if (topicIdFromMetadata && topicNameFromMetadata) {
            topicIdToNameMap.set(topicIdFromMetadata, topicNameFromMetadata);
            console.error(`[your_program] DEBUG: Loaded topic mapping: ${topicIdFromMetadata} -> ${topicNameFromMetadata}`);
          }
        }
      }
    }
    
    metadataLoaded = true; // Set flag that metadata has been loaded (even if empty)

    // Log the outcome for debugging on CodeCrafters platform
    if (topicIdToNameMap.size > 0) {
      console.error(`[your_program] Final loaded topic metadata: ${JSON.stringify(Array.from(topicIdToNameMap.entries()))}`);
    } else {
      console.error(`[your_program] WARNING: No topic metadata loaded from ${logDir}. This is crucial!`);
    }
  } catch (error) {
    console.error(`[your_program] ERROR: Failed to load topic metadata from ${logDir}: ${error.message}`);
    // Ensure metadataLoaded is true even on error to prevent repeated attempts
    metadataLoaded = true; 
  }
}


export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
    console.error(`[your_program] DEBUG: handleFetchApiRequest called`);
    
    // Ensure metadata is loaded exactly once at the beginning of the first request handling
    if (!metadataLoaded) {
        loadTopicMetadata("/tmp/kraft-combined-logs");
    }

    // Constants for response fields
    const throttleTime = Buffer.alloc(4).fill(0);
    const errorCode = Buffer.alloc(2).fill(0);
    const sessionId = Buffer.alloc(4).fill(0);
    const responseTagBuffer = Buffer.from([0]); // Empty tag buffer for response body

    // Parse request - Fetch API v16
    // Message Size (4) + API Key (2) + API Version (2) + Correlation ID (4) = 12 bytes
    let offset = 12; 
    
    // Read Client ID (compact string: VarInt length + bytes).
    // The VarInt value itself determines length: 0 means null, >0 means (length + 1).
    let clientIdLengthVarIntInfo = readVarInt(buffer, offset);
    offset = clientIdLengthVarIntInfo.offset;
    const clientIdStringLength = clientIdLengthVarIntInfo.value === 0 ? -1 : clientIdLengthVarIntInfo.value - 1; // -1 for null, 0 for empty string, etc.
    
    console.error(`[your_program] DEBUG: Client ID VarInt value: ${clientIdLengthVarIntInfo.value}, Decoded length: ${clientIdStringLength}, Current offset: ${offset}`);
    
    if (clientIdStringLength >= 0) { // If it's not a null string (-1 decoded length)
        offset += clientIdStringLength; // Advance past the actual string bytes
    }
    
    // Read RequestHeader TAG_BUFFER (flexible versions have a tag buffer in the header). Should be 0 for this stage.
    const headerTagBuffer = buffer.readUInt8(offset++); 
    console.error(`[your_program] DEBUG: Header TAG_BUFFER: ${headerTagBuffer}, New offset: ${offset}`);
    
    // Read request body fields
    const replicaId = buffer.readInt32BE(offset); offset += 4;
    const maxWaitMs = buffer.readInt32BE(offset); offset += 4;
    const minBytes = buffer.readInt32BE(offset); offset += 4;
    const maxBytes = buffer.readInt32BE(offset); offset += 4;
    const isolationLevel = buffer.readUInt8(offset); offset += 1;
    const sessionIdFromReq = buffer.readInt32BE(offset); offset += 4;
    const sessionEpoch = buffer.readInt32BE(offset); offset += 4;
    
    console.error(`[your_program] DEBUG: After fixed request body fields, offset: ${offset}`);
    console.error(`[your_program] DEBUG: replicaId: ${replicaId}, maxWaitMs: ${maxWaitMs}, minBytes: ${minBytes}, maxBytes: ${maxBytes}, isolationLevel: ${isolationLevel}`);
    
    // Read topics array (COMPACT_ARRAY of FetchTopic)
    let topicArrayInfo = readVarInt(buffer, offset);
    offset = topicArrayInfo.offset;
    const numTopicsInRequest = topicArrayInfo.value > 0 ? topicArrayInfo.value - 1 : 0; // Value 0 means null (no topics), 1 means 0 topics, etc.

    console.error(`[your_program] DEBUG: VarInt value for topics array: ${topicArrayInfo.value}, Decoded number of topics in request: ${numTopicsInRequest}, Current offset: ${offset}`);
    
    const topicResponses = [];

    // Process topics from request
    if (numTopicsInRequest > 0) {
        for (let i = 0; i < numTopicsInRequest; i++) {
            const topicIdBuffer = buffer.subarray(offset, offset + 16);
            offset += 16; // Advance past topic_id (UUID)
            offset += 1; // Skip tag buffer for topic in request (COMPACT_ARRAY of TaggedFields)

            // Lookup topic name using the pre-loaded map. Convert UUID buffer to lowercase hex string.
            const topicIdHex = topicIdBuffer.toString('hex').toLowerCase();
            const topicName = topicIdToNameMap.get(topicIdHex);
            
            if (!topicName) {
                console.error(`[your_program] WARN: Could not find topic name for ID: ${topicIdHex}. Skipping this topic in response.`);
                continue; // Skip processing this topic if name not found
            }

            console.error(`[your_program] DEBUG: Processing requested topic ${topicIdHex} -> ${topicName}`);
            
            // partitions: COMPACT_ARRAY of FetchPartition
            let partitionArrayInfo = readVarInt(buffer, offset);
            offset = partitionArrayInfo.offset;
            const numPartitionsInRequest = partitionArrayInfo.value > 0 ? partitionArrayInfo.value - 1 : 0;
            
            console.error(`[your_program] DEBUG: Number of partitions for topic ${topicName}: ${numPartitionsInRequest}`);
            
            const partitionResponses = [];
            for (let j = 0; j < numPartitionsInRequest; j++) {
                const partitionIndex = buffer.readInt32BE(offset); offset += 4;
                // These are request fields. You should extract them but not use them directly as response values.
                offset += 4; // current_leader_epoch (INT32)
                offset += 8; // fetch_offset (INT64)
                offset += 8; // log_start_offset (INT64)
                offset += 4; // partition_max_bytes (INT32)
                offset += 1; // partition tag buffer in request (COMPACT_ARRAY of TaggedFields)

                console.error(`[your_program] DEBUG: Processing partition ${partitionIndex} for topic ${topicName}`);
                
                let recordBatchBuffer;
                try {
                    const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${partitionIndex}/00000000000000000000.log`;
                    
                    if (fs.existsSync(logFilePath)) {
                        const fileContent = fs.readFileSync(logFilePath);
                        recordBatchBuffer = writeCompactBytes(fileContent);
                        console.error(`[your_program] DEBUG: Read ${fileContent.length} bytes from ${logFilePath}`);
                    } else {
                        console.error(`[your_program] WARNING: Log file not found at ${logFilePath}. Sending empty record batch.`);
                        recordBatchBuffer = writeCompactBytes(Buffer.alloc(0)); // Empty record batch
                    }
                } catch (error) {
                    console.error(`[your_program] ERROR: Reading log file for partition ${topicName}-${partitionIndex}: ${error.message}. Sending empty record batch.`);
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
                    writeVarInt(1),          // aborted_transactions (COMPACT_ARRAY, 0 elements means VarInt 1) - Empty array
                    Buffer.alloc(4).fill(0), // preferred_read_replica (INT32) - Placeholder value (0)
                    recordBatchBuffer,       // records (COMPACT_BYTES)
                    responseTagBuffer,       // partition tag buffer (COMPACT_ARRAY of TaggedFields)
                ]);
                partitionResponses.push(partitionResponse);
            }
            
            const topicResponse = Buffer.concat([
                topicIdBuffer, // Use the original topic ID from request
                writeVarInt(partitionResponses.length + 1), // num_partitions (COMPACT_ARRAY length)
                ...partitionResponses,
                responseTagBuffer, // Topic tag buffer
            ]);
            topicResponses.push(topicResponse);
        }
    } else {
        // This block should ideally not be hit for this stage's test case,
        // as the test explicitly requests topics.
        // If it is hit, it means there's an issue with parsing the request topics.
        console.error(`[your_program] DEBUG: No topics explicitly requested. Returning an empty topic list as per typical Kafka behavior.`);
        // Kafka generally returns an empty topics array if none are explicitly requested,
        // unless it's a very specific all-topics fetch which is not standard.
        // For this stage, an empty response will lead to failure.
        // So, if you reach here, the request parsing is the problem.
    }

    // Build response body:
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

    console.error(`[your_program] DEBUG: Sending response with ${fullResponseData.length} bytes`);
    connection.write(Buffer.concat([messageSizeBuffer, fullResponseData]));
};
