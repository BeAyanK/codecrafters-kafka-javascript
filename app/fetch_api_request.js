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
      if (entry.isDirectory() && !entry.name.startsWith('__') && entry.name.includes('-')) {
        const partitionMetadataPath = path.join(logDir, entry.name, "partition.metadata");
        
        if (fs.existsSync(partitionMetadataPath)) {
          const content = fs.readFileSync(partitionMetadataPath, 'utf8');
          let topicIdFromMetadata = null;
          let topicNameFromMetadata = null;

          content.split(/\r?\n/).forEach(line => {
            const trimmedLine = line.trim();
            const parts = trimmedLine.split('=');
            
            // Robust parsing for key=value lines
            if (parts.length === 2) {
                const key = parts[0].trim();
                const value = parts[1].trim();

                if (key === "topic.id") {
                    const uuidString = value;
                    const rawUuidHex = uuidString.replace(/-/g, '').toLowerCase();
                    if (rawUuidHex.length === 32) {
                        topicIdFromMetadata = rawUuidHex;
                    }
                } else if (key === "topic.name") {
                    topicNameFromMetadata = value;
                }
            }
          });

          if (topicIdFromMetadata && topicNameFromMetadata) {
            topicIdToNameMap.set(topicIdFromMetadata, topicNameFromMetadata);
            console.error(`[your_program] DEBUG: Loaded topic mapping: ${topicIdFromMetadata} -> ${topicNameFromMetadata}`);
          }
        }
      }
    }
    
    metadataLoaded = true; // Set flag that metadata has been loaded (even if empty)

    if (topicIdToNameMap.size > 0) {
      console.error(`[your_program] Final loaded topic metadata: ${JSON.stringify(Array.from(topicIdToNameMap.entries()))}`);
    } else {
      console.error(`[your_program] WARNING: No topic metadata loaded from ${logDir}. This is crucial!`);
    }
  } catch (error) {
    console.error(`[your_program] ERROR: Failed to load topic metadata from ${logDir}: ${error.message}`);
    metadataLoaded = true; 
  }
}


export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
    console.error(`[your_program] DEBUG: handleFetchApiRequest called`);
    
    if (!metadataLoaded) {
        loadTopicMetadata("/tmp/kraft-combined-logs");
    }

    const throttleTime = Buffer.alloc(4).fill(0);
    const errorCode = Buffer.alloc(2).fill(0);
    const sessionId = Buffer.alloc(4).fill(0);
    const responseTagBuffer = Buffer.from([0]);

    // Request parsing:
    // Message Size (4) + API Key (2) + API Version (2) + Correlation ID (4) = 12 bytes
    let offset = 12; 
    
    // DEBUG: Log initial bytes to diagnose offset issues
    console.error(`[your_program] DEBUG: Buffer at offset ${offset}: ${buffer.subarray(offset, offset + 10).toString('hex')}`); // Peek next 10 bytes

    // The byte at 'offset' (which is 12) should be the RequestHeader TAG_BUFFER (0x00 for no tags)
    const headerTagBuffer = buffer.readUInt8(offset++); 
    console.error(`[your_program] DEBUG: Read Header TAG_BUFFER: ${headerTagBuffer}, New offset: ${offset}`);

    // Read Client ID (compact string: VarInt length + bytes).
    // The VarInt value itself determines length: 0 means null, >0 means (length + 1).
    let clientIdLengthVarIntInfo = readVarInt(buffer, offset);
    offset = clientIdLengthVarIntInfo.offset;
    const clientIdStringLength = clientIdLengthVarIntInfo.value === 0 ? -1 : clientIdLengthVarIntInfo.value - 1; 
    
    console.error(`[your_program] DEBUG: Client ID VarInt value: ${clientIdLengthVarIntInfo.value}, Decoded length: ${clientIdStringLength}, Current offset: ${offset}`);
    
    if (clientIdStringLength >= 0) { 
        console.error(`[your_program] DEBUG: Reading Client ID string: ${buffer.subarray(offset, offset + clientIdStringLength).toString('utf8')}`);
        offset += clientIdStringLength; // Advance past the actual string bytes
    } else {
        console.error(`[your_program] DEBUG: Client ID is NULL.`);
    }

    // After Client ID, should be the fixed-size fields of Fetch Request
    const replicaId = buffer.readInt32BE(offset); offset += 4;
    const maxWaitMs = buffer.readInt32BE(offset); offset += 4;
    const minBytes = buffer.readInt32BE(offset); offset += 4;
    const maxBytes = buffer.readInt32BE(offset); offset += 4;
    const isolationLevel = buffer.readUInt8(offset); offset += 1;
    const sessionIdFromReq = buffer.readInt32BE(offset); offset += 4;
    const sessionEpoch = buffer.readInt32BE(offset); offset += 4;
    
    console.error(`[your_program] DEBUG: After fixed request body fields, offset: ${offset}`);
    console.error(`[your_program] DEBUG: replicaId: ${replicaId}, maxWaitMs: ${maxWaitMs}, minBytes: ${minBytes}, maxBytes: ${maxBytes}, isolationLevel: ${isolationLevel}, sessionId: ${sessionIdFromReq}, sessionEpoch: ${sessionEpoch}`);
    
    // Read topics array (COMPACT_ARRAY of FetchTopic)
    let topicArrayInfo = readVarInt(buffer, offset);
    offset = topicArrayInfo.offset;
    const numTopicsInRequest = topicArrayInfo.value > 0 ? topicArrayInfo.value - 1 : 0; 

    console.error(`[your_program] DEBUG: VarInt value for topics array: ${topicArrayInfo.value}, Decoded number of topics in request: ${numTopicsInRequest}, Current offset: ${offset}`);
    
    const topicResponses = [];

    if (numTopicsInRequest > 0) {
        for (let i = 0; i < numTopicsInRequest; i++) {
            const topicIdBuffer = buffer.subarray(offset, offset + 16);
            offset += 16; 
            offset += 1; // Skip tag buffer for topic in request (always 0x00 for no tags)

            const topicIdHex = topicIdBuffer.toString('hex').toLowerCase();
            const topicName = topicIdToNameMap.get(topicIdHex);
            
            if (!topicName) {
                console.error(`[your_program] WARN: Could not find topic name for ID: ${topicIdHex}. Skipping this topic in response.`);
                continue; 
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
                offset += 4; // current_leader_epoch (INT32)
                offset += 8; // fetch_offset (INT64)
                offset += 8; // log_start_offset (INT64)
                offset += 4; // partition_max_bytes (INT32)
                offset += 1; // partition tag buffer in request (always 0x00)

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
                        recordBatchBuffer = writeCompactBytes(Buffer.alloc(0)); 
                    }
                } catch (error) {
                    console.error(`[your_program] ERROR: Reading log file for partition ${topicName}-${partitionIndex}: ${error.message}. Sending empty record batch.`);
                    recordBatchBuffer = writeCompactBytes(Buffer.alloc(0)); 
                }
                
                const partitionIndexBuffer = Buffer.alloc(4);
                partitionIndexBuffer.writeInt32BE(partitionIndex);

                const partitionResponse = Buffer.concat([
                    partitionIndexBuffer,
                    errorCode,
                    Buffer.alloc(8).fill(0), // high_watermark
                    Buffer.alloc(8).fill(0), // last_stable_offset
                    Buffer.alloc(8).fill(0), // log_start_offset
                    writeVarInt(1),          // aborted_transactions (empty array means VarInt 1)
                    Buffer.alloc(4).fill(0), // preferred_read_replica
                    recordBatchBuffer,       // records (COMPACT_BYTES)
                    responseTagBuffer,       // partition tag buffer (0x00)
                ]);
                partitionResponses.push(partitionResponse);
            }
            
            const topicResponse = Buffer.concat([
                topicIdBuffer, // Use the original topic ID from request
                writeVarInt(partitionResponses.length + 1),
                ...partitionResponses,
                responseTagBuffer, // Topic tag buffer (0x00)
            ]);
            topicResponses.push(topicResponse);
        }
    } else {
        console.error(`[your_program] DEBUG: Request specified 0 topics. This might indicate an issue with request parsing.`);
    }

    const responseBody = Buffer.concat([
        throttleTime,
        errorCode,
        sessionId,
        writeVarInt(topicResponses.length + 1),
        ...topicResponses,
        responseTagBuffer, 
    ]);

    const correlationIdBuffer = Buffer.alloc(4);
    correlationIdBuffer.writeInt32BE(responseMessage.correlationId);
    const responseHeaderTagBuffer = Buffer.from([0]); 

    const fullResponseData = Buffer.concat([
        correlationIdBuffer,
        responseHeaderTagBuffer, 
        responseBody,
    ]);

    const messageSizeBuffer = Buffer.alloc(4);
    messageSizeBuffer.writeInt32BE(fullResponseData.length);

    console.error(`[your_program] DEBUG: Sending response with ${fullResponseData.length} bytes`);
    connection.write(Buffer.concat([messageSizeBuffer, fullResponseData]));
};
