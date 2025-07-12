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
  const length = writeVarInt(data.length + 1);
  return Buffer.concat([length, data]);
}

// Global map to store topicId (hex string) to topicName mapping
let topicIdToNameMap = new Map();

// Flag to ensure metadata loading happens only once
let metadataLoaded = false;

// Function to load topic metadata from partition.metadata files
function loadTopicMetadata(logDir) {
  if (metadataLoaded) return;

  try {
    const entries = fs.readdirSync(logDir, { withFileTypes: true });
    console.error(`[your_program] DEBUG: Found ${entries.length} entries in ${logDir}`);

    for (const entry of entries) {
      if (entry.isDirectory() && !entry.name.startsWith('__')) {
        const [topicName, partition] = entry.name.split('-');
        if (!topicName || partition === undefined) continue;

        const partitionMetadataPath = path.join(logDir, entry.name, "partition.metadata");
        if (fs.existsSync(partitionMetadataPath)) {
          const content = fs.readFileSync(partitionMetadataPath, 'utf8');
          
          content.split(/\r?\n/).forEach(line => {
            const trimmedLine = line.trim();
            if (trimmedLine.startsWith("topic_id=")) {
              const uuidString = trimmedLine.substring("topic_id=".length);
              const rawUuidHex = uuidString.replace(/-/g, '').toLowerCase();
              if (rawUuidHex.length === 32) {
                topicIdToNameMap.set(rawUuidHex, topicName);
                console.error(`[your_program] DEBUG: Loaded topic mapping: ${rawUuidHex} -> ${topicName}`);
              }
            }
          });
        }
      }
    }
    metadataLoaded = true;
  } catch (error) {
    console.error(`[your_program] ERROR: Failed to load topic metadata: ${error}`);
    metadataLoaded = true;
  }
}

export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
    console.error(`[your_program] DEBUG: handleFetchApiRequest called`);
    
    if (!metadataLoaded) {
        loadTopicMetadata("/tmp/kraft-combined-logs");
    }

    // Constants for response fields
    const throttleTime = Buffer.alloc(4).fill(0);
    const errorCode = Buffer.alloc(2).fill(0);
    const sessionId = Buffer.alloc(4).fill(0);
    const responseTagBuffer = Buffer.from([0]);

    // Parse request - Fetch API v16
    let offset = 12; // Skip message size (4) + API key (2) + API version (2) + correlation ID (4)
    
    // Read client ID (compact string)
    let clientIdVarIntInfo = readVarInt(buffer, offset);
    offset = clientIdVarIntInfo.offset;
    const clientIdLength = clientIdVarIntInfo.value - 1;
    console.error(`[your_program] DEBUG: Client ID length: ${clientIdLength}, offset: ${offset}`);
    if (clientIdLength > 0) {
        offset += clientIdLength;
    }
    
    // Read TAG_BUFFER (should be 0)
    const headerTagBuffer = buffer.readUInt8(offset++);
    console.error(`[your_program] DEBUG: Header TAG_BUFFER: ${headerTagBuffer}, offset: ${offset}`);
    
    // Read request body fields
    const replicaId = buffer.readInt32BE(offset); offset += 4;
    const maxWaitMs = buffer.readInt32BE(offset); offset += 4;
    const minBytes = buffer.readInt32BE(offset); offset += 4;
    const maxBytes = buffer.readInt32BE(offset); offset += 4;
    const isolationLevel = buffer.readUInt8(offset); offset += 1;
    const sessionIdFromReq = buffer.readInt32BE(offset); offset += 4;
    const sessionEpoch = buffer.readInt32BE(offset); offset += 4;
    
    console.error(`[your_program] DEBUG: After request body fields, offset: ${offset}`);
    console.error(`[your_program] DEBUG: replicaId: ${replicaId}, maxWaitMs: ${maxWaitMs}, minBytes: ${minBytes}, maxBytes: ${maxBytes}`);
    
    // Read topics array (compact array)
    let topicArrayInfo = readVarInt(buffer, offset);
    offset = topicArrayInfo.offset;
    
    // Fix: For compact arrays, 0 means empty, 1 means 1 element, etc.
    // Only subtract 1 if the value is > 0
    const numTopics = topicArrayInfo.value > 0 ? topicArrayInfo.value - 1 : 0;

    console.error(`[your_program] DEBUG: VarInt value: ${topicArrayInfo.value}, Number of topics in request: ${numTopics}, offset: ${offset}`);
    
    const topicResponses = [];

    // Process topics from request
    if (numTopics > 0) {
        // Process specific topics requested
        for (let i = 0; i < numTopics; i++) {
            const topicIdBuffer = buffer.subarray(offset, offset + 16);
            offset += 16;
            const topicTagBuffer = buffer.readUInt8(offset++);
            
            const topicIdHex = topicIdBuffer.toString('hex');
            const topicName = topicIdToNameMap.get(topicIdHex) || 'unknown';
            
            console.error(`[your_program] DEBUG: Processing topic ${topicIdHex} -> ${topicName}`);
            
            // Process partitions
            let partitionArrayInfo = readVarInt(buffer, offset);
            offset = partitionArrayInfo.offset;
            const numPartitions = partitionArrayInfo.value > 0 ? partitionArrayInfo.value - 1 : 0;
            
            console.error(`[your_program] DEBUG: Number of partitions for topic ${topicIdHex}: ${numPartitions}`);
            
            const partitionResponses = [];
            for (let j = 0; j < numPartitions; j++) {
                const partitionIndex = buffer.readInt32BE(offset); offset += 4;
                const currentLogOffset = buffer.readInt64BE ? buffer.readBigInt64BE(offset) : buffer.readInt32BE(offset + 4);
                offset += 8;
                const lastFetchedEpoch = buffer.readInt32BE(offset); offset += 4;
                const fetchOffset = buffer.readInt64BE ? buffer.readBigInt64BE(offset) : buffer.readInt32BE(offset + 4);
                offset += 8;
                const partitionMaxBytes = buffer.readInt32BE(offset); offset += 4;
                const partitionTagBuffer = buffer.readUInt8(offset++);
                
                console.error(`[your_program] DEBUG: Processing partition ${partitionIndex} for topic ${topicIdHex}`);
                
                // Create partition response
                const partitionResponseBuffer = Buffer.alloc(4);
                partitionResponseBuffer.writeInt32BE(partitionIndex);
                
                const partitionResponse = Buffer.concat([
                    partitionResponseBuffer,
                    errorCode,
                    Buffer.alloc(8).fill(0), // high_watermark
                    Buffer.alloc(8).fill(0), // last_stable_offset
                    Buffer.alloc(8).fill(0), // log_start_offset
                    writeVarInt(1), // aborted_transactions (empty array)
                    Buffer.alloc(4).fill(0), // preferred_read_replica
                    writeCompactBytes(Buffer.alloc(0)), // empty records
                    responseTagBuffer
                ]);
                partitionResponses.push(partitionResponse);
            }
            
            const topicResponse = Buffer.concat([
                topicIdBuffer, // Use the original topic ID from request
                writeVarInt(partitionResponses.length + 1),
                ...partitionResponses,
                responseTagBuffer
            ]);
            topicResponses.push(topicResponse);
        }
    } else {
        // No specific topics requested - return all available topics
        console.error(`[your_program] DEBUG: No topics in request, returning all available topics`);
        
        // Create responses for all available topics
        for (const [topicIdHex, topicName] of topicIdToNameMap.entries()) {
            console.error(`[your_program] DEBUG: Creating response for topic ${topicIdHex} -> ${topicName}`);
            
            // Convert hex string back to buffer
            const topicIdBuffer = Buffer.from(topicIdHex, 'hex');
            
            // Find partitions for this topic
            const partitionResponses = [];
            try {
                const entries = fs.readdirSync("/tmp/kraft-combined-logs", { withFileTypes: true });
                
                for (const entry of entries) {
                    if (entry.isDirectory() && entry.name.startsWith(topicName + '-')) {
                        const partitionIndex = parseInt(entry.name.split('-')[1]);
                        if (!isNaN(partitionIndex)) {
                            console.error(`[your_program] DEBUG: Found partition ${partitionIndex} for topic ${topicName}`);
                            
                            const partitionResponseBuffer = Buffer.alloc(4);
                            partitionResponseBuffer.writeInt32BE(partitionIndex);
                            
                            const partitionResponse = Buffer.concat([
                                partitionResponseBuffer,
                                errorCode,
                                Buffer.alloc(8).fill(0), // high_watermark
                                Buffer.alloc(8).fill(0), // last_stable_offset
                                Buffer.alloc(8).fill(0), // log_start_offset
                                writeVarInt(1), // aborted_transactions (empty array)
                                Buffer.alloc(4).fill(0), // preferred_read_replica
                                writeCompactBytes(Buffer.alloc(0)), // empty records
                                responseTagBuffer
                            ]);
                            partitionResponses.push(partitionResponse);
                        }
                    }
                }
            } catch (error) {
                console.error(`[your_program] ERROR: Failed to read partitions for topic ${topicName}: ${error}`);
            }
            
            if (partitionResponses.length > 0) {
                const topicResponse = Buffer.concat([
                    topicIdBuffer,
                    writeVarInt(partitionResponses.length + 1),
                    ...partitionResponses,
                    responseTagBuffer
                ]);
                topicResponses.push(topicResponse);
            }
        }
    }

    // Build response
    const responseBody = Buffer.concat([
        throttleTime,
        errorCode,
        sessionId,
        writeVarInt(topicResponses.length + 1),
        ...topicResponses,
        responseTagBuffer
    ]);

    const correlationIdBuffer = Buffer.alloc(4);
    correlationIdBuffer.writeInt32BE(responseMessage.correlationId);
    const responseHeaderTagBuffer = Buffer.from([0]);

    const fullResponseData = Buffer.concat([
        correlationIdBuffer,
        responseHeaderTagBuffer,
        responseBody
    ]);

    const messageSizeBuffer = Buffer.alloc(4);
    messageSizeBuffer.writeInt32BE(fullResponseData.length);

    console.error(`[your_program] DEBUG: Sending response with ${fullResponseData.length} bytes`);
    connection.write(Buffer.concat([messageSizeBuffer, fullResponseData]));
};
