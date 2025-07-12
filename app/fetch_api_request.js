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

    // Parse request
    let offset = 12;
    const headerTagBuffer = buffer.readUInt8(offset++);
    
    // Read client ID
    let clientIdVarIntInfo = readVarInt(buffer, offset);
    offset = clientIdVarIntInfo.offset;
    const clientIdLength = clientIdVarIntInfo.value - 1;
    offset += clientIdLength;

    // Read request body
    const replicaId = buffer.readInt32BE(offset); offset += 4;
    const maxWaitMs = buffer.readInt32BE(offset); offset += 4;
    const minBytes = buffer.readInt32BE(offset); offset += 4;
    const maxBytes = buffer.readInt32BE(offset); offset += 4;
    const isolationLevel = buffer.readUInt8(offset); offset += 1;
    const sessionIdFromReq = buffer.readInt32BE(offset); offset += 4;
    const sessionEpoch = buffer.readInt32BE(offset); offset += 4;
    
    // Read topics array
    let topicArrayInfo = readVarInt(buffer, offset);
    offset = topicArrayInfo.offset;
    const numTopics = topicArrayInfo.value - 1;

    const topicResponses = [];
    
    if (numTopics === 0) {
        console.error(`[your_program] DEBUG: No topics in request, sending empty response`);
        
        // Return empty response when no topics are requested
        const responseBody = Buffer.concat([
            throttleTime,
            errorCode,
            sessionId,
            writeVarInt(1), // num_responses = 0 (encoded as 1 for compact array)
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

        console.error(`[your_program] DEBUG: Sending empty response with ${fullResponseData.length} bytes`);
        connection.write(Buffer.concat([messageSizeBuffer, fullResponseData]));
        return;
    }

    // Process topics from request
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
        const numPartitions = partitionArrayInfo.value - 1;
        
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
