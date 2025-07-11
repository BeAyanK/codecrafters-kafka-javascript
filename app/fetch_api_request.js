import fs from "fs";
// Removed `sendResponseMessage` import as it's directly using `connection.write`

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
  const length = writeVarInt(data.length);
  return Buffer.concat([length, data]);
}

export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
  // Constants for response fields (filled with zeros as per requirements)
  const throttleTime = Buffer.alloc(4).fill(0); // No throttling
  const errorCode = Buffer.alloc(2).fill(0);     // No error
  const sessionId = Buffer.alloc(4).fill(0);     // Session ID
  // const sessionEpoch = Buffer.alloc(4).fill(0);  // Removed as per test expectations
  const responseTagBuffer = Buffer.from([0]);    // Empty TAG_BUFFER

  // This part of the request parsing is simplified and may not be fully robust,
  // but it's enough to extract the necessary information for this stage.
  // A proper implementation would parse all flexible fields correctly.
  let offset = 12; // Start after basic header (size, key, version, correlationId)
  
  // Skip over client_id (COMPACT_STRING) and the request tag buffer
  const { value: clientIdLength, offset: newOffset } = readVarInt(buffer, offset);
  offset = newOffset + clientIdLength + 1;

  // Now, parse the actual FetchRequest fields
  offset += 4; // replica_id
  offset += 4; // max_wait_ms
  offset += 4; // min_bytes
  offset += 4; // max_bytes
  
  // isolation_level: INT8
  offset += 1;

  // session_id: INT32
  offset += 4;

  // session_epoch: INT32
  offset += 4;

  // topics: COMPACT_ARRAY of FetchTopic
  let topicArrayInfo = readVarInt(buffer, offset);
  offset = topicArrayInfo.offset;
  const numTopics = topicArrayInfo.value - 1;

  const topicResponses = [];

  for (let i = 0; i < numTopics; i++) {
    const topicId = buffer.subarray(offset, offset + 16); // topic_id: UUID
    offset += 16;
    
    // Find topic name from metadata log
    const metaLog = fs.readFileSync(`/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`);
    let topicName = "";
    
    // This simplified parsing assumes a specific structure for the metadata log
    // A full implementation would parse the RecordBatches within the log
    let searchPos = 0;
    while(searchPos < metaLog.length) {
      const idPos = metaLog.indexOf(topicId, searchPos);
      if (idPos === -1) break;

      // Heuristically find the topic name before the UUID
      // This is not a robust parsing method
      const nameLengthPos = idPos - 2;
      const nameLength = metaLog.readInt8(nameLengthPos) - 1;
      const namePos = nameLengthPos - nameLength;
      if (namePos > 0) {
          topicName = metaLog.subarray(namePos, namePos + nameLength).toString();
          break;
      }
      searchPos = idPos + 16;
    }
    

    // partitions: COMPACT_ARRAY of FetchPartition
    let partitionArrayInfo = readVarInt(buffer, offset);
    offset = partitionArrayInfo.offset;
    const numPartitions = partitionArrayInfo.value - 1; 

    const partitionResponses = [];

    for (let j = 0; j < numPartitions; j++) {
      const partitionIndex = buffer.readInt32BE(offset); offset += 4;
      const currentLeaderEpoch = buffer.readInt32BE(offset); offset += 4;
      const fetchOffset = buffer.readBigInt64BE(offset); offset += 8;
      const logStartOffsetReq = buffer.readBigInt64BE(offset); offset += 8;
      const partitionMaxBytes = buffer.readInt32BE(offset); offset += 4;
      offset += 1; // partition tag buffer

      let recordBatchBuffer = Buffer.from([1, 0]); // Empty compact bytes
      try {
        const pIndex = partitionIndex;
        const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${pIndex}/00000000000000000000.log`;

        if (fs.existsSync(logFilePath)) {
          const fileContent = fs.readFileSync(logFilePath);
          recordBatchBuffer = writeCompactBytes(fileContent);
        }
      } catch (error) {
        console.error("Error reading log file for partition:", error);
      }
      
      const partitionIndexBuffer = Buffer.alloc(4);
      partitionIndexBuffer.writeInt32BE(partitionIndex);

      const currentLeaderEpochBuffer = Buffer.alloc(4);
      currentLeaderEpochBuffer.writeInt32BE(currentLeaderEpoch);

      const partitionResponse = Buffer.concat([
        partitionIndexBuffer,                               
        errorCode,                                          
        Buffer.alloc(8).fill(0), // high_watermark
        Buffer.alloc(8).fill(0), // last_stable_offset
        Buffer.alloc(8).fill(0), // log_start_offset
        writeVarInt(0), // aborted_transactions length (is nullable, 0 means null)
        Buffer.alloc(4).fill(0), // preferred_read_replica
        recordBatchBuffer, // records
        responseTagBuffer, // partition tag buffer
      ]);
      partitionResponses.push(partitionResponse);
    }
    
    const topicResponse = Buffer.concat([
      topicId,
      writeVarInt(partitionResponses.length + 1),
      ...partitionResponses,
      responseTagBuffer,
    ]);
    topicResponses.push(topicResponse);
  }

  // Construct the full Fetch Response (v16) body
  const responseBody = Buffer.concat([
    throttleTime,
    errorCode,
    sessionId,
    // sessionEpoch is removed here
    writeVarInt(topicResponses.length + 1), // topics: COMPACT_ARRAY
    ...topicResponses,
    responseTagBuffer, // Tag buffer for the entire response
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

  connection.write(Buffer.concat([messageSizeBuffer, fullResponseData]));
};
