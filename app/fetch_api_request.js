import fs from "fs";

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
  // Constants for response fields
  const throttleTime = Buffer.alloc(4).fill(0);
  const errorCode = Buffer.alloc(2).fill(0);
  const sessionId = Buffer.alloc(4).fill(0);
  const responseTagBuffer = Buffer.from([0]);

  // --- Start: Corrected Request Parsing ---
  let offset = 12; // Start after correlationId

  // The request header isn't flexible, client_id is a STRING (INT16 length).
  const clientIdLength = buffer.readInt16BE(offset);
  offset += 2; // Advance past the INT16 length
  offset += clientIdLength; // Advance past the client_id string

  // Now, parse the FetchRequest v16 body fields from the correct offset
  offset += 4; // replica_id (int32)
  offset += 4; // max_wait_ms (int32)
  offset += 4; // min_bytes (int32)
  offset += 4; // max_bytes (int32)
  offset += 1; // isolation_level (int8)
  offset += 4; // session_id (int32)
  offset += 4; // session_epoch (int32)
  // --- End: Corrected Request Parsing ---

  // topics: COMPACT_ARRAY of FetchTopic
  let topicArrayInfo = readVarInt(buffer, offset);
  offset = topicArrayInfo.offset;
  const numTopics = topicArrayInfo.value - 1;

  const topicResponses = [];

  for (let i = 0; i < numTopics; i++) {
    const topicId = buffer.subarray(offset, offset + 16);
    offset += 16;
    offset += 1; // Skip tag buffer for topic

    const metaLog = fs.readFileSync(`/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`);
    let topicName = "";

    // This is a simplified search for the topic name. A robust solution
    // would parse the metadata log's record batches properly.
    let searchPos = 0;
    while(searchPos < metaLog.length) {
      const idPos = metaLog.indexOf(topicId, searchPos);
      if (idPos === -1) break;
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
      offset += 4; // current_leader_epoch
      offset += 8; // fetch_offset
      offset += 8; // log_start_offset
      offset += 4; // partition_max_bytes
      offset += 1; // partition tag buffer

      let recordBatchBuffer = Buffer.from([1, 0]); // Empty compact bytes
      try {
        const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${partitionIndex}/00000000000000000000.log`;
        if (fs.existsSync(logFilePath)) {
          const fileContent = fs.readFileSync(logFilePath);
          recordBatchBuffer = writeCompactBytes(fileContent);
        }
      } catch (error) {
        console.error(`Error reading log file for partition ${topicName}-${partitionIndex}:`, error);
      }
      
      const partitionIndexBuffer = Buffer.alloc(4);
      partitionIndexBuffer.writeInt32BE(partitionIndex);

      const partitionResponse = Buffer.concat([
        partitionIndexBuffer,
        errorCode,
        Buffer.alloc(8).fill(0), // high_watermark
        Buffer.alloc(8).fill(0), // last_stable_offset
        Buffer.alloc(8).fill(0), // log_start_offset
        writeVarInt(0),          // aborted_transactions
        Buffer.from([0,0,0,0]),  // preferred_read_replica
        recordBatchBuffer,       // records
        responseTagBuffer,       // partition tag buffer
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

  connection.write(Buffer.concat([messageSizeBuffer, fullResponseData]));
};
