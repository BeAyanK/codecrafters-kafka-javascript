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
  const sessionEpoch = Buffer.alloc(4).fill(0);  // Session Epoch
  const responseTagBuffer = Buffer.from([0]);            // Empty TAG_BUFFER

  let offset = 12; // Start after correlationId (4 bytes message size + 2 api key + 2 api version + 4 correlationId)

  // Parse Fetch Request (v16) fields
  // replica_id: INT32
  // max_wait_ms: INT32
  // min_bytes: INT32
  // max_bytes: INT32
  // max_bytes_per_partition: INT32
  buffer.readInt32BE(offset); offset += 4; // replica_id
  buffer.readInt32BE(offset); offset += 4; // max_wait_ms
  buffer.readInt32BE(offset); offset += 4; // min_bytes
  buffer.readInt32BE(offset); offset += 4; // max_bytes
  buffer.readInt32BE(offset); offset += 4; // max_bytes_per_partition

  // rack_id: COMPACT_STRING
  let rackIdInfo = readVarInt(buffer, offset);
  offset = rackIdInfo.offset;
  offset += rackIdInfo.value; // Skip rack_id string bytes
  offset += 1; // Skip tag buffer for rack_id

  // isolation_level: INT8
  buffer.readInt8(offset); offset += 1; // isolation_level

  // session_id: INT32
  buffer.readInt32BE(offset); offset += 4; // session_id

  // session_epoch: INT32
  buffer.readInt32BE(offset); offset += 4; // session_epoch

  // topics: COMPACT_ARRAY of FetchTopic
  let topicArrayInfo = readVarInt(buffer, offset);
  offset = topicArrayInfo.offset;
  const numTopics = topicArrayInfo.value - 1; // Adjust for compact array encoding (length is encoded as value + 1)

  const topicResponses = [];

  for (let i = 0; i < numTopics; i++) {
    const topicId = buffer.subarray(offset, offset + 16); // topic_id: UUID
    offset += 16;
    offset += 1; // Skip tag buffer for topic id

    // Find topic name from metadata log
    const metaLog = fs.readFileSync(`/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`);
    let topicName = "";

    // Iterate through the metadata log to find the topic name associated with the topicId
    // This is more robust than relying on fixed offsets/lengths, by re-parsing the metadata log structure.
    let metaLogParseOffset = 0;
    while (metaLogParseOffset < metaLog.length) {
      try {
        // Read the `topic_name` length (1 byte, not compact)
        const currentTopicNameLengthWithCompact = metaLog.readUInt8(metaLogParseOffset);
        const currentTopicNameLength = currentTopicNameLengthWithCompact - 1; // Actual topic name length
        metaLogParseOffset += 1;

        // Read the `topic_name`
        const currentTopicName = metaLog.subarray(metaLogParseOffset, metaLogParseOffset + currentTopicNameLength);
        metaLogParseOffset += currentTopicNameLength;

        // Skip tag buffer for topic name (1 byte)
        metaLogParseOffset += 1;

        // Read the `topic_id` (16 bytes UUID)
        const currentTopicId = metaLog.subarray(metaLogParseOffset, metaLogParseOffset + 16);
        metaLogParseOffset += 16;

        // Compare topicId
        if (currentTopicId.equals(topicId)) {
          topicName = currentTopicName.toString();
          break; // Found the topic name
        }
      } catch (e) {
        // Break if parsing error (e.g., end of buffer)
        console.warn("Error parsing metadata log:", e);
        break;
      }
    }


    // partitions: COMPACT_ARRAY of FetchPartition
    let partitionArrayInfo = readVarInt(buffer, offset);
    offset = partitionArrayInfo.offset;
    const numPartitions = partitionArrayInfo.value - 1; // Adjust for compact array encoding

    const partitionResponses = [];

    for (let j = 0; j < numPartitions; j++) {
      const partitionIndex = buffer.readInt32BE(offset); // partition_index: INT32
      offset += 4;
      offset += 1; // Skip tag buffer for partition_index

      // current_leader_epoch: INT32
      const currentLeaderEpoch = buffer.readInt32BE(offset);
      offset += 4;

      // Read from log file and construct RecordBatch
      let recordBatchBuffer = Buffer.from([]);
      try {
        const pIndex = partitionIndex;
        const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${pIndex}/00000000000000000000.log`;
        if (fs.existsSync(logFilePath)) {
          const fileContent = fs.readFileSync(logFilePath);

          // For this stage, the log file itself contains the full RecordBatch.
          // We just need to wrap its contents as COMPACT_BYTES.
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
        partitionIndexBuffer,                               // partition_index: INT32
        errorCode,                                          // error_code: INT16
        Buffer.alloc(8).fill(0),                            // high_watermark: INT64 (0 as per requirement)
        Buffer.alloc(8).fill(0),                            // last_stable_offset: INT64 (0 as per requirement)
        currentLeaderEpochBuffer,                           // current_leader_epoch: INT32
        Buffer.alloc(8).fill(0),                            // log_start_offset: INT64 (0 as per requirement)
        writeVarInt(1),                                     // aborted_transactions: COMPACT_ARRAY (0 elements, so length is VarInt(1))
        responseTagBuffer,                                  // Tag buffer for aborted_transactions (compact arrays have tags)
        Buffer.alloc(4).fill(0),                            // preferred_read_replica: INT32 (always 0 for v16)
        recordBatchBuffer,                                  // records: COMPACT_BYTES
        responseTagBuffer,                                  // Tag buffer for partition response
      ]);
      partitionResponses.push(partitionResponse);
    }
    
    const topicResponse = Buffer.concat([
      topicId,                                        // topic_id: UUID
      writeVarInt(partitionResponses.length + 1),     // partitions: COMPACT_ARRAY
      ...partitionResponses,
      responseTagBuffer,                              // Tag buffer for topic response
    ]);
    topicResponses.push(topicResponse);
  }

  // Construct the full Fetch Response (v16) body
  const responseBody = Buffer.concat([
    throttleTime,
    errorCode,
    sessionId,
    sessionEpoch,
    writeVarInt(topicResponses.length + 1), // topics: COMPACT_ARRAY
    ...topicResponses,
    responseTagBuffer, // Tag buffer for the entire response
  ]);

  // Correlation ID and Message Size
  const correlationIdBuffer = Buffer.alloc(4);
  correlationIdBuffer.writeInt32BE(responseMessage.correlationId);
  
  // For flexible versions (like v16), the header requires a tag buffer.
  const responseHeaderTagBuffer = Buffer.from([0]);

  const fullResponseData = Buffer.concat([
    correlationIdBuffer,
    responseHeaderTagBuffer, // This is the corrected line
    responseBody,
  ]);

  const messageSizeBuffer = Buffer.alloc(4);
  messageSizeBuffer.writeInt32BE(fullResponseData.length);

  // Send the complete response back to the client
  connection.write(Buffer.concat([messageSizeBuffer, fullResponseData]));
};
