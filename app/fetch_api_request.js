import fs from "fs";
import { sendResponseMessage } from "./utils/index.js";

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

// Helper: Write Compact Bytes
function writeCompactBytes(data) {
  const length = writeVarInt(data.length);
  return Buffer.concat([length, data]);
}

export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
  // Constants
  const throttleTime = Buffer.alloc(4).writeInt32BE(0); // No throttling
  const errorCode = Buffer.alloc(2).writeInt16BE(0);     // No error
  const sessionId = Buffer.alloc(4).fill(0);             // Session ID
  const tagBuffer = Buffer.from([0]);                    // Empty TAG_BUFFER

  // Find offset after session_id field
  const sessionIdOffset = buffer.indexOf(sessionId) + 4;

  // Skip epoch, topic array length
  const topicArrayLength = buffer.readUInt8(sessionIdOffset + 4 + 1);
  let topicIndex = sessionIdOffset + 5;

  const responses = [];

  for (let i = 0; i < topicArrayLength; i++) {
    const topicId = buffer.subarray(topicIndex, topicIndex + 16);
    topicIndex += 16;

    // Read log file to get topic name
    const metaLog = fs.readFileSync(`/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`);
    const topicNameStart = metaLog.indexOf(topicId) - 3;
    const topicName = metaLog.subarray(topicNameStart, topicNameStart + 3).toString();

    // Read partition count
    const numPartitions = buffer.readUInt8(topicIndex);
    topicIndex += 1;

    const partitions = [];

    for (let j = 0; j < numPartitions; j++) {
      const partitionIndex = buffer.readInt32BE(topicIndex);
      topicIndex += 4;

      // Placeholder values
      const partitionErrorCode = Buffer.alloc(2).writeInt16BE(0);
      const highWaterMark = Buffer.alloc(8);
      const lastStableOffset = Buffer.alloc(8);
      const logStartOffset = Buffer.alloc(8);
      const abortedTransactions = Buffer.from([0]); // empty array
      const preferredReadReplica = Buffer.alloc(4).fill(0);

      // Read actual message log file
      const logPath = `/tmp/kraft-combined-logs/${topicName}-${partitionIndex}/00000000000000000000.log`;
      let recordBatch = fs.readFileSync(logPath);
      const compactRecords = writeCompactBytes(recordBatch);

      // Build partition response
      const partitionResponse = Buffer.concat([
        Buffer.alloc(1).writeUInt8(partitionIndex),
        partitionErrorCode,
        highWaterMark,
        lastStableOffset,
        logStartOffset,
        abortedTransactions,
        preferredReadReplica,
        compactRecords,
        tagBuffer,
      ]);

      partitions.push(partitionResponse);
    }

    const topicResponse = Buffer.concat([
      topicId,
      Buffer.alloc(1).writeUInt8(numPartitions),
      ...partitions,
      tagBuffer,
    ]);

    responses.push(topicResponse);
  }

  const responseBody = Buffer.concat([
    throttleTime,
    errorCode,
    sessionId,
    Buffer.alloc(1).writeUInt8(responses.length),
    ...responses,
    tagBuffer,
  ]);

  const correlationId = Buffer.alloc(4);
  correlationId.writeInt32BE(responseMessage.correlationId);

  const fullResponse = Buffer.concat([
    correlationId,
    responseBody,
  ]);

  const messageSize = Buffer.alloc(4);
  messageSize.writeInt32BE(fullResponse.length);

  const finalMessage = Buffer.concat([messageSize, fullResponse]);

  sendResponseMessage(connection, {
    messageSize,
    correlationId,
    throttleTime,
    errorCode,
    sessionId,
    responses: Buffer.concat(responses),
    tagBuffer,
  });
};
