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
  const throttleTime = Buffer.alloc(4).fill(0); // No throttling
  const errorCode = Buffer.alloc(2).fill(0);     // No error
  const sessionId = Buffer.alloc(4).fill(0);     // Session ID
  const tagBuffer = Buffer.from([0]);            // Empty TAG_BUFFER

  const clientLength = buffer.subarray(12, 14);
  const clientLengthValue = clientLength.readInt16BE();
  const sessionIdIndex = clientLengthValue + 28;

  const topicArrayLength = buffer.readUInt8(sessionIdIndex + 8);
  let topicIndex = sessionIdIndex + 9;

  const responses = [];

  for (let i = 0; i < topicArrayLength; i++) {
    const topicId = buffer.subarray(topicIndex, topicIndex + 16);
    topicIndex += 16;

    const metaLog = fs.readFileSync(`/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`);
    const logFileIndex = metaLog.indexOf(topicId);
    let topicName = "";

    if (logFileIndex !== -1) {
      topicName = metaLog.subarray(logFileIndex - 3, logFileIndex).toString();
    }

    const partitionArrayLength = buffer.readUInt8(topicIndex);
    topicIndex += 1;

    const partitions = [];

    for (let j = 0; j < partitionArrayLength; j++) {
      const partitionIndex = buffer.readInt32BE(topicIndex);
      topicIndex += 4;

      const highWaterMark = Buffer.alloc(8).fill(0);
      const lastStableOffset = Buffer.alloc(8).fill(0);
      const logStartOffset = Buffer.alloc(8).fill(0);
      const abortedTransactions = Buffer.from([0]);
      const preferredReadReplica = Buffer.alloc(4).fill(0);

      let recordBatch = Buffer.from([]);

      try {
        const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${partitionIndex}/00000000000000000000.log`;
        if (fs.existsSync(logFilePath)) {
          recordBatch = fs.readFileSync(logFilePath);
          recordBatch = writeCompactBytes(recordBatch); // Use compact bytes
        }
      } catch (error) {
        console.error("Error reading log file:", error);
      }

      const partitionResponse = Buffer.concat([
        Buffer.from([partitionIndex]), // int8
        errorCode,
        highWaterMark,
        lastStableOffset,
        logStartOffset,
        abortedTransactions,
        preferredReadReplica,
        recordBatch,
        tagBuffer,
      ]);

      partitions.push(partitionResponse);
    }

    const topicResponse = Buffer.concat([
      topicId,
      Buffer.from([partitionArrayLength]),
      ...partitions,
      tagBuffer,
    ]);

    responses.push(topicResponse);
  }

  const responseBody = Buffer.concat([
    throttleTime,
    errorCode,
    sessionId,
    Buffer.from([responses.length]),
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

  const finalMessage = Buffer.concat([
    messageSize,
    fullResponse,
  ]);

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
