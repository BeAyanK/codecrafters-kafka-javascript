import fs from "fs";
import { sendResponseMessage } from "./utils/index.js";

// Helper: Write VarInt (compact integer) as per Kafka protocol
function writeVarInt(value) {
  const buffer = [];
  while ((value & 0xFFFFFF80) !== 0) {
    buffer.push((value & 0x7F) | 0x80);
    value >>>= 7;
  }
  buffer.push(value & 0x7F);
  return Buffer.from(buffer);
}

// Helper: Write Compact String
function writeCompactString(str) {
  const strBuffer = Buffer.from(str, "utf8");
  const len = writeVarInt(strBuffer.length);
  return Buffer.concat([len, strBuffer]);
}

// Helper: Write Compact Bytes
function writeCompactBytes(buffer) {
  const len = writeVarInt(buffer.length);
  return Buffer.concat([len, buffer]);
}

export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
  const tagBuffer = Buffer.from([0]); // Empty TAG_BUFFER
  const throttleTime = Buffer.alloc(4).writeInt32BE(0); // No throttling
  const errorCode = Buffer.alloc(2).writeInt16BE(0); // No error
  const sessionId = Buffer.alloc(4).fill(0); // Session ID

  const topicArrayLength = buffer.readUInt8(buffer.indexOf(tagBuffer) + 9);
  let topicIndex = buffer.indexOf(tagBuffer) + 10;

  const responses = [];

  for (let i = 0; i < topicArrayLength; i++) {
    const topicId = buffer.subarray(topicIndex, topicIndex + 16);
    topicIndex += 16;

    const logFile = fs.readFileSync(`/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`);
    const logFileIndex = logFile.indexOf(topicId);

    let topicName = "";
    if (logFileIndex !== -1) {
      topicName = logFile.subarray(logFileIndex - 3, logFileIndex).toString();
    }

    const partitionArrayLength = buffer.readUInt8(topicIndex);
    topicIndex += 1;

    const partitionResponses = [];

    for (let j = 0; j < partitionArrayLength; j++) {
      const partitionIndex = buffer.readInt32BE(topicIndex + 1);
      topicIndex += 5; // Skip index + padding/error handling

      const highWaterMark = Buffer.alloc(8).fill(0);
      const lastStableOffset = Buffer.alloc(8).fill(0);
      const logStartOffset = Buffer.alloc(8).fill(0);
      const abortedTransactions = Buffer.from([0]); // No transactions
      const preferredReadReplica = Buffer.alloc(4).fill(0); // No preferred replica

      let recordBatch = Buffer.from([]);

      try {
        const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${partitionIndex}/00000000000000000000.log`;
        if (fs.existsSync(logFilePath)) {
          recordBatch = fs.readFileSync(logFilePath);
        }
      } catch (err) {
        console.error("Error reading log file:", err);
      }

      // Use compact bytes for records
      const compactRecords = writeCompactBytes(recordBatch);

      // Build partition response
      const partitionResponse = Buffer.concat([
        Buffer.alloc(1).writeUInt8(partitionIndex), // Partition Index
        Buffer.alloc(2).writeInt16BE(0),            // Error Code
        highWaterMark,
        lastStableOffset,
        logStartOffset,
        abortedTransactions,
        preferredReadReplica,
        compactRecords,
        tagBuffer,
      ]);

      partitionResponses.push(partitionResponse);
    }

    const topicResponse = Buffer.concat([
      topicId,
      Buffer.alloc(1).writeUInt8(partitionResponses.length),
      ...partitionResponses,
      tagBuffer,
    ]);

    responses.push(topicResponse);
  }

  const fetchResponseBody = Buffer.concat([
    throttleTime,
    errorCode,
    Buffer.alloc(4).fill(0), // Session ID
    Buffer.alloc(1).writeUInt8(responses.length),
    ...responses,
    tagBuffer,
  ]);

  const correlationIdBuffer = Buffer.alloc(4);
  correlationIdBuffer.writeInt32BE(responseMessage.correlationId);

  const messageSize = Buffer.alloc(4);
  messageSize.writeInt32BE(fetchResponseBody.length + 4); // +4 for correlationId

  const fullResponse = Buffer.concat([
    messageSize,
    correlationIdBuffer,
    fetchResponseBody,
    tagBuffer,
  ]);

  sendResponseMessage(connection, {
    messageSize,
    correlationId: correlationIdBuffer,
    throttleTime,
    errorCode,
    sessionId,
    responses: Buffer.concat(responses),
    tagBuffer,
  });
};
