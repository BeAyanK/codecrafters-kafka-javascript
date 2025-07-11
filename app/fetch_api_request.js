import fs from "fs";
import { sendResponseMessage } from "./utils/index.js";

export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
  const clientLength = buffer.subarray(12, 14);
  const clientLengthValue = clientLength.readInt16BE();
  const throttleTime = Buffer.from([0, 0, 0, 0]);
  const errorCode = Buffer.from([0, 0]);
  let responses = Buffer.from([1]); // Default empty response
  const tagBuffer = Buffer.from([0]);
  const sessionIdIndex = clientLengthValue + 28;
  const sessionId = buffer.subarray(sessionIdIndex, sessionIdIndex + 4);

  const _sessionEpoch = buffer.subarray(sessionIdIndex + 4, sessionIdIndex + 8);
  const topicArrayLength = buffer.subarray(sessionIdIndex + 8, sessionIdIndex + 9);
  let topicIndex = sessionIdIndex + 9;

  const numTopics = topicArrayLength.readInt8();

  if (numTopics === 0) {
    responses = Buffer.from([0]); // No topics
  } else {
    const topics = [];

    for (let i = 0; i < numTopics; i++) {
      const topicId = buffer.subarray(topicIndex, topicIndex + 16);
      topicIndex += 16;

      const logFile = fs.readFileSync(
        `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`,
      );
      const logFileIndex = logFile.indexOf(topicId);
      let partitionError = Buffer.from([0, 0]); // No error
      let topicName = "";

      if (logFileIndex !== -1) {
        topicName = logFile.subarray(logFileIndex - 3, logFileIndex).toString();
      }

      const partitionArrayLength = buffer.readUInt8(topicIndex);
      topicIndex += 1;

      const partitions = [];

      for (let j = 0; j < partitionArrayLength; j++) {
        const partitionIndex = buffer.readInt32BE(topicIndex); // Read partition index
        topicIndex += 4;

        const highWaterMark = Buffer.alloc(8);
        const lastStableOffset = Buffer.alloc(8);
        const logStartOffset = Buffer.alloc(8);
        const abortedTransactions = Buffer.from([0]);
        const preferredReadReplica = Buffer.from([0, 0, 0, 0]);

        let recordBatch = Buffer.from([0]); // default empty batch

        if (logFileIndex !== -1) {
          try {
            const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${partitionIndex}/00000000000000000000.log`;
            if (fs.existsSync(logFilePath)) {
              recordBatch = fs.readFileSync(logFilePath);

              // Prepend record length if needed (Kafka v2+ format)
              const recordsLength = Buffer.alloc(4);
              recordsLength.writeInt32BE(recordBatch.length);
              recordBatch = Buffer.concat([recordsLength, recordBatch]);
            }
          } catch (error) {
            console.error("Error reading log file:", error);
            partitionError = Buffer.from([0, 100]); // Unexpected error
          }
        }

        const partitionResponse = Buffer.concat([
          Buffer.from([0]), // Partition index (int8)
          partitionError,
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

      topics.push(topicResponse);
    }

    responses = Buffer.concat([
      Buffer.from([numTopics]),
      ...topics,
    ]);
  }

  const fetchResponse = Buffer.concat([
    throttleTime,
    errorCode,
    sessionId,
    responses,
    tagBuffer,
  ]);

  const correlationIdBuffer = Buffer.alloc(4);
  correlationIdBuffer.writeInt32BE(responseMessage.correlationId);

  const fullResponse = Buffer.concat([
    correlationIdBuffer,
    fetchResponse,
    tagBuffer,
  ]);

  const messageSize = Buffer.alloc(4);
  messageSize.writeInt32BE(fullResponse.length);

  const finalMessage = Buffer.concat([messageSize, fullResponse]);

  sendResponseMessage(connection, {
    messageSize,
    correlationId: correlationIdBuffer,
    throttleTime,
    errorCode,
    sessionId,
    responses,
    tagBuffer,
  });
};
