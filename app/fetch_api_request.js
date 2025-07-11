import fs from "fs";
import { sendResponseMessage } from "./utils/index.js";

export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
  const clientLength = buffer.subarray(12, 14);
  const clientLengthValue = clientLength.readInt16BE();
  const throttleTime = Buffer.from([0, 0, 0, 0]);
  const errorCode = Buffer.from([0, 0]);
  let responses = Buffer.from([1]);
  const tagBuffer = Buffer.from([0]);
  const sessionIdIndex = clientLengthValue + 28;
  const sessionId = buffer.subarray(sessionIdIndex, sessionIdIndex + 4);
  const _sessionEpoch = buffer.subarray(sessionIdIndex + 4, sessionIdIndex + 8);
  const topicArrayLength = buffer.subarray(sessionIdIndex + 8, sessionIdIndex + 9);
  let topicIndex = sessionIdIndex + 9;

  if (topicArrayLength.readInt8() > 1) {
    const topics = new Array(topicArrayLength.readInt8() - 1)
      .fill(0)
      .map((_) => {
        const topicId = buffer.subarray(topicIndex, topicIndex + 16);
        topicIndex += 16;
        const logFile = fs.readFileSync(
          `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`,
        );
        const logFileIndex = logFile.indexOf(topicId);
        let partitionError = Buffer.from([0, 100]);
        let topicName = "";
        if (logFileIndex !== -1) {
          partitionError = Buffer.from([0, 0]);
          topicName = logFile.subarray(logFileIndex - 3, logFileIndex);
        }
        const partitionArrayIndex = topicIndex;
        const partitionLength = buffer.subarray(partitionArrayIndex, partitionArrayIndex + 1);
        const partitionIndex = buffer.subarray(partitionArrayIndex + 1, partitionArrayIndex + 5);
        const highWaterMark = Buffer.from(new Array(8).fill(0));
        const last_stable_offset = Buffer.from(new Array(8).fill(0));
        const log_start_offset = Buffer.from(new Array(8).fill(0));
        const aborted_transactions = Buffer.from([0]);
        const preferredReadReplica = Buffer.from([0, 0, 0, 0]);
        
        // Get record batch as Buffer
        let recordBatch = Buffer.from([0]); // Default empty batch
        if (logFileIndex !== -1) {
          try {
            const logFilePath = `/tmp/kraft-combined-logs/${topicName.toString()}-${partitionIndex.readInt32BE()}/00000000000000000000.log`;
            if (fs.existsSync(logFilePath)) {
              recordBatch = fs.readFileSync(logFilePath);
              // Prepend record length if needed
              const recordsLength = Buffer.alloc(4);
              recordsLength.writeInt32BE(recordBatch.length);
              recordBatch = Buffer.concat([recordsLength, recordBatch]);
            }
          } catch (error) {
            console.error("Error reading log file:", error);
          }
        }

        const partitionArrayBuffer = Buffer.concat([
          partitionLength,
          partitionIndex,
          partitionError,
          highWaterMark,
          last_stable_offset,
          log_start_offset,
          aborted_transactions,
          preferredReadReplica,
          recordBatch,
          tagBuffer,
        ]);
        return Buffer.concat([topicId, partitionArrayBuffer, tagBuffer]);
      });
    responses = Buffer.concat([topicArrayLength, ...topics]);
  }

  let fetchRequestResponse = {
    correlationId: responseMessage.correlationId,
    responseHeaderTagbuffer: tagBuffer,
    throttleTime,
    errorCode,
    sessionId,
    responses,
    tagBuffer,
  };

  const messageSizeBuffer = Buffer.alloc(4);
  messageSizeBuffer.writeInt32BE(Buffer.concat(Object.values(fetchRequestResponse)).length);

  fetchRequestResponse = {
    messageSize: messageSizeBuffer,
    ...fetchRequestResponse,
  };

  sendResponseMessage(connection, fetchRequestResponse);
};
