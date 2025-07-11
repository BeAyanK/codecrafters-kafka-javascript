import fs from "fs";
import { sendResponseMessage } from "./utils/index.js";
export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
  const clientLength = buffer.subarray(12, 14);
  const clientLengthValue = clientLength.readInt16BE();
  const throttleTime = Buffer.from([0, 0, 0, 0]);
  const errorCode = Buffer.from([0, 0]);
  let responses = Buffer.from([1]);
  const tagBuffer = Buffer.from([0]);
  const sessionIdIndex = clientLengthValue + 28; // skip 15 bytes before and 13 bytes after client record
  const sessionId = buffer.subarray(sessionIdIndex, sessionIdIndex + 4);
  const _sessionEpoch = buffer.subarray(sessionIdIndex + 4, sessionIdIndex + 8);
  const topicArrayLength = buffer.subarray(
    sessionIdIndex + 8,
    sessionIdIndex + 9,
  );
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
        const partitionLength = buffer.subarray(
          partitionArrayIndex,
          partitionArrayIndex + 1,
        );
        const partitionIndex = buffer.subarray(
          partitionArrayIndex + 1,
          partitionArrayIndex + 5,
        );
        const highWaterMark = Buffer.from(new Array(8).fill(0));
        const last_stable_offset = Buffer.from(new Array(8).fill(0));
        const log_start_offset = Buffer.from(new Array(8).fill(0));
        const aborted_transactions = Buffer.from([0]);
        const preferredReadReplica = Buffer.from([0, 0, 0, 0]);
        const recordBatch =
          logFileIndex === -1
            ? Buffer.from([0])
            : readFromFileBuffer(topicName.toString(), partitionIndex.readInt32BE());
        console.log("recordBatch", recordBatch);
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
  messageSizeBuffer.writeInt32BE([
    Buffer.concat(Object.values(fetchRequestResponse)).length,
  ]);
  fetchRequestResponse = {
    messageSize: messageSizeBuffer,
    ...fetchRequestResponse,
  };
  sendResponseMessage(connection, fetchRequestResponse);
};
function readFromFileBuffer(topicName, partitionIndex, offset = 0, batchSize = 10) {
    try {
        const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${partitionIndex}/00000000000000000000.log`;

        if (!fs.existsSync(logFilePath)) {
            console.error(`Log file not found: ${logFilePath}`);
            return Buffer.from([0]); // Return empty buffer instead of throwing
        }

        const logFile = fs.readFileSync(logFilePath); // Read entire file
        
        // For now, return the raw log file content as a simple record batch
        // In a real implementation, this would need proper Kafka record batch formatting
        if (logFile.length === 0) {
            return Buffer.from([0]); // Empty batch
        }
        
        // Create a simple record batch structure
        const baseOffset = Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]); // int64
        const batchLength = Buffer.alloc(4);
        batchLength.writeInt32BE(logFile.length + 49); // batch overhead + data
        const partitionLeaderEpoch = Buffer.from([0, 0, 0, 0]); // int32
        const magicByte = Buffer.from([2]); // int8
        const crc = Buffer.from([0, 0, 0, 0]); // int32 (would normally calculate this)
        const attributes = Buffer.from([0, 0]); // int16
        const lastOffsetDelta = Buffer.from([0, 0, 0, 0]); // int32
        const baseTimestamp = Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]); // int64
        const maxTimestamp = Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]); // int64
        const producerId = Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]); // int64
        const producerEpoch = Buffer.from([0, 0]); // int16
        const baseSequence = Buffer.from([0, 0, 0, 0]); // int32
        const records = Buffer.from([0, 0, 0, 1]); // int32 (record count)
        
        return Buffer.concat([
            baseOffset,
            batchLength,
            partitionLeaderEpoch,
            magicByte,
            crc,
            attributes,
            lastOffsetDelta,
            baseTimestamp,
            maxTimestamp,
            producerId,
            producerEpoch,
            baseSequence,
            records,
            logFile // actual message data
        ]);
    } catch (error) {
        console.error("Error reading log file:", error);
        return Buffer.from([0]); // Return empty buffer on error
    }
}
