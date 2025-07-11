import fs from "fs";
import { sendResponseMessage } from "./utils/index.js";

export const handleFetchApiRequest = (connection, responseMessage, buffer) => {
  const clientLength = buffer.subarray(12, 14);
  const clientLengthValue = clientLength.readInt16BE();
  const throttleTime = Buffer.from([0, 0, 0, 0]);
  const errorCode = Buffer.from([0, 0]);
  const tagBuffer = Buffer.from([0]);
  const sessionId = Buffer.from([0, 0, 0, 0]); // Default session ID

  // Build responses
  let responses = Buffer.from([0]); // Start with 0 topics by default
  const topicArrayLength = buffer.subarray(sessionIdIndex + 8, sessionIdIndex + 9);
  
  if (topicArrayLength.readInt8() > 0) {
    const topics = [];
    let topicIndex = sessionIdIndex + 9;
    
    for (let i = 0; i < topicArrayLength.readInt8(); i++) {
      const topicId = buffer.subarray(topicIndex, topicIndex + 16);
      topicIndex += 16;
      
      const logFile = fs.readFileSync(
        `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`,
      );
      
      const logFileIndex = logFile.indexOf(topicId);
      const partitionError = logFileIndex === -1 ? Buffer.from([0, 100]) : Buffer.from([0, 0]);
      const topicName = logFileIndex === -1 ? "" : logFile.subarray(logFileIndex - 3, logFileIndex);
      
      const partitionIndex = buffer.subarray(topicIndex, topicIndex + 4);
      topicIndex += 4;
      
      const recordBatch = logFileIndex === -1
        ? Buffer.alloc(4, 0)
        : readFromFileBuffer(topicName.toString(), partitionIndex.readInt32BE());
      
      const partitionResponse = Buffer.concat([
        partitionIndex,
        partitionError,
        Buffer.from(new Array(8).fill(0)), // highWaterMark
        Buffer.from(new Array(8).fill(0)), // last_stable_offset
        Buffer.from(new Array(8).fill(0)), // log_start_offset
        Buffer.from([0]), // aborted_transactions
        Buffer.from([0, 0, 0, 0]), // preferredReadReplica
        recordBatch,
        tagBuffer
      ]);
      
      topics.push(Buffer.concat([
        topicId,
        Buffer.from([1]), // partitions array length
        partitionResponse,
        tagBuffer
      ]));
    }
    
    responses = Buffer.concat([
      Buffer.from([topics.length]), // topics array length
      ...topics
    ]);
  }

  // Build complete response
  const responseBody = Buffer.concat([
    throttleTime,
    errorCode,
    sessionId,
    responses,
    tagBuffer
  ]);

  const responseHeader = Buffer.concat([
    responseMessage.correlationId,
    tagBuffer
  ]);

  const messageSize = responseHeader.length + responseBody.length;
  const messageSizeBuffer = Buffer.alloc(4);
  messageSizeBuffer.writeInt32BE(messageSize);

  const fetchRequestResponse = {
    messageSize: messageSizeBuffer,
    correlationId: responseMessage.correlationId,
    responseHeaderTagBuffer: tagBuffer,
    throttleTime,
    errorCode,
    sessionId,
    responses,
    tagBuffer
  };

  sendResponseMessage(connection, fetchRequestResponse);
};

function readFromFileBuffer(topicName, partitionIndex) {
  try {
    const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${partitionIndex}/00000000000000000000.log`;
    
    if (!fs.existsSync(logFilePath)) {
      return Buffer.alloc(4, 0);
    }

    const logFile = fs.readFileSync(logFilePath);
    
    if (logFile.length === 0) {
      return Buffer.alloc(4, 0);
    }
    
    const recordsLength = Buffer.alloc(4);
    recordsLength.writeInt32BE(logFile.length);
    
    return Buffer.concat([recordsLength, logFile]);
  } catch (error) {
    console.error("Error reading log file:", error);
    return Buffer.alloc(4, 0);
  }
}
