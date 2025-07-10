import { sendResponseMessage } from "./utils/index.js";

export const handleDescribeTopicPartitionsRequest = (
  connection,
  responseMessage,
  buffer,
) => {
  const clientLength = buffer.subarray(12, 14);
  const clientLengthValue = clientLength.readInt16BE();
  const tagBuffer = Buffer.from([0]);
  const throttleTimeMs = Buffer.from([0, 0, 0, 0]);
  const errorCode = Buffer.from([0, 3]);
  const topicId = Buffer.from(new Array(16).fill(0));
  const topicAuthorizedOperations = Buffer.from("00000df8", "hex");

  let updatedResponse = {
    correlationId: responseMessage.correlationId,
    tagBuffer,
    throttleTimeMs,
  };

  const topicArrayLength =
    buffer.subarray(clientLengthValue + 15, clientLengthValue + 16).readInt8() -
    1;

  let topicIndex = clientLengthValue + 16;
  const topics = new Array(topicArrayLength).fill(0).map((_) => {
    const topicLength = buffer.subarray(topicIndex, topicIndex + 1);
    topicIndex += 1;
    const topicName = buffer.subarray(
      topicIndex,
      topicIndex + topicLength.readInt8() - 1,
    );

    topicIndex += topicLength.readInt8() - 1;
    return [topicLength, topicName];
  });

  updatedResponse.topicLength = Buffer.from([topics.length + 1]);
  topics.forEach(([topicLength, topicName], index) => {
    updatedResponse[`${index}topicName`] = Buffer.concat([
      errorCode,
      topicLength,
      topicName,
      topicId,
      Buffer.from([0]),
      Buffer.from([1]),
      topicAuthorizedOperations,
      tagBuffer,
    ]);
  });

  const responsePartitionLimitIndex = topicIndex + 1;
  const _responsePartitionLimit = buffer.subarray(
    responsePartitionLimitIndex,
    responsePartitionLimitIndex + 4,
  );
  const cursorIndex = responsePartitionLimitIndex + 4;
  const cursor = buffer.subarray(cursorIndex, cursorIndex + 1);
  updatedResponse = { ...updatedResponse, cursor, cursortagbuffer: tagBuffer };

  const messageSize = Buffer.from([
    0,
    0,
    0,
    Buffer.concat(Object.values(updatedResponse)).length,
  ]);
  updatedResponse = {
    messageSize,
    ...updatedResponse,
  };

  console.log("messageSize", messageSize.readInt32BE());
  sendResponseMessage(connection, updatedResponse);
};
