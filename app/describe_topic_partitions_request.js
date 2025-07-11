import fs from "fs";

import { sendResponseMessage } from "./utils/index.js";

export const handleDescribeTopicPartitionsRequest = (
  connection,

  responseMessage,

  buffer
) => {
  const clientLength = buffer.subarray(12, 14);

  const clientLengthValue = clientLength.readInt16BE();

  const tagBuffer = Buffer.from([0]);

  const throttleTimeMs = Buffer.from([0, 0, 0, 0]);

  let errorCode = Buffer.from([0, 3]);

  let topicId = Buffer.from(new Array(16).fill(0));

  let partitions = [];

  let partitionBuffer = Buffer.from([0]);

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

      topicIndex + topicLength.readInt8() - 1
    );

    topicIndex += topicLength.readInt8() - 1;

    return [topicLength, topicName];
  });

  const responsePartitionLimitIndex = topicIndex + 1;

  const _responsePartitionLimit = buffer.subarray(
    responsePartitionLimitIndex,

    responsePartitionLimitIndex + 4
  );

  const cursorIndex = responsePartitionLimitIndex + 4;

  const cursor = buffer.subarray(cursorIndex, cursorIndex + 1);

  const logFile = fs.readFileSync(
    `/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log`
  );

  updatedResponse.topicLength = Buffer.from([topics.length + 1]);

  topics.forEach(([topicLength, topicName], index) => {
    let topicIndexInLogFile = logFile.indexOf(topicName.toString());

    if (topicIndexInLogFile !== -1) {
      errorCode = Buffer.from([0, 0]);

      topicIndexInLogFile = topicIndexInLogFile + topicLength.readUInt8() - 1;

      topicId = logFile.subarray(topicIndexInLogFile, topicIndexInLogFile + 16);

      let topicLogs = logFile.subarray(topicIndexInLogFile + 16);

      let partitionIndex = topicLogs.indexOf(topicId);

      while (partitionIndex !== -1) {
        const partitionErrorCode = Buffer.from([0, 0]);

        const partionId = topicLogs.subarray(
          partitionIndex - 4,

          partitionIndex
        );

        let replicaIndex = partitionIndex + 16;

        const lengthOfReplicaArray = topicLogs.subarray(
          replicaIndex,

          replicaIndex + 1
        );

        const replicaArrayLength = lengthOfReplicaArray.readInt8() - 1;

        replicaIndex += 1;

        const replicaArray = new Array(replicaArrayLength).fill(0).map((_) => {
          const replica = topicLogs.subarray(replicaIndex, replicaIndex + 4);

          replicaIndex += 4;

          return replica;
        });

        const replicaArrayBuffer = Buffer.concat([
          lengthOfReplicaArray,

          ...replicaArray,
        ]);

        let isrNodesIndex = replicaIndex;

        const isrNodesArray = topicLogs.subarray(
          isrNodesIndex,

          isrNodesIndex + 1
        );

        const isrNodesArrayLength = isrNodesArray.readInt8() - 1;

        isrNodesIndex += 1;

        const isrNodes = new Array(isrNodesArrayLength).fill(0).map((_) => {
          const isrNode = topicLogs.subarray(isrNodesIndex, isrNodesIndex + 4);

          isrNodesIndex += 4;

          return isrNode;
        });

        const isrNodesBuffer = Buffer.concat([isrNodesArray, ...isrNodes]);

        const lengthOfRemovingReplicas = topicLogs.subarray(
          isrNodesIndex,

          isrNodesIndex + 1
        );

        isrNodesIndex += 1;

        const lengthOfAddingReplicas = topicLogs.subarray(
          isrNodesIndex,

          isrNodesIndex + 1
        );

        let leaderIndex = isrNodesIndex + 1;

        const leaderId = topicLogs.subarray(leaderIndex, leaderIndex + 4);

        const leaderEppoch = topicLogs.subarray(
          leaderIndex + 4,

          leaderIndex + 8
        );

        topicLogs = topicLogs.subarray(leaderIndex + 8);

        partitions.push(
          Buffer.concat([
            partitionErrorCode,

            partionId,

            leaderId,

            leaderEppoch,

            replicaArrayBuffer,

            isrNodesBuffer,

            tagBuffer,

            tagBuffer,

            tagBuffer,

            tagBuffer,
          ])
        );

        partitionIndex = topicLogs.indexOf(topicId);

        if (partitionIndex === -1) {
          break;
        }
      }

      partitionBuffer = Buffer.concat([
        Buffer.from([partitions.length + 1]),

        ...partitions,
      ]);
    }

    updatedResponse[`${index}topicName`] = Buffer.concat([
      errorCode,

      topicLength,

      topicName,

      topicId,

      Buffer.from([0]),

      partitionBuffer,

      topicAuthorizedOperations,

      tagBuffer,
    ]);
  });

  updatedResponse = {
    ...updatedResponse,

    cursor,

    cursortagbuffer: tagBuffer,
  };

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

  sendResponseMessage(connection, updatedResponse);
};
