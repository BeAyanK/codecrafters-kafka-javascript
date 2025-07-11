import { sendResponseMessage } from "./utils/index.js";
import * as fs from "fs";

export const handleDescribeTopicPartitionsRequest = (
  connection,
  responseMessage,
  buffer,
) => {
  const clientLength = buffer.subarray(12, 14);
  const clientLengthValue = clientLength.readInt16BE();
  const tagBuffer = Buffer.from([0]);
  const throttleTimeMs = Buffer.from([0, 0, 0, 0]);

  let updatedResponse = {
    correlationId: responseMessage.correlationId,
    tagBuffer,
    throttleTimeMs,
  };

  const topicArrayLength = buffer
    .subarray(clientLengthValue + 15, clientLengthValue + 16)
    .readInt8();

  let topicIndex = clientLengthValue + 16;
  const topics = new Array(topicArrayLength).fill(0).map((_) => {
    const topicLength = buffer.subarray(topicIndex, topicIndex + 1);
    topicIndex += 1;
    const topicNameBuffer = buffer.subarray(
      topicIndex,
      topicIndex + topicLength.readInt8(),
    );
    topicIndex += topicLength.readInt8();
    return {
      topicLength,
      topicName: topicNameBuffer.toString("utf8"),
      topicNameBuffer,
    };
  });

  // Read the __cluster_metadata log file
  const metadataLogPath =
    "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log";
  const metadataLog = fs.readFileSync(metadataLogPath);

  topics.forEach(({ topicLength, topicName, topicNameBuffer }, index) => {
    let topicId = Buffer.from(new Array(16).fill(0)); // Default to all zeros
    let partitionIndex = Buffer.from([0, 0, 0, 0]); // Default to 0
    let errorCode = Buffer.from([0, 0]); // Default to no error
    let topicAuthorizedOperations = Buffer.from("00000df8", "hex"); // Default value

    // Simple parsing of __cluster_metadata for demonstration.
    // In a real implementation, you'd need a robust Kafka log parser.
    const logContent = metadataLog.toString("utf8");
    const lines = logContent.split("\n");

    for (const line of lines) {
      if (line.includes(`"topicName":"${topicName}"`)) {
        // Attempt to find topicId
        const topicIdMatch = line.match(/"topicId":"([0-9a-fA-F-]+)"/);
        if (topicIdMatch && topicIdMatch[1]) {
          // Convert UUID string to Buffer (remove hyphens and parse as hex)
          const uuidHex = topicIdMatch[1].replace(/-/g, "");
          topicId = Buffer.from(uuidHex, "hex");
        }

        // Attempt to find partition
        const partitionMatch = line.match(/"partitionId":(\d+)/);
        if (partitionMatch && partitionMatch[1]) {
          partitionIndex = Buffer.alloc(4);
          partitionIndex.writeInt32BE(parseInt(partitionMatch[1]), 0);
        }
        break; // Found the topic, no need to search further
      }
    }

    // Construct partition data
    const partitionErrorCode = Buffer.from([0, 0]); // No error for partition
    const leaderId = Buffer.from([0, 0, 0, 0]); // Assuming leader is 0 for now
    const leaderEpoch = Buffer.from([0, 0, 0, 0]);
    const replicaNodesCount = Buffer.from([0, 0, 0, 1]); // One replica
    const replicaNodeId = Buffer.from([0, 0, 0, 0]); // Node 0
    const isrNodesCount = Buffer.from([0, 0, 0, 1]); // One ISR node
    const isrNodeId = Buffer.from([0, 0, 0, 0]); // Node 0
    const offlineReplicasCount = Buffer.from([0, 0, 0, 0]); // No offline replicas
    const tagBufferPartition = Buffer.from([0]);

    const partitionData = Buffer.concat([
      partitionErrorCode,
      partitionIndex,
      leaderId,
      leaderEpoch,
      replicaNodesCount,
      replicaNodeId,
      isrNodesCount,
      isrNodeId,
      offlineReplicasCount,
      tagBufferPartition,
    ]);

    const partitionsCount = Buffer.from([0, 0, 0, 1]); // One partition

    updatedResponse[`${index}topicName`] = Buffer.concat([
      errorCode, // Topic error code
      topicLength,
      topicNameBuffer,
      topicId,
      partitionsCount, // Number of partitions for this topic
      partitionData, // Partition details
      topicAuthorizedOperations,
      tagBuffer,
    ]);
  });

  updatedResponse.topicLength = Buffer.from([topics.length]); // Number of topics

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
