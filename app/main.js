// main.js
import { pick, sendResponseMessage } from "./utils/index.js";
import { handleApiVersionsRequest } from "./api_versions_request.js";
import { handleDescribeTopicPartitionsRequest } from "./describe_topic_partitions_request.js";
import { handleFetchApiRequest } from "./fetch_api_request.js"; // Correct import
import net from "net";
import { parseRequest } from "./request_parser.js";

const server = net.createServer((connection) => {
  connection.on("data", (buffer) => {
    const { messageSize, requestApiKey, requestApiVersion, correlationId } =
      parseRequest(buffer);
    const responseMessage = {
      messageSize,
      requestApiKey,
      requestApiVersion,
      correlationId: correlationId.readInt32BE(), // Read correlationId as int for passing
    };
    const requestVersion = requestApiVersion.readInt16BE();
    if (requestApiKey.readInt16BE() === 1) {
      handleFetchApiRequest(connection, responseMessage, buffer); // Pass connection, responseMessage, and full buffer
    } else if (requestApiKey.readInt16BE() === 18) {
      handleApiVersionsRequest(connection, responseMessage, requestVersion);
    } else if (requestApiKey.readInt16BE() === 75) {
      handleDescribeTopicPartitionsRequest(connection, responseMessage, buffer);
    } else {
      // Fallback for unhandled API keys
      // This part might also need to be adjusted to return a proper Kafka response header + error body
      // For simplicity, just sending messageSize and correlationId
      const unknownResponse = Buffer.concat([
        responseMessage.messageSize,
        Buffer.alloc(2).fill(0), // No error code
        Buffer.from([0, 0, 0, 0]), // Throttle time
        Buffer.from([0]), // Tag Buffer
      ]);
      connection.write(unknownResponse); // Directly write the response
    }
  });
});
server.listen(9092, "127.0.0.1");
