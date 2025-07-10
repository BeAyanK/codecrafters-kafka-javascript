import net from "net";

// You can use print statements as follows for debugging, they'll be visible when running tests.

console.log("Logs from your program will appear here!");

// Constants

const API_VERSIONS_KEY = 18; // ApiVersions API key

const DESCRIBE_TOPIC_PARTITIONS_KEY = 75; // DescribeTopicPartitions API key

const UNSUPPORTED_VERSION = 35; // Error code for unsupported version

const SUCCESS = 0; // Success code

const UNKNOWN_TOPIC_OR_PARTITION = 3; // Error code for unknown topic or partition

const MAX_SUPPORTED_VERSION_API_VERSIONS = 4; // Maximum supported version for ApiVersions

const MIN_SUPPORTED_VERSION_API_VERSIONS = 0; // Minimum supported version for ApiVersions

const MAX_SUPPORTED_VERSION_DESCRIBE_TOPIC_PARTITIONS = 0; // Maximum supported version for DescribeTopicPartitions

const MIN_SUPPORTED_VERSION_DESCRIBE_TOPIC_PARTITIONS = 0; // Minimum supported version for DescribeTopicPartitions

// Function to read a string from a buffer with Kafka's string format

function readString(buffer, offset) {
  const length = buffer.readInt16BE(offset);

  offset += 2;

  const value = buffer.toString("utf8", offset, offset + length);

  offset += length;

  return { value, offset };
}

// Function to write a string to a buffer with Kafka's string format

function writeString(buffer, value, offset) {
  buffer.writeInt16BE(value.length, offset);

  offset += 2;

  buffer.write(value, offset);

  offset += value.length;

  return offset;
}

const server = net.createServer((connection) => {
  connection.on("data", (data) => {
    // Extract request details

    const apiKey = data.readInt16BE(4);

    const apiVersion = data.readInt16BE(6);

    const correlationId = data.readInt32BE(8);

    console.log(
      `Received request: apiKey=${apiKey}, apiVersion=${apiVersion}, correlationId=${correlationId}`
    );

    let response;

    if (apiKey === API_VERSIONS_KEY) {
      // Handle ApiVersions request

      console.log("Processing ApiVersions request");

      // Create response with two API entries (APIVersions and DescribeTopicPartitions)

      // Each API entry is 6 bytes (2 for key, 2 for min version, 2 for max version) + 1 byte for tag buffer

      // Total size increases by 7 bytes for the additional API entry

      response = Buffer.alloc(30); // 23 (previous size) + 7 (new API entry)

      let offset = 0;

      // Message size (4 bytes) - 26 bytes for the rest of the message (19 + 7)

      response.writeInt32BE(26, offset);

      offset += 4;

      // Correlation ID (4 bytes) - from the request

      response.writeInt32BE(correlationId, offset);

      offset += 4;

      // Check if API version is supported

      if (
        apiVersion < MIN_SUPPORTED_VERSION_API_VERSIONS ||
        apiVersion > MAX_SUPPORTED_VERSION_API_VERSIONS
      ) {
        // Unsupported version - respond with error code

        console.log(
          `Unsupported version ${apiVersion}, responding with error code ${UNSUPPORTED_VERSION}`
        );

        response.writeInt16BE(UNSUPPORTED_VERSION, offset);
      } else {
        // Supported version - respond with success

        console.log(`Supported version ${apiVersion}, responding with success`);

        response.writeInt16BE(SUCCESS, offset);
      }

      offset += 2;

      // API keys array length (1 byte) - 3 in COMPACT_ARRAY format (N+1) for 2 entries

      response.writeUInt8(3, offset);

      offset += 1;

      // First API key entry (ApiVersions)

      response.writeInt16BE(API_VERSIONS_KEY, offset); // API key (18 = API_VERSIONS)

      offset += 2;

      response.writeInt16BE(MIN_SUPPORTED_VERSION_API_VERSIONS, offset); // Min version (0)

      offset += 2;

      response.writeInt16BE(MAX_SUPPORTED_VERSION_API_VERSIONS, offset); // Max version (4)

      offset += 2;

      // First API key entry tag buffer (1 byte) - 0 = no tagged fields

      response.writeUInt8(0, offset);

      offset += 1;

      // Second API key entry (DescribeTopicPartitions)

      response.writeInt16BE(DESCRIBE_TOPIC_PARTITIONS_KEY, offset); // API key (75 = DESCRIBE_TOPIC_PARTITIONS)

      offset += 2;

      response.writeInt16BE(
        MIN_SUPPORTED_VERSION_DESCRIBE_TOPIC_PARTITIONS,
        offset
      ); // Min version (0)

      offset += 2;

      response.writeInt16BE(
        MAX_SUPPORTED_VERSION_DESCRIBE_TOPIC_PARTITIONS,
        offset
      ); // Max version (0)

      offset += 2;

      // Second API key entry tag buffer (1 byte) - 0 = no tagged fields

      response.writeUInt8(0, offset);

      offset += 1;

      // Throttle time (4 bytes) - 0

      response.writeInt32BE(0, offset);

      offset += 4;

      // Response tag buffer (1 byte) - 0 = no tagged fields

      response.writeUInt8(0, offset);
    } else if (apiKey === DESCRIBE_TOPIC_PARTITIONS_KEY) {
      // Handle DescribeTopicPartitions request

      console.log("Processing DescribeTopicPartitions request");

      // Parse the request to get the topic name

      // Skip the header (12 bytes) and read the topics array

      let offset = 12;

      // Read throttle_time_ms (INT32)

      offset += 4;

      // Read topics array length (INT32)

      const topicsLength = data.readInt32BE(offset);

      offset += 4;

      // Create buffers for response

      const buffers = [];

      // For each topic in the request

      for (let i = 0; i < topicsLength; i++) {
        // Read topic name

        const topicResult = readString(data, offset);

        const topicName = topicResult.value;

        offset = topicResult.offset;

        console.log(`Topic name: ${topicName}`);

        // Create response for this topic

        // For unknown topic, we need to return:

        // - topic_name: STRING

        // - topic_id: UUID (all zeros for unknown topic)

        // - error_code: INT16 (3 = UNKNOWN_TOPIC_OR_PARTITION)

        // - is_internal: BOOLEAN

        // - partitions: ARRAY (empty for unknown topic)

        // - tag_buffer: empty (1 byte with value 0)

        // Calculate buffer size for this topic

        // 2 + topicName.length (topic_name) + 16 (topic_id) + 2 (error_code) + 1 (is_internal) + 4 (partitions array length) + 1 (tag_buffer)

        const topicBufferSize = 2 + topicName.length + 16 + 2 + 1 + 4 + 1;

        const topicBuffer = Buffer.alloc(topicBufferSize);

        let topicOffset = 0;

        // Write topic name

        topicOffset = writeString(topicBuffer, topicName, topicOffset);

        // Write topic_id (UUID) - all zeros for unknown topic

        for (let j = 0; j < 16; j++) {
          topicBuffer.writeUInt8(0, topicOffset + j);
        }

        topicOffset += 16;

        // Write error_code (3 = UNKNOWN_TOPIC_OR_PARTITION)

        topicBuffer.writeInt16BE(UNKNOWN_TOPIC_OR_PARTITION, topicOffset);

        topicOffset += 2;

        // Write is_internal (BOOLEAN) - false (0)

        topicBuffer.writeUInt8(0, topicOffset);

        topicOffset += 1;

        // Write partitions array length (INT32) - 0 (empty)

        topicBuffer.writeInt32BE(0, topicOffset);

        topicOffset += 4;

        // Write tag_buffer - empty (1 byte with value 0)

        topicBuffer.writeUInt8(0, topicOffset);

        buffers.push(topicBuffer);
      }

      // Create the response header

      const headerBuffer = Buffer.alloc(14); // 4 (message size) + 4 (correlation ID) + 2 (error code) + 4 (throttle_time_ms)

      let headerOffset = 0;

      // Message size will be calculated later

      headerOffset += 4;

      // Correlation ID

      headerBuffer.writeInt32BE(correlationId, headerOffset);

      headerOffset += 4;

      // Error code (0 = SUCCESS)

      headerBuffer.writeInt16BE(SUCCESS, headerOffset);

      headerOffset += 2;

      // Throttle time (0)

      headerBuffer.writeInt32BE(0, headerOffset);

      headerOffset += 4;

      // Create the topics array length buffer

      const topicsLengthBuffer = Buffer.alloc(4);

      topicsLengthBuffer.writeInt32BE(topicsLength, 0);

      // Create the tag buffer for the response

      const responseTagBuffer = Buffer.alloc(1);

      responseTagBuffer.writeUInt8(0, 0);

      // Combine all buffers

      const combinedBuffers = [
        headerBuffer,
        topicsLengthBuffer,
        ...buffers,
        responseTagBuffer,
      ];

      response = Buffer.concat(combinedBuffers);

      // Calculate and set the message size

      response.writeInt32BE(response.length - 4, 0);
    } else {
      // For other requests, just return a header with correlation ID

      console.log(`Processing unsupported request with API key ${apiKey}`);

      response = Buffer.alloc(8);

      // Message size (4 bytes) - 4 bytes for the header

      response.writeInt32BE(4, 0);

      // Correlation ID (4 bytes) - from the request

      response.writeInt32BE(correlationId, 4);
    }

    // Send the response

    console.log(`Sending response of ${response.length} bytes`);

    connection.write(response);
  });
});

server.listen(9092, "127.0.0.1", () => {
  console.log("Kafka server listening on port 9092");
});
