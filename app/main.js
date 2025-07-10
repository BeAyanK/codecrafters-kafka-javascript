import net from "net";

console.log("Logs from your program will appear here!");

// Constants
const API_VERSIONS_KEY = 18;  // ApiVersions API key
const DESCRIBE_TOPIC_PARTITIONS_KEY = 75;  // DescribeTopicPartitions API key
const UNSUPPORTED_VERSION = 35;  // Error code for unsupported version
const SUCCESS = 0;  // Success code

// Version ranges
const API_VERSIONS_MIN = 0;
const API_VERSIONS_MAX = 4;
const DESCRIBE_TOPIC_PARTITIONS_MIN = 0;
const DESCRIBE_TOPIC_PARTITIONS_MAX = 0;

const server = net.createServer((connection) => {
  connection.on('data', (data) => {
    // Extract request details
    const apiKey = data.readInt16BE(4);
    const apiVersion = data.readInt16BE(6);
    const correlationId = data.readInt32BE(8);
    
    console.log(`Received request: apiKey=${apiKey}, apiVersion=${apiVersion}, correlationId=${correlationId}`);
    
    let response;
    
    if (apiKey === API_VERSIONS_KEY) {
      // Handle ApiVersions request
      console.log("Processing ApiVersions request");
      
      // Create response with both API entries
      response = Buffer.alloc(31); // Total response size
      let offset = 0;
      
      // Message size (4 bytes) - 27 bytes for the rest of the message
      response.writeInt32BE(27, offset);
      offset += 4;
      
      // Correlation ID (4 bytes) - from the request
      response.writeInt32BE(correlationId, offset);
      offset += 4;
      
      // Check if API version is supported
      if (apiVersion < API_VERSIONS_MIN || apiVersion > API_VERSIONS_MAX) {
        console.log(`Unsupported version ${apiVersion}, responding with error code ${UNSUPPORTED_VERSION}`);
        response.writeInt16BE(UNSUPPORTED_VERSION, offset);
      } else {
        console.log(`Supported version ${apiVersion}, responding with success`);
        response.writeInt16BE(SUCCESS, offset);
      }
      offset += 2;
      
      // API keys array length (1 byte) - 3 in COMPACT_ARRAY format (N+1)
      // We have 2 APIs (18 and 75), so N+1 = 3
      response.writeUInt8(3, offset);
      offset += 1;
      
      // First API key entry (18 - ApiVersions)
      response.writeInt16BE(API_VERSIONS_KEY, offset);
      offset += 2;
      response.writeInt16BE(API_VERSIONS_MIN, offset);
      offset += 2;
      response.writeInt16BE(API_VERSIONS_MAX, offset);
      offset += 2;
      response.writeUInt8(0, offset); // No tagged fields
      offset += 1;
      
      // Second API key entry (75 - DescribeTopicPartitions)
      response.writeInt16BE(DESCRIBE_TOPIC_PARTITIONS_KEY, offset);
      offset += 2;
      response.writeInt16BE(DESCRIBE_TOPIC_PARTITIONS_MIN, offset);
      offset += 2;
      response.writeInt16BE(DESCRIBE_TOPIC_PARTITIONS_MAX, offset);
      offset += 2;
      response.writeUInt8(0, offset); // No tagged fields
      offset += 1;
      
      // Throttle time (4 bytes) - 0
      response.writeInt32BE(0, offset);
      offset += 4;
      
      // Response tag buffer (1 byte) - 0 = no tagged fields
      response.writeUInt8(0, offset);
    } else {
      // For non-ApiVersions requests, just return a header with correlation ID
      console.log("Processing non-ApiVersions request");
      
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
