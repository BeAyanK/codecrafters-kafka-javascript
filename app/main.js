import { createServer } from "net";
import { Buffer } from "buffer";

const server = createServer((connection) => {
  connection.on("data", (data) => {
    if (data.length < 12) {
      console.error("Request too small");
      return;
    }

    // Parse correlation ID from request header (offset 8-11)
    const correlationId = data.readInt32BE(8);

    // Define supported API versions
    const apiVersions = [
      {
        apiKey: 18,       // API_VERSIONS
        minVersion: 0,
        maxVersion: 4
      }
    ];

    // Prepare response body:
    // error_code (2) + throttle_time_ms (4) + api_versions count (4) + 6 * number of versions
    const bodyLength = 2 + 4 + 4 + (apiVersions.length * 6);
    const headerLength = 4; // correlation_id
    const messageSize = headerLength + bodyLength;

    // Allocate full buffer including 4 bytes for message size prefix
    const response = Buffer.alloc(4 + messageSize);
    let offset = 0;

    // 1. Write message_size (excludes these 4 bytes)
    response.writeInt32BE(messageSize, offset);
    offset += 4;

    // 2. Write correlation_id
    response.writeInt32BE(correlationId, offset);
    offset += 4;

    // 3. Write error_code (2 bytes)
    response.writeInt16BE(0, offset); // 0 = NO_ERROR
    offset += 2;

    // 4. Write throttle_time_ms (4 bytes)
    response.writeInt32BE(0, offset); // No throttling
    offset += 4;

    // 5. Write number of API versions (4 bytes)
    response.writeInt32BE(apiVersions.length, offset);
    offset += 4;

    // 6. Write each API version (6 bytes each)
    apiVersions.forEach(api => {
      response.writeInt16BE(api.apiKey, offset);
      offset += 2;
      response.writeInt16BE(api.minVersion, offset);
      offset += 2;
      response.writeInt16BE(api.maxVersion, offset);
      offset += 2;
    });

    // Sanity check: buffer offset should match allocated size
    if (offset !== response.length) {
      console.error(`Buffer error: wrote ${offset} bytes but allocated ${response.length}`);
      return;
    }

    // Send response
    connection.write(response);
  });

  connection.on("error", (err) => {
    console.error("Connection error:", err);
  });
});

server.on("error", (err) => {
  console.error("Server error:", err);
});

server.listen(9092, "127.0.0.1", () => {
  console.log("Server listening on port 9092");
});

process.on("SIGTERM", () => {
  server.close();
  process.exit(0);
});
