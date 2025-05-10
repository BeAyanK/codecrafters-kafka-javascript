import { createServer } from "net";
import { Buffer } from "buffer";

const server = createServer((connection) => {
  connection.on("data", (data) => {
    if (data.length < 12) {
      console.error("Request too small");
      return;
    }

    // Parse correlation ID from request header
    const correlationId = data.readInt32BE(8);

    // Define supported API versions
    const apiVersions = [
      {
        apiKey: 18,       // API_VERSIONS
        minVersion: 0,
        maxVersion: 4
      }
    ];

    // Calculate response body length:
    // error_code (2) + array length (4) + each entry (6 * N) + throttle_time_ms (4) + TAG_SECTION (1)
    const bodyLength = 2 + 4 + (apiVersions.length * 6) + 4 + 1;
    const headerLength = 4; // correlation_id
    const messageSize = headerLength + bodyLength;

    // Allocate full buffer (4 extra bytes for the message length prefix)
    const response = Buffer.alloc(4 + messageSize);
    let offset = 0;

    // Write message_size (excludes these 4 bytes)
    response.writeInt32BE(messageSize, offset);
    offset += 4;

    // Write correlation_id
    response.writeInt32BE(correlationId, offset);
    offset += 4;

    // Write error_code
    response.writeInt16BE(0, offset); // NO_ERROR
    offset += 2;

    // Write api_versions count
    response.writeInt32BE(apiVersions.length, offset);
    offset += 4;

    // Write each api_version
    apiVersions.forEach(api => {
      response.writeInt16BE(api.apiKey, offset);
      offset += 2;
      response.writeInt16BE(api.minVersion, offset);
      offset += 2;
      response.writeInt16BE(api.maxVersion, offset);
      offset += 2;
    });

    // Write throttle_time_ms
    response.writeInt32BE(0, offset);
    offset += 4;

    // Write TAG_SECTION (empty, just a single 0 byte)
    response.writeUInt8(0, offset);
    offset += 1;

    // Final buffer size check
    if (offset !== response.length) {
      console.error(`Buffer mismatch: expected ${response.length}, wrote ${offset}`);
      return;
    }

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
