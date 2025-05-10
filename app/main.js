import { createServer } from "net";
import { Buffer } from "buffer";

const server = createServer((connection) => {
  connection.on("data", (data) => {
    if (data.length < 12) {
      console.error("Request too small");
      return;
    }

    // Parse header fields
    const requestApiKey = data.readInt16BE(4);
    const requestApiVersion = data.readInt16BE(6);
    const correlationId = data.readInt32BE(8);

    // Create APIVersions response
    const apiVersions = [
      {
        apiKey: 18, // API_VERSIONS
        minVersion: 0,
        maxVersion: 4
      }
    ];

    // Calculate response size
    // Header: 4 (message_size) + 4 (correlation_id)
    // Body: 2 (error_code) + 4 (throttle_time_ms) + 4 (api_versions array length) + 6 per api_version
    const bodySize = 2 + 4 + 4 + (apiVersions.length * 6);
    const responseSize = 4 + bodySize; // Total response size

    const response = Buffer.alloc(responseSize);
    let offset = 0;

    // Write message_size (4 bytes) - size of remaining message (header + body)
    response.writeInt32BE(4 + bodySize, offset); // 4 for header + bodySize
    offset += 4;

    // Write correlation_id (4 bytes)
    response.writeInt32BE(correlationId, offset);
    offset += 4;

    // Write error_code (2 bytes) - 0 (NO_ERROR)
    response.writeInt16BE(0, offset);
    offset += 2;

    // Write throttle_time_ms (4 bytes) - 0
    response.writeInt32BE(0, offset);
    offset += 4;

    // Write api_versions array length (4 bytes)
    response.writeInt32BE(apiVersions.length, offset);
    offset += 4;

    // Write each api_version entry (6 bytes each)
    for (const api of apiVersions) {
      response.writeInt16BE(api.apiKey, offset);
      offset += 2;
      response.writeInt16BE(api.minVersion, offset);
      offset += 2;
      response.writeInt16BE(api.maxVersion, offset);
      offset += 2;
    }

    // Verify we filled exactly the buffer size
    if (offset !== responseSize) {
      console.error(`Buffer size mismatch: wrote ${offset} bytes, expected ${responseSize}`);
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
