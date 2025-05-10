import { createServer } from "net";
import { Buffer } from "buffer";

const server = createServer((connection) => {
  connection.on("data", (data) => {
    if (data.length < 12) {
      console.error("Request too small");
      return;
    }

    // Parse header fields
    const correlationId = data.readInt32BE(8);

    // Create APIVersions response data
    const apiVersions = [{
      apiKey: 18,    // API_VERSIONS
      minVersion: 0,
      maxVersion: 4
    }];

    // Calculate sizes:
    // Header: 4 (correlation_id)
    // Body: 2 (error_code) + 4 (throttle_time_ms) + 4 (api_versions count) + 6 (per api_version)
    const bodySize = 2 + 4 + 4 + (apiVersions.length * 6);
    const totalSize = 4 + bodySize; // 4 (message_size) + bodySize

    // Create buffer with exact required size
    const response = Buffer.alloc(totalSize);
    let offset = 0;

    // 1. Write message_size (4 bytes) - size of everything after this field
    response.writeInt32BE(bodySize, offset); // bodySize = header + body
    offset += 4;

    // 2. Write correlation_id (4 bytes)
    response.writeInt32BE(correlationId, offset);
    offset += 4;

    // 3. Write error_code (2 bytes)
    response.writeInt16BE(0, offset); // 0 = NO_ERROR
    offset += 2;

    // 4. Write throttle_time_ms (4 bytes)
    response.writeInt32BE(0, offset); // 0 = no throttle
    offset += 4;

    // 5. Write api_versions array length (4 bytes)
    response.writeInt32BE(apiVersions.length, offset);
    offset += 4;

    // 6. Write each api_version entry (6 bytes each)
    apiVersions.forEach(api => {
      response.writeInt16BE(api.apiKey, offset);
      offset += 2;
      response.writeInt16BE(api.minVersion, offset);
      offset += 2;
      response.writeInt16BE(api.maxVersion, offset);
      offset += 2;
    });

    // Verify we filled exactly the buffer size
    if (offset !== totalSize) {
      console.error(`Buffer error: wrote ${offset} bytes but allocated ${totalSize}`);
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
