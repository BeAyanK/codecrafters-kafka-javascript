import { createServer } from "net";
import { Buffer } from "buffer";

const server = createServer((connection) => {
  connection.on("data", (data) => {
    // Minimum request size check (4 bytes message_size + 8 bytes header)
    if (data.length < 12) {
      console.error("Request too small");
      return;
    }

    // Parse header fields
    const requestApiKey = data.readInt16BE(4);
    const requestApiVersion = data.readInt16BE(6);
    const correlationId = data.readInt32BE(8);

    // Create response body for APIVersions (v4)
    const apiVersions = [
      {
        apiKey: 18, // API_VERSIONS
        minVersion: 0,
        maxVersion: 4
      }
      // Add more API keys as needed
    ];

    // Calculate response body size:
    // error_code (2) + [api_versions array]
    let bodySize = 2;
    bodySize += 1; // api_versions array length (1 byte for compact array)
    apiVersions.forEach(() => {
      bodySize += 2; // api_key
      bodySize += 2; // min_version
      bodySize += 2; // max_version
      bodySize += 1; // tagged fields (empty)
    });
    bodySize += 1; // tagged fields (empty)

    // Create response buffer (4 message_size + 4 correlation_id + bodySize)
    const response = Buffer.alloc(8 + bodySize);
    let offset = 0;

    // Write message_size (4 bytes) - total size minus these 4 bytes
    response.writeInt32BE(4 + bodySize, offset);
    offset += 4;

    // Write correlation_id (4 bytes)
    response.writeInt32BE(correlationId, offset);
    offset += 4;

    // Write error_code (2 bytes) - 0 (NO_ERROR)
    response.writeInt16BE(0, offset);
    offset += 2;

    // Write api_versions array length (1 byte for compact array)
    response.writeUInt8(apiVersions.length, offset);
    offset += 1;

    // Write each api_version entry
    for (const api of apiVersions) {
      response.writeInt16BE(api.apiKey, offset);
      offset += 2;
      response.writeInt16BE(api.minVersion, offset);
      offset += 2;
      response.writeInt16BE(api.maxVersion, offset);
      offset += 2;
      // Write empty tagged fields (1 byte - 0)
      response.writeUInt8(0, offset);
      offset += 1;
    }

    // Write empty tagged fields (1 byte - 0)
    response.writeUInt8(0, offset);
    offset += 1;

    // Send the response
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

// Keep the process running
process.on("SIGTERM", () => {
  server.close();
  process.exit(0);
});
