import { createServer } from "net";
import { Buffer } from "buffer";

const server = createServer((connection) => {
  connection.on("data", (data) => {
    // Minimum request size check (4 bytes message_size + 8 bytes header)
    if (data.length < 12) {
      console.error("Request too small");
      return;
    }

    // Parse correlation_id from request
    // message_size: 4 bytes (we don't need it here)
    // request_api_key: 2 bytes (offset 4)
    // request_api_version: 2 bytes (offset 6)
    // correlation_id: 4 bytes (offset 8)
    const correlationId = data.readInt32BE(8);

    // Create response buffer (4 bytes message_size + 4 bytes correlation_id)
    const response = Buffer.alloc(8);
    
    // Write message_size (4 bytes) - value 0 for this stage
    response.writeInt32BE(0, 0);
    
    // Write correlation_id (4 bytes) - from the request
    response.writeInt32BE(correlationId, 4);
    
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
