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

    // Create response buffer
    let response;
    
    // Check if ApiVersions version is supported (0-4)
    if (requestApiVersion < 0 || requestApiVersion > 4) {
      // Create error response (4 bytes message_size + 4 bytes correlation_id + 2 bytes error_code)
      response = Buffer.alloc(10);
      
      // Write message_size (4 bytes) - value 0 for this stage
      response.writeInt32BE(0, 0);
      
      // Write correlation_id (4 bytes)
      response.writeInt32BE(correlationId, 4);
      
      // Write error_code (2 bytes) - 35 (UNSUPPORTED_VERSION)
      response.writeInt16BE(35, 8);
    } else {
      // Create basic response (4 bytes message_size + 4 bytes correlation_id)
      response = Buffer.alloc(8);
      
      // Write message_size (4 bytes) - value 0 for this stage
      response.writeInt32BE(0, 0);
      
      // Write correlation_id (4 bytes)
      response.writeInt32BE(correlationId, 4);
    }
    
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
