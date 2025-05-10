import { createServer } from "net";
import { Buffer } from "buffer";

const server = createServer((connection) => {
  connection.on("data", (data) => {
    // Create response buffer (4 bytes message_size + 4 bytes correlation_id)
    const response = Buffer.alloc(8);
    
    // Write message_size (4 bytes) - value 0 for this stage
    response.writeInt32BE(0, 0);
    
    // Write correlation_id (4 bytes) - must be 7
    response.writeInt32BE(7, 4);
    
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
