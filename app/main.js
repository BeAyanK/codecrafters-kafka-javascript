import net from "net";


// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
  // Handle connection
});

server.listen(9092, "127.0.0.1");
import net from "net";
import { Buffer } from "buffer";

const server = net.createServer((connection) => {
  connection.on("data", (data) => {
    // For this stage, we ignore the request data
    
    // Create the response buffer
    const response = Buffer.alloc(8); // 4 bytes for message_size + 4 bytes for correlation_id
    
    // Set message_size (4 bytes) - for this stage, value doesn't matter
    response.writeInt32BE(0, 0); // Writing 0 as a placeholder
    
    // Set correlation_id (4 bytes) - must be 7
    response.writeInt32BE(7, 4); // Writing 7 at offset 4
    
    // Send the response
    connection.write(response);
  });
});

server.listen(9092, "127.0.0.1");
