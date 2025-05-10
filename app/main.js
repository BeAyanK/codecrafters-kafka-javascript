import net from "net";


// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
  // Handle connection
});

server.listen(9092, "127.0.0.1");
