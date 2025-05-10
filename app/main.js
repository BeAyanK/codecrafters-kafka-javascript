// Importing necessary functions
import { parseApiVersionsRequest, writeHeaderAndApiVersionsResponse } from './parser.js'; // Assuming these are in parser.js
import net from 'net';

// Handle the socket connection and data parsing
const server = net.createServer((socket) => {
  socket.on('data', (data) => {
    try {
      const { apiKey, apiVersion, correlationId } = parseApiVersionsRequest(data);

      if (apiKey === 18) {  // Check for ApiVersions request
        const response = writeHeaderAndApiVersionsResponse(correlationId, apiVersion);
        socket.write(response);  // Send the response back to the client
      } else {
        // Handle other API key cases if necessary
        console.log(`Unhandled API Key: ${apiKey}`);
      }
    } catch (error) {
      console.error('Error handling data:', error);
      socket.destroy();  // Close the socket on error
    }
  });

  socket.on('error', (err) => {
    console.error('Socket error:', err);
  });
});

// Start the server
server.listen(9092, () => {
  console.log('Server listening on port 9092');
});


