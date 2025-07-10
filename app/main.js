// main.js
import { parseApiVersionsRequest, writeHeaderAndApiVersionsResponse } from './parser.js';
import net from 'net';

const server = net.createServer((socket) => {
  socket.on('data', (data) => {
    try {
      const { apiKey, apiVersion, correlationId } = parseApiVersionsRequest(data);

      if (apiKey === 18) {  // Check for ApiVersions request
        const response = writeHeaderAndApiVersionsResponse(correlationId, apiVersion);
        socket.write(response);
      } else {
        console.log(`Unhandled API Key: ${apiKey}`);
      }
    } catch (error) {
      console.error('Error handling data:', error);
      socket.destroy();
    }
  });

  socket.on('error', (err) => {
    console.error('Socket error:', err);
  });
});

server.listen(9092, () => {
  console.log('Server listening on port 9092');
});
