// parser.js

export function parseApiVersionsRequest(data) {
  // Parse the request header (assuming v4 format)
  // First 4 bytes: message length (we can ignore for request parsing)
  // Next 4 bytes: API key (int16)
  const apiKey = data.readInt16BE(4);
  // Next 2 bytes: API version (int16)
  const apiVersion = data.readInt16BE(6);
  // Next 4 bytes: correlation ID (int32)
  const correlationId = data.readInt32BE(8);
  
  return { apiKey, apiVersion, correlationId };
}

export function writeHeaderAndApiVersionsResponse(correlationId, apiVersion, apiVersions) {
  // Calculate message length:
  // Header: 4 (length) + 4 (correlationId) + 4 (error code + array length)
  // Body: For each apiVersion entry: 2 (apiKey) + 2 (min) + 2 (max) = 6 bytes
  const messageLength = 4 + 4 + (apiVersions.length * 6);
  
  // Create a buffer with enough space
  const buffer = Buffer.alloc(4 + messageLength); // +4 for the length itself
  
  // Write message length (4 bytes)
  buffer.writeInt32BE(messageLength, 0);
  
  // Write correlation ID (4 bytes)
  buffer.writeInt32BE(correlationId, 4);
  
  // Write error code (int16) - 0 for no error
  buffer.writeInt16BE(0, 8);
  
  // Write API versions array length (int16)
  buffer.writeInt16BE(apiVersions.length, 10);
  
  // Write each API version entry
  let offset = 12;
  for (const version of apiVersions) {
    buffer.writeInt16BE(version.apiKey, offset);
    buffer.writeInt16BE(version.minVersion, offset + 2);
    buffer.writeInt16BE(version.maxVersion, offset + 4);
    offset += 6;
  }
  
  return buffer;
}
