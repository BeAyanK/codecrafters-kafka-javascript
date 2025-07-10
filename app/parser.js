// parser.js

function parseKafkaString(buffer, offset) {
  const length = buffer.readInt16BE(offset);
  offset += 2;
  const value = buffer.slice(offset, offset + length).toString("utf-8");
  offset += length;
  return { value, offset };
}

export function parseApiVersionsRequest(buffer) {
  let offset = 0;

  const totalLength = buffer.readInt32BE(offset);
  offset += 4;

  const apiKey = buffer.readInt16BE(offset);
  offset += 2;

  const apiVersion = buffer.readInt16BE(offset);
  offset += 2;

  const correlationId = buffer.readInt32BE(offset);
  offset += 4;

  const clientId = parseKafkaString(buffer, offset);
  offset = clientId.offset;

  const softwareName = parseKafkaString(buffer, offset);
  offset = softwareName.offset;

  const softwareVersion = parseKafkaString(buffer, offset);
  offset = softwareVersion.offset;

  // Optional: tagged fields
  if (offset < buffer.length) {
    // Don't read if not enough bytes
    const taggedFieldsLength = buffer.readUInt8(offset);
    offset += 1 + taggedFieldsLength; // skip (not needed in our case)
  }

  return {
    apiKey,
    apiVersion,
    correlationId,
  };
}

export function writeHeaderAndApiVersionsResponse(correlationId, requestApiVersion) {
  // Create API versions array with required entries
  const apiVersions = [
    { apiKey: 18, minVersion: 0, maxVersion: 4 } // API_VERSIONS must have maxVersion >= 4
  ];

  // Calculate response size:
  // Header: correlationId (4) + errorCode (2)
  // Body: apiVersionsCount (4) + apiVersions entries (6 each) + throttleTimeMs (4) + taggedFields (1)
  const responseSize = 4 + 2 + 4 + (apiVersions.length * 6) + 4 + 1;

  // Create response buffer
  const response = Buffer.alloc(responseSize);
  let offset = 0;

  // Write correlation ID (4 bytes)
  response.writeInt32BE(correlationId, offset);
  offset += 4;

  // Write error code (2 bytes) - 0 for no error
  response.writeInt16BE(0, offset);
  offset += 2;

  // Write apiVersions array length (4 bytes)
  response.writeInt32BE(apiVersions.length, offset);
  offset += 4;

  // Write each API version entry (6 bytes each)
  for (const version of apiVersions) {
    response.writeInt16BE(version.apiKey, offset);
    response.writeInt16BE(version.minVersion, offset + 2);
    response.writeInt16BE(version.maxVersion, offset + 4);
    offset += 6;
  }

  // Write throttle time (4 bytes) - 0 for no throttling
  response.writeInt32BE(0, offset);
  offset += 4;

  // Write tagged fields (1 byte) - empty
  response.writeUInt8(0, offset);
  offset += 1;

  // Create final response with length prefix
  const finalResponse = Buffer.alloc(4 + response.length);
  finalResponse.writeInt32BE(response.length, 0); // message size prefix
  response.copy(finalResponse, 4);

  return finalResponse;
}
