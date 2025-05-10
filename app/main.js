const net = require("net");
const { parseApiVersionsRequest } = require("./parser"); // assuming you have this
const { writeHeaderAndApiVersionsResponse } = require("./response"); // the function we just fixed

const server = net.createServer((socket) => {
  socket.on("data", (data) => {
    try {
      const { apiKey, apiVersion, correlationId } = parseApiVersionsRequest(data);

      if (apiKey === 18) { // ApiVersions request
        const response = writeHeaderAndApiVersionsResponse(correlationId, apiVersion);
        socket.write(response);
      } else {
        console.log("Unknown apiKey:", apiKey);
      }
    } catch (err) {
      console.error("Error handling data:", err);
    }
  });
});

server.listen(9092, () => {
  console.log("Server listening on port 9092");
});


function parseKafkaString(buffer, offset) {
  const length = buffer.readInt16BE(offset);
  offset += 2;
  const value = buffer.slice(offset, offset + length).toString("utf-8");
  offset += length;
  return { value, offset };
}

function parseApiVersionsRequest(buffer) {
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

function writeHeaderAndApiVersionsResponse(correlationId, requestApiVersion) {
  const apiVersions = [
    { apiKey: 0, minVersion: 0, maxVersion: 3 },
    { apiKey: 1, minVersion: 0, maxVersion: 7 },
    { apiKey: 18, minVersion: 0, maxVersion: 3 },
  ];

  const apiVersionsCount = apiVersions.length;

  const errorCode = 0;
  const taggedFieldsSize = 1; // single byte, always 0

  // Response body size:
  // correlationId (4) + errorCode (2) + apiVersions (6 * count) + taggedFields (1)
  const responseBodySize = 4 + 2 + (6 * apiVersionsCount) + taggedFieldsSize;

  const responseBody = Buffer.alloc(responseBodySize);
  let offset = 0;

  // ResponseHeader: correlationId
  responseBody.writeInt32BE(correlationId, offset);
  offset += 4;

  // error_code
  responseBody.writeInt16BE(errorCode, offset);
  offset += 2;

  // num_api_keys
  responseBody.writeInt32BE(apiVersionsCount, offset);
  offset += 4;

  for (const version of apiVersions) {
    responseBody.writeInt16BE(version.apiKey, offset);
    offset += 2;
    responseBody.writeInt16BE(version.minVersion, offset);
    offset += 2;
    responseBody.writeInt16BE(version.maxVersion, offset);
    offset += 2;
  }

  // Tagged fields
  responseBody.writeUInt8(0x00, offset);
  offset += 1;

  const finalResponse = Buffer.alloc(4 + responseBody.length);
  finalResponse.writeInt32BE(responseBody.length, 0); // message size prefix
  responseBody.copy(finalResponse, 4);

  return finalResponse;
}

