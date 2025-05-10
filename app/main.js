import net from "net";

const PORT = 9092;

const server = net.createServer((socket) => {
  console.log("Server listening on port", PORT);

  socket.on("data", (data) => {
    try {
      const { apiKey, apiVersion, correlationId } = parseApiVersionsRequest(data);

      if (apiKey === 18) {
        const response = writeHeaderAndApiVersionsResponse(correlationId);
        socket.write(response);
      } else {
        console.log("Unknown apiKey:", apiKey);
      }
    } catch (err) {
      console.error("Error handling data:", err);
    }
  });

  socket.on("error", (err) => {
    console.error("Socket error:", err);
  });
});

server.listen(PORT);

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

function writeHeaderAndApiVersionsResponse(correlationId) {
  const apiVersions = [
    { apiKey: 0, minVersion: 0, maxVersion: 3 },
    { apiKey: 1, minVersion: 0, maxVersion: 7 },
    { apiKey: 18, minVersion: 0, maxVersion: 3 },
  ];

  const apiVersionsCount = apiVersions.length;

  // Calculate exact response size
  const responseBodySize = 4 + 4 + (apiVersionsCount * 6) + 1; // correlationId + apiVersionsCount (int32) + versions + tagged fields

  const responseBody = Buffer.alloc(responseBodySize);
  let offset = 0;

  responseBody.writeInt32BE(correlationId, offset); // 4 bytes
  offset += 4;

  responseBody.writeInt32BE(apiVersionsCount, offset); // 4 bytes
  offset += 4;

  for (const version of apiVersions) {
    responseBody.writeInt16BE(version.apiKey, offset);
    offset += 2;
    responseBody.writeInt16BE(version.minVersion, offset);
    offset += 2;
    responseBody.writeInt16BE(version.maxVersion, offset);
    offset += 2;
  }

  responseBody.writeUInt8(0x00, offset); // tagged fields
  offset += 1;

  const finalResponse = Buffer.alloc(4 + responseBody.length);
  finalResponse.writeInt32BE(responseBody.length, 0);
  responseBody.copy(finalResponse, 4);

  return finalResponse;
}
