import net from "net";
import fs from "fs";

const server = net.createServer();
const PORT = 9092;

function writeHeaderAndApiVersionsResponse(socket, correlationId) {
  const RESPONSE_HEADER_SIZE = 4 + 4; // response size + correlation id
  const ERROR_CODE = 0;
  const NUM_API_KEYS = 0;
  const THROTTLE_TIME_MS = 0;
  const TAG_BUFFER = 0; // empty tag buffer (UNSIGNED_VARINT = 0)

  // total message length after response size
  const responseBodyLength = 2 + 4 + 4 + 1; // errorCode + numApiKeys + throttleTime + tagBuffer
  const totalLength = 4 + responseBodyLength; // correlationId + responseBody

  const buffer = Buffer.alloc(4 + totalLength); // 4 bytes for totalLength prefix
  let offset = 0;

  buffer.writeInt32BE(totalLength, offset); // total length (excluding this int)
  offset += 4;

  buffer.writeInt32BE(correlationId, offset); // correlation id
  offset += 4;

  buffer.writeInt16BE(ERROR_CODE, offset); // error code
  offset += 2;

  buffer.writeInt32BE(NUM_API_KEYS, offset); // number of api keys
  offset += 4;

  buffer.writeInt32BE(THROTTLE_TIME_MS, offset); // throttle time
  offset += 4;

  buffer.writeUInt8(TAG_BUFFER, offset); // tag buffer (unsigned varint 0)
  offset += 1;

  socket.write(buffer);
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

  // clientId is next, which is a Kafka string: 2-byte length prefix
  const clientIdLength = buffer.readInt16BE(offset);
  offset += 2 + clientIdLength;

  // clientSoftwareName and clientSoftwareVersion are both Kafka strings
  const nameLength = buffer.readInt16BE(offset);
  offset += 2 + nameLength;

  const versionLength = buffer.readInt16BE(offset);
  offset += 2 + versionLength;

  // apiVersion 3 and 4 contain a tagged fields section — skip it
  const taggedField = buffer.readUInt8(offset);
  offset += 1;

  return {
    apiKey,
    apiVersion,
    correlationId,
  };
}

server.on("connection", (socket) => {
  socket.on("data", (data) => {
    try {
      const { apiKey, apiVersion, correlationId } = parseApiVersionsRequest(data);

      if (apiKey === 18 && (apiVersion === 3 || apiVersion === 4)) {
        writeHeaderAndApiVersionsResponse(socket, correlationId);
      } else {
        console.log(`Unsupported API Key: ${apiKey} or version: ${apiVersion}`);
      }
    } catch (err) {
      console.error("Error handling data:", err);
    }
  });
});

server.listen(PORT, () => {
  console.log(`Server listening on port ${PORT}`);
});
