const readFromFileBuffer = (topicName, partitionIndex) => {
  try {
    const partitionNum = partitionIndex.readInt32BE();
    const logFilePath = `/tmp/kraft-combined-logs/${topicName}-${partitionNum}/00000000000000000000.log`;
    
    if (!fs.existsSync(logFilePath)) {
      return Buffer.from([0]); // Return empty batch if no file
    }

    const logFile = fs.readFileSync(logFilePath);
    
    // Create a simple RecordBatch with the file contents
    const baseOffset = Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]); // int64
    const batchLength = Buffer.from([0, 0, 0, logFile.length]); // int32
    const partitionLeaderEpoch = Buffer.from([0, 0, 0, 0]); // int32
    const magicByte = Buffer.from([2]); // int8
    const crc = Buffer.from([0, 0, 0, 0]); // int32 (would normally calculate this)
    const attributes = Buffer.from([0]); // int16
    const lastOffsetDelta = Buffer.from([0, 0]); // int32
    const baseTimestamp = Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]); // int64
    const maxTimestamp = Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]); // int64
    const producerId = Buffer.from([0, 0, 0, 0, 0, 0, 0, 0]); // int64
    const producerEpoch = Buffer.from([0, 0]); // int16
    const baseSequence = Buffer.from([0, 0, 0, 0]); // int32
    const records = Buffer.from([0, 0, 0, 1]); // int32 (record count)
    
    return Buffer.concat([
      baseOffset,
      batchLength,
      partitionLeaderEpoch,
      magicByte,
      crc,
      attributes,
      lastOffsetDelta,
      baseTimestamp,
      maxTimestamp,
      producerId,
      producerEpoch,
      baseSequence,
      records,
      logFile // actual message data
    ]);
  } catch (error) {
    console.error("Error reading log file:", error);
    return Buffer.from([0]); // Return empty batch on error
  }
};
