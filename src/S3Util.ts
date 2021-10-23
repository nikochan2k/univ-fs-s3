function isStream(stream: any) {
  return (
    stream !== null &&
    typeof stream === "object" &&
    typeof stream.pipe === "function"
  );
}

export function isWritableStream(stream: any): stream is WritableStream {
  return (
    isStream(stream) &&
    stream.writable !== false &&
    typeof stream._write === "function" &&
    typeof stream._writableState === "object"
  );
}

export function isReadableStream(stream: any): stream is ReadableStream<any> {
  return (
    isStream(stream) &&
    stream.readable !== false &&
    typeof stream._read === "function" &&
    typeof stream._readableState === "object"
  );
}
