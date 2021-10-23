import { GetObjectCommand } from "@aws-sdk/client-s3";
import { Readable } from "stream";
import { isBlob } from "univ-conv";
import {
  AbstractReadStream,
  OpenReadOptions,
  Source,
  SourceType,
} from "univ-fs";
import { S3File } from "./S3File";
import { isReadableStream } from "./S3Util";

export class S3ReadStream extends AbstractReadStream {
  private body: Blob | Readable | ReadableStream | undefined;
  private remaining = new Uint8Array([]);

  constructor(private s3File: S3File, options: OpenReadOptions) {
    super(s3File, options);
  }

  public async _close(): Promise<void> {
    if (!this.body) {
      return;
    }

    if (isBlob(this.body)) {
    } else if (isReadableStream(this.body)) {
      const stream = this.body as ReadableStream;
      stream.cancel();
    } else {
      const readable = this.body as Readable;
      readable.removeAllListeners();
      readable.destroy();
    }
    delete this.body;
  }

  public async _read(size?: number): Promise<Source | null> {
    const body = await this._buildBody();
    const s3FS = this.s3File.s3FS;
    const path = this.s3File.path;
    if (isBlob(body)) {
      const blob = body as Blob;
      const length = blob.size;
      if (length <= this.position) {
        return null;
      }
      let end = this.position + (size == null ? this.bufferSize : size);
      if (length < end) {
        end = length;
      }
      return blob.slice(this.position, end);
    } else if (isReadableStream(body)) {
      const u8 = new Uint8Array(this.remaining);
      this.remaining = new Uint8Array([]);
      const offset = u8.byteLength;
      const stream = body;
      const reader = stream.getReader();
      let res = await reader.read();
      while (!res.done) {
        const chunk = await this.converter.toUint8Array(res.value);
        if (size != null && size < u8.byteLength + chunk.byteLength) {
          const headLength = size - u8.byteLength;
          const head = chunk.slice(0, headLength);
          u8.set(head, offset);
          this.remaining = chunk.slice(headLength);
          break;
        } else {
          u8.set(chunk, offset);
        }
        res = await reader.read();
      }
      return u8;
    } else {
      const readable = body as Readable;
      return new Promise<Source | null>((resolve, reject) => {
        const onError = (err: Error) => {
          reject(s3FS._error(path, err, true));
          this._close();
        };
        readable.on("error", onError);
        const onEnd = () => {
          resolve(null);
          this._close();
        };
        readable.on("end", onEnd);
        const onReadable = () => {
          const buffer: Buffer = size ? readable.read(size) : readable.read();
          if (buffer) {
            resolve(buffer);
          } else {
            resolve(null);
          }
          readable.removeAllListeners();
        };
        readable.on("readable", onReadable);
      });
    }
  }

  public async _seek(start: number): Promise<void> {
    this._close();
    this._buildBody(start);
  }

  protected getDefaultSourceType(): SourceType {
    return "Buffer";
  }

  private async _buildBody(start?: number) {
    if (this.body) {
      if (start) {
        this._close();
      } else {
        return this.body;
      }
    }

    const s3FS = this.s3File.s3FS;
    const path = this.s3File.path;
    const cmd = new GetObjectCommand(s3FS._createCommand(path));
    try {
      const obj = await s3FS.s3.send(cmd);
      this.body = obj.Body;
      return this.body;
    } catch (e) {
      throw s3FS._error(path, e, true);
    }
  }
}
