import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { Readable } from "stream";
import {
  Converter,
  Data,
  hasReadableStream,
  isBlob,
  isBuffer,
  isNode,
  isReadable,
  isReadableStream,
  isReadableStreamData,
} from "univ-conv";
import {
  AbstractFile,
  ErrorLike,
  NotFoundError,
  OpenOptions,
  WriteOptions,
} from "univ-fs";
import { S3FileSystem } from "./S3FileSystem";

export class S3File extends AbstractFile {
  constructor(private s3fs: S3FileSystem, path: string) {
    super(s3fs, path);
  }

  public async _rm(): Promise<void> {
    const s3fs = this.s3fs;
    const path = this.path;

    try {
      const cmd = new DeleteObjectCommand(s3fs._createCommand(path));
      await s3fs.s3.send(cmd);
    } catch (e) {
      throw s3fs._error(path, e, false);
    }
  }

  protected async _load(
    _options: OpenOptions // eslint-disable-line
  ): Promise<Data> {
    const s3fs = this.s3fs;
    const path = this.path;
    const cmd = new GetObjectCommand(s3fs._createCommand(path));

    try {
      const obj = await s3fs.s3.send(cmd);
      return obj.Body || "";
    } catch (e) {
      throw s3fs._error(path, e, true);
    }
  }

  protected async _save(data: Data, options: WriteOptions): Promise<void> {
    const s3fs = this.s3fs;
    const path = this.path;
    const converter = new Converter(options);

    let head: Data | undefined;
    if (options.append) {
      try {
        head = await this._load(options);
      } catch (e: unknown) {
        if ((e as ErrorLike).name !== NotFoundError.name) {
          throw e;
        }
      }
    }
    let body: string | Readable | ReadableStream<unknown> | Blob | Uint8Array;
    if (head) {
      if (typeof head === "string") {
        body = await converter.merge([head, data], "UTF8");
      } else if (isReadable(head) || isNode) {
        body = await converter.merge([head, data], "Readable");
      } else if (isReadableStream(head) || hasReadableStream) {
        body = await converter.merge([head, data], "ReadableStream");
      } else {
        body = await converter.merge([head, data], "Uint8Array");
      }
    } else {
      if (
        typeof data === "string" ||
        isBlob(data) ||
        isBuffer(data) ||
        isReadableStreamData(data)
      ) {
        body = data;
      } else {
        body = await converter.toUint8Array(data);
      }
    }

    let length: number | undefined;
    if (!isReadableStreamData(data)) {
      length = await converter.getSize(body);
    }

    try {
      const cmd = new PutObjectCommand({
        ...s3fs._createCommand(path),
        Body: body,
        ContentLength: length,
      });
      await s3fs.s3.send(cmd);
    } catch (e) {
      throw s3fs._error(path, e, false);
    }
  }
}
