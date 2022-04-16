import {
  DeleteObjectCommand,
  GetObjectCommand,
  GetObjectCommandInput,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { Readable } from "stream";
import {
  blobConverter,
  Data,
  hasBuffer,
  readableConverter,
  readableStreamConverter,
} from "univ-conv";
import { AbstractFile, ReadOptions, Stats, WriteOptions } from "univ-fs";
import { EMPTY_BUFFER } from "univ-conv";
import { S3FileSystem } from "./S3FileSystem";

export class S3File extends AbstractFile {
  constructor(private s3fs: S3FileSystem, path: string) {
    super(s3fs, path);
  }

  public async _rm(): Promise<void> {
    const s3fs = this.s3fs;
    const path = this.path;

    try {
      const cmd = new DeleteObjectCommand(s3fs._createCommand(path, false));
      const client = await s3fs._getClient();
      await client.send(cmd);
    } catch (e) {
      throw s3fs._error(path, e, true);
    }
  }

  public supportRangeRead(): boolean {
    return true;
  }

  public supportRangeWrite(): boolean {
    return false;
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected async _load(_stats: Stats, options: ReadOptions): Promise<Data> {
    const s3fs = this.s3fs;
    const path = this.path;
    const length = options.length;
    if (length === 0) {
      return EMPTY_BUFFER;
    }
    const start = options.start;
    let range: string | undefined;
    if (typeof start === "number" || typeof length === "number") {
      const s = typeof start === "number" ? start : 0;
      const e = typeof length === "number" ? (s + length - 1).toString() : "";
      range = `bytes=${s}-${e}`;
    }
    const cmdIn: GetObjectCommandInput = s3fs._createCommand(path, false);
    if (range) {
      cmdIn.Range = range;
    }
    const cmd = new GetObjectCommand(cmdIn);

    try {
      const client = await s3fs._getClient();
      const obj = await client.send(cmd);
      return obj.Body || "";
    } catch (e) {
      throw s3fs._error(path, e, false);
    }
  }

  protected async _save(
    data: Data,
    stats: Stats | undefined,
    options: WriteOptions
  ): Promise<void> {
    const s3fs = this.s3fs;
    const path = this.path;
    const converter = this._getConverter();

    try {
      let head: Data | undefined;
      if (options.append && stats) {
        head = await this._load(stats, options);
      }
      let body: Readable | ReadableStream<unknown> | Blob | Uint8Array;
      /* eslint-disable */
      if (head) {
        if (
          readableConverter().typeEquals(head) ||
          readableConverter().typeEquals(data)
        ) {
          body = await converter.merge([head, data], "readable", options);
        } else if (
          readableStreamConverter().typeEquals(head) ||
          readableStreamConverter().typeEquals(data)
        ) {
          body = await converter.merge([head, data], "readablestream", options);
        } else if (
          blobConverter().typeEquals(head) ||
          blobConverter().typeEquals(data)
        ) {
          body = await converter.merge([head, data], "blob", options);
        } else if (hasBuffer) {
          body = await converter.merge([head, data], "buffer", options);
        } else {
          body = await converter.merge([head, data], "uint8array", options);
        }
      } else {
        if (readableConverter().typeEquals(data)) {
          body = await converter.convert(data, "readable", options);
        } else if (readableStreamConverter().typeEquals(data)) {
          body = await converter.convert(data, "readablestream", options);
        } else if (blobConverter().typeEquals(data)) {
          body = await converter.convert(data, "blob", options);
        } else if (hasBuffer) {
          body = await converter.toBuffer(data);
        } else {
          body = await converter.toUint8Array(data);
        }
      }

      let metadata: { [key: string]: string } | undefined;
      if (stats) {
        metadata = s3fs._createMetadata(stats);
      }

      const client = await s3fs._getClient();
      if (
        readableConverter().typeEquals(body) ||
        readableStreamConverter().typeEquals(body)
      ) {
        const upload = new Upload({
          client,
          params: {
            ...s3fs._createCommand(path, false),
            Body: body,
            Metadata: metadata,
          },
        });
        await upload.done();
      } else {
        const length = await converter.getSize(body as Data);
        const cmd = new PutObjectCommand({
          ...s3fs._createCommand(path, false),
          Body: body,
          ContentLength: length,
          Metadata: metadata,
        });
        await client.send(cmd);
      }
      /* eslint-enable */
    } catch (e) {
      throw s3fs._error(path, e, true);
    }
  }
}
