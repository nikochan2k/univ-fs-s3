import * as fs from "fs";
import {
  AbstractReadStream,
  OpenReadOptions,
  Source,
  SourceType,
} from "univ-fs";
import { S3File } from "./S3File";
import { convertError } from "./S3FileSystem";

export class S3ReadStream extends AbstractReadStream {
  private readStream?: fs.ReadStream;

  constructor(private s3File: S3File, options: OpenReadOptions) {
    super(s3File, options);
  }

  public async _close(): Promise<void> {}

  public _read(size?: number): Promise<Source | null> {
    const readStream = this._buildReadStream();
    return new Promise<Source | null>((resolve, reject) => {
      const nodeFile = this.s3File;
      const onError = (err: Error) => {
        reject(convertError(nodeFile.fs.repository, nodeFile.path, err, false));
        this._destory();
      };
      readStream.on("error", onError);
      const onEnd = () => {
        resolve(null);
        this._destory();
      };
      readStream.on("end", onEnd);
      const onReadable = () => {
        const buffer: Buffer = size ? readStream.read(size) : readStream.read();
        if (buffer) {
          resolve(buffer);
        } else {
          resolve(null);
        }
        readStream.removeAllListeners();
      };
      readStream.on("readable", onReadable);
    });
  }

  public async _seek(start: number): Promise<void> {
    this._destory();
    this._buildReadStream(start);
  }

  protected getDefaultSourceType(): SourceType {
    return "Buffer";
  }

  private _buildReadStream(start?: number) {
    if (this.readStream && !this.readStream.destroyed) {
      if (start) {
        this._destory();
      } else {
        return this.readStream;
      }
    }

    const nodeFile = this.s3File;
    const repository = nodeFile.fs.repository;
    const path = nodeFile.path;
    try {
      this.readStream = fs.createReadStream(nodeFile._getFullPath(), {
        flags: "r",
        highWaterMark: this.bufferSize,
        start,
      });
      return this.readStream;
    } catch (e) {
      throw convertError(repository, path, e, false);
    }
  }

  private _destory() {
    if (!this.readStream) {
      return;
    }

    this.readStream.removeAllListeners();
    this.readStream.destroy();
    this.readStream = undefined;
  }
}
