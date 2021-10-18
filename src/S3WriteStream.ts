import * as fs from "fs";
import {
  AbstractWriteStream,
  createError,
  NoModificationAllowedError,
  OpenWriteOptions,
  Source,
} from "univ-fs";
import { S3File } from "./S3File";
import { convertError } from "./S3FileSystem";

export class S3WriteStream extends AbstractWriteStream {
  private writeStream?: fs.WriteStream;

  constructor(private nodeFile: S3File, options: OpenWriteOptions) {
    super(nodeFile, options);
  }

  public _buildWriteStream(start?: number) {
    const writeStream = this.writeStream;
    if (writeStream && !writeStream.destroyed) {
      if (start != null) {
        this._destory();
      } else {
        return writeStream;
      }
    }

    const nodeFile = this.nodeFile;
    try {
      const flags =
        (start != null ? "a" : "w") + (this.options.create ? "" : "x");
      this.writeStream = fs.createWriteStream(nodeFile._getFullPath(), {
        flags,
        highWaterMark: this.bufferSize,
        start,
      });
      return this.writeStream;
    } catch (e) {
      throw convertError(nodeFile.fs.repository, nodeFile.path, e, true);
    }
  }

  public async _close(): Promise<void> {
    this._destory();
  }

  public async _truncate(len: number): Promise<void> {
    this._destory();

    return new Promise<void>((resolve, reject) => {
      const nodeFile = this.nodeFile;
      fs.truncate(nodeFile._getFullPath(), len, (e) => {
        if (e) {
          reject(
            createError({
              name: NoModificationAllowedError.name,
              repository: nodeFile.fs.repository,
              path: nodeFile.path,
              e,
            })
          );
          return;
        }
        resolve();
      });
    });
  }

  public async _write(src: Source): Promise<number> {
    const writeStream = this.writeStream as fs.WriteStream;
    return new Promise<number>(async (resolve, reject) => {
      const nodeBuffer = await this.converter.toBuffer(src);
      writeStream.write(nodeBuffer, (err) => {
        if (err) {
          const file = this.file;
          reject(convertError(file.fs.repository, file.path, err, true));
          return;
        }
        resolve(nodeBuffer.byteLength);
      });
    });
  }

  protected async _seek(start: number): Promise<void> {
    this._buildWriteStream(start);
  }

  private async _destory(): Promise<void> {
    if (!this.writeStream) {
      return;
    }

    this.writeStream.removeAllListeners();
    this.writeStream.destroy();
    delete this.writeStream;
  }
}
