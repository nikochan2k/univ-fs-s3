import { DeleteObjectCommand } from "@aws-sdk/client-s3";
import {
  AbstractFile,
  AbstractReadStream,
  AbstractWriteStream,
  OpenReadOptions,
  OpenWriteOptions,
} from "univ-fs";
import { S3FileSystem } from ".";
import { S3ReadStream } from "./S3ReadStream";
import { S3WriteStream } from "./S3WriteStream";

export class S3File extends AbstractFile {
  constructor(private s3FS: S3FileSystem, path: string) {
    super(s3FS, path);
  }

  public async _createReadStream(
    options: OpenReadOptions
  ): Promise<AbstractReadStream> {
    return new S3ReadStream(this, options);
  }

  public async _createWriteStream(
    options: OpenWriteOptions
  ): Promise<AbstractWriteStream> {
    return new S3WriteStream(this, options);
  }

  public async _rm(): Promise<void> {
    const s3FS = this.s3FS;
    const path = this.path;
    try {
      const cmd = new DeleteObjectCommand(s3FS._createCommand(path));
      await s3FS.s3.send(cmd);
    } catch (e) {
      throw s3FS._error(path, e, false);
    }
  }
}
