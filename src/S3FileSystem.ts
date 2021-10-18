import {
  CopyObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  HeadObjectCommandOutput,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
  S3ClientConfig,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import {
  AbstractDirectory,
  AbstractFile,
  AbstractFileSystem,
  createError,
  FileSystemOptions,
  getPathParts,
  HeadOptions,
  NoModificationAllowedError,
  NotFoundError,
  NotReadableError,
  NotSupportedError,
  PatchOptions,
  Props,
  Stats,
  URLType,
} from "univ-fs";
import { S3Directory } from "./S3Directory";
import { S3File } from "./S3File";

export interface Command {
  Bucket: string;
  Key: string;
}

export class S3FileSystem extends AbstractFileSystem {
  public s3: S3Client;

  constructor(
    repository: string,
    public bucket: string,
    config: S3ClientConfig,
    options?: FileSystemOptions
  ) {
    super(repository, options);
    this.s3 = new S3Client({ ...config });
  }

  public _createCommand(path: string): Command {
    const key = this._getKey(path);
    return {
      Bucket: this.bucket,
      Key: key,
    };
  }

  public _error(path: string, e: any, read: boolean) {
    let name: string;
    if (e.name === "NotFound" || e.$metadata?.httpStatusCode === 404) {
      name = NotFoundError.name;
    } else if (read) {
      name = NotReadableError.name;
    } else {
      name = NoModificationAllowedError.name;
    }
    return createError({
      name,
      repository: this.repository,
      path,
      e,
    });
  }

  public _getKey(path: string) {
    const parts = getPathParts(this.repository + "/" + path);
    return parts.join("/");
  }

  public _getPrefix(path: string) {
    return this._getKey(path) + "/";
  }

  private handleHead(data: HeadObjectCommandOutput) {
    const metadata = { ...data.Metadata };
    const created = parseInt(metadata["created"]!);
    const deleted = parseInt(metadata["deleted"]!);
    return {
      created: created || undefined,
      modified: data.LastModified?.getTime(),
      deleted: deleted || undefined,
      size: data.ContentLength,
    } as Stats;
  }

  public async _head(path: string, _options: HeadOptions): Promise<Stats> {
    const fileHeadCmd = new HeadObjectCommand(this._createCommand(path));
    const dirHeadCmd = new HeadObjectCommand({
      Bucket: this.bucket,
      Key: this._getKey(path) + "/",
    });
    const dirListCmd = new ListObjectsV2Command({
      Bucket: this.bucket,
      Delimiter: "/",
      Prefix: this._getPrefix(path),
      MaxKeys: 1,
    });
    const fileHead = this.s3.send(fileHeadCmd);
    const dirHead = this.s3.send(dirHeadCmd);
    const dirList = this.s3.send(dirListCmd);
    const [fileHeadRes, dirHeadRes, dirListRes] = await Promise.allSettled([
      fileHead,
      dirHead,
      dirList,
    ]);
    if (fileHeadRes.status === "fulfilled") {
      return this.handleHead(fileHeadRes.value);
    } else if (dirHeadRes.status === "fulfilled") {
      const stats = this.handleHead(dirHeadRes.value);
      delete stats.size;
      return stats;
    } else if (dirListRes.status === "fulfilled") {
      return {};
    } else {
      throw this._error(path, fileHeadRes.reason, true);
    }
  }

  public async _patch(
    path: string,
    props: Props,
    _options: PatchOptions
  ): Promise<void> {
    const key = this._getKey(path);
    try {
      const cmd = new CopyObjectCommand({
        Bucket: this.bucket,
        CopySource: this.bucket + "/" + key,
        Key: key,
        Metadata: props,
      });
      await this.s3.send(cmd);
    } catch (e) {
      throw this._error(path, e, false);
    }
  }

  public async getDirectory(path: string): Promise<AbstractDirectory> {
    return new S3Directory(this, path);
  }

  public async getFile(path: string): Promise<AbstractFile> {
    return new S3File(this, path);
  }

  public toURL(path: string, urlType?: URLType): Promise<string> {
    if (urlType === "DELETE") {
      throw createError({
        name: NotSupportedError.name,
        repository: this.repository,
        path,
        e: '"DELETE" is not supported',
      });
    }
    if (urlType === "GET") {
      const cmd = new GetObjectCommand(this._createCommand(path));
      return getSignedUrl(this.s3, cmd);
    } else {
      const cmd = new PutObjectCommand(this._createCommand(path));
      return getSignedUrl(this.s3, cmd);
    }
  }
}
