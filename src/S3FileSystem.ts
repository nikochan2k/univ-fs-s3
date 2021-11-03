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
    public bucket: string,
    repository: string,
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

  public _error(path: string, e: unknown, read: boolean) {
    let name: string;
    if (
      (e as any).name === "NotFound" || // eslint-disable-line
      (e as any).$metadata?.httpStatusCode === 404 // eslint-disable-line
    ) {
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
      e: e as any, // eslint-disable-line
    });
  }

  public _getKey(path: string) {
    const parts = getPathParts(this.repository + "/" + path);
    return parts.join("/");
  }

  public _getPrefix(path: string) {
    return this._getKey(path) + "/";
  }

  public async _head(path: string): Promise<Stats> {
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
      return this._handleHead(fileHeadRes.value, true);
    } else if (dirHeadRes.status === "fulfilled") {
      const stats = this._handleHead(dirHeadRes.value, false);
      delete stats.size;
      return stats;
    }
    if (dirListRes.status === "fulfilled") {
      const res = dirListRes.value;
      if (res.Contents || res.CommonPrefixes) {
        return {};
      }
    }
    throw this._error(path, fileHeadRes.reason, true);
  }

  public async _patch(
    path: string,
    props: Props,
    _options: PatchOptions // eslint-disable-line
  ): Promise<void> {
    const metadata: { [key: string]: string } = {};
    for (const [key, value] of Object.entries(props)) {
      metadata[key] = "" + value; // eslint-disable-line
    }
    const key = this._getKey(path);
    try {
      const cmd = new CopyObjectCommand({
        Bucket: this.bucket,
        CopySource: this.bucket + "/" + key,
        Key: key,
        Metadata: metadata,
      });
      await this.s3.send(cmd);
    } catch (e) {
      throw this._error(path, e, false);
    }
  }

  public async getDirectory(path: string): Promise<AbstractDirectory> {
    return Promise.resolve(new S3Directory(this, path));
  }

  public async getFile(path: string): Promise<AbstractFile> {
    return Promise.resolve(new S3File(this, path));
  }

  public toURL(path: string, urlType?: URLType): Promise<string> {
    if (urlType === "DELETE") {
      throw createError({
        name: NotSupportedError.name,
        repository: this.repository,
        path,
        e: { message: '"DELETE" is not supported' },
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

  private _handleHead(data: HeadObjectCommandOutput, isFile: boolean) {
    const metadata = { ...data.Metadata };
    let created: number | undefined;
    if (metadata["created"]) {
      created = parseInt(metadata["created"]);
    }
    let deleted: number | undefined;
    if (metadata["deleted"]) {
      deleted = parseInt(metadata["deleted"]);
    }
    let size: number | undefined;
    if (isFile) {
      size = data.ContentLength;
    }
    return {
      created: created || undefined,
      modified: data.LastModified?.getTime(),
      deleted: deleted || undefined,
      size,
    } as Stats;
  }
}
