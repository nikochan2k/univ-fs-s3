import {
  CopyObjectCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  HeadObjectCommandInput,
  HeadObjectCommandOutput,
  ListObjectsV2Command,
  PutObjectCommand,
  PutObjectCommandInput,
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
  joinPaths,
  NoModificationAllowedError,
  NotFoundError,
  NotReadableError,
  NotSupportedError,
  PatchOptions,
  Props,
  Stats,
  URLOptions,
} from "univ-fs";
import { S3Directory } from "./S3Directory";
import { S3File } from "./S3File";

export interface Command {
  Bucket: string;
  Key: string;
}

export class S3FileSystem extends AbstractFileSystem {
  private client?: S3Client;

  constructor(
    public bucket: string,
    repository: string,
    private config: S3ClientConfig,
    options?: FileSystemOptions
  ) {
    super(repository, options);
  }

  public _createCommand(path: string, isDirectory: boolean): Command {
    const key = this._getKey(path, isDirectory);
    return {
      Bucket: this.bucket,
      Key: key,
    };
  }

  public _error(path: string, e: unknown, write: boolean) {
    let name: string;
    if (
      (e as any).name === "NotFound" || // eslint-disable-line
      (e as any).$metadata?.httpStatusCode === 404 // eslint-disable-line
    ) {
      name = NotFoundError.name;
    } else if (write) {
      name = NoModificationAllowedError.name;
    } else {
      name = NotReadableError.name;
    }
    return createError({
      name,
      repository: this.repository,
      path,
      e: e as any, // eslint-disable-line
    });
  }

  public async _getClient() {
    if (this.client) {
      return this.client;
    }

    this.client = new S3Client({ ...this.config });
    const input: HeadObjectCommandInput | PutObjectCommandInput = {
      Bucket: this.bucket,
      Key: this._getKey("/", true),
      Body: "",
    };
    try {
      const headCmd = new HeadObjectCommand(input);
      await this.client.send(headCmd);
      return this.client;
    } catch (e: unknown) {
      const err = this._error("/", e, false);
      if (err.name !== NotFoundError.name) {
        throw err;
      }
    }
    const putCmd = new PutObjectCommand(input);
    try {
      await this.client.send(putCmd);
      return this.client;
    } catch (e) {
      throw this._error("/", e, true);
    }
  }

  public _getKey(path: string, isDirectory: boolean) {
    let key: string;
    if (!path || path === "/") {
      key = this.repository;
    } else {
      key = joinPaths(this.repository, path);
    }
    if (isDirectory) {
      key += "/";
    }
    return key;
  }

  public async _head(path: string): Promise<Stats> {
    const fileHeadCmd = new HeadObjectCommand(this._createCommand(path, false));
    const dirHeadCmd = new HeadObjectCommand({
      Bucket: this.bucket,
      Key: this._getKey(path, true),
    });
    const dirListCmd = new ListObjectsV2Command({
      Bucket: this.bucket,
      Delimiter: "/",
      Prefix: this._getKey(path, true),
      MaxKeys: 1,
    });
    const client = await this._getClient();
    const fileHead = client.send(fileHeadCmd);
    const dirHead = client.send(dirHeadCmd);
    const dirList = client.send(dirListCmd);
    const [fileHeadRes, dirHeadRes, dirListRes] = await Promise.allSettled([
      fileHead,
      dirHead,
      dirList,
    ]);
    if (fileHeadRes.status === "fulfilled") {
      return this._handleHead(fileHeadRes.value, false);
    } else if (dirHeadRes.status === "fulfilled") {
      const stats = this._handleHead(dirHeadRes.value, true);
      delete stats.size;
      return stats;
    }
    if (dirListRes.status === "fulfilled") {
      const res = dirListRes.value;
      if (
        (res.Contents && 0 < res.Contents.length) ||
        (res.CommonPrefixes && 0 < res.CommonPrefixes.length)
      ) {
        return {};
      }
    }
    throw this._error(path, fileHeadRes.reason, false);
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
    const key = this._getKey(path, props["size"] == null);
    try {
      const cmd = new CopyObjectCommand({
        Bucket: this.bucket,
        CopySource: this.bucket + "/" + key,
        Key: key,
        Metadata: metadata,
      });
      const client = await this._getClient();
      await client.send(cmd);
    } catch (e) {
      throw this._error(path, e, true);
    }
  }

  public async getDirectory(path: string): Promise<AbstractDirectory> {
    return Promise.resolve(new S3Directory(this, path));
  }

  public async getFile(path: string): Promise<AbstractFile> {
    return Promise.resolve(new S3File(this, path));
  }

  public async toURL(path: string, options?: URLOptions): Promise<string> {
    options = { urlType: "GET", expires: 86400, ...options };
    const client = await this._getClient();
    switch (options.urlType) {
      case "GET": {
        const cmd = new GetObjectCommand(this._createCommand(path, false));
        return getSignedUrl(client, cmd, { expiresIn: options.expires });
      }
      case "PUT":
      case "POST": {
        const cmd = new PutObjectCommand(this._createCommand(path, false));
        return getSignedUrl(client, cmd, { expiresIn: options.expires });
      }
      case "DELETE": {
        const cmd = new DeleteObjectCommand(this._createCommand(path, false));
        return getSignedUrl(client, cmd, { expiresIn: options.expires });
      }
      default:
        throw createError({
          name: NotSupportedError.name,
          repository: this.repository,
          path,
          e: { message: `"${options.urlType}" is not supported` }, // eslint-disable-line
        });
    }
  }

  private _handleHead(data: HeadObjectCommandOutput, isDirectory: boolean) {
    const stats: Stats = {};
    if (!isDirectory) {
      stats.size = data.ContentLength;
    }
    if (data.LastModified) {
      stats.modified = data.LastModified.getTime();
    }
    if (data.ETag) {
      stats.etag = data.ETag;
    }
    for (const [key, value] of Object.entries(data.Metadata ?? {})) {
      if (key === "size" || key === "etag") {
        continue;
      }
      stats[key] = value;
    }

    return stats;
  }
}
