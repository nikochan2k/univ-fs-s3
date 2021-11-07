import {
  CopyObjectCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  HeadObjectCommandInput,
  HeadObjectCommandOutput,
  ListObjectsV2Command,
  ListObjectsV2CommandOutput,
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
  HeadOptions,
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

  public _createMetadata(props: Props) {
    const metadata: { [key: string]: string } = {};
    for (const [key, value] of Object.entries(props)) {
      if (0 <= ["size", "etag", "modified"].indexOf(key)) {
        continue;
      }
      metadata[key] = "" + value; // eslint-disable-line
    }
    return metadata;
  }

  public _dispose() {
    if (this.client) {
      this.client.destroy();
    }
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

  public async _getDirectory(path: string): Promise<AbstractDirectory> {
    return Promise.resolve(new S3Directory(this, path));
  }

  public async _getFile(path: string): Promise<AbstractFile> {
    return Promise.resolve(new S3File(this, path));
  }

  public _getKey(path: string, isDirectory: boolean) {
    let key: string;
    if (!path || path === "/") {
      key = this.repository;
    } else {
      key = joinPaths(this.repository, path, false);
    }
    if (isDirectory) {
      key += "/";
    }
    return key;
  }

  public async _head(path: string, options?: HeadOptions): Promise<Stats> {
    options = { ...options };
    const isFile = !options.type || options.type === "file";
    const isDirectory = !options.type || options.type === "directory";
    const client = await this._getClient();
    let fileHead: Promise<HeadObjectCommandOutput>;
    if (isFile) {
      const fileHeadCmd = new HeadObjectCommand(
        this._createCommand(path, false)
      );
      fileHead = client.send(fileHeadCmd);
    } else {
      fileHead = Promise.reject();
    }
    let dirHead: Promise<HeadObjectCommandOutput>;
    let dirList: Promise<ListObjectsV2CommandOutput>;
    if (isDirectory) {
      const dirHeadCmd = new HeadObjectCommand(this._createCommand(path, true));
      dirHead = client.send(dirHeadCmd);
      const dirListCmd = new ListObjectsV2Command({
        Bucket: this.bucket,
        Delimiter: "/",
        Prefix: this._getKey(path, true),
        MaxKeys: 1,
      });
      dirList = client.send(dirListCmd);
    } else {
      dirHead = Promise.reject();
      dirList = Promise.reject();
    }
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
    } else if (dirListRes.status === "fulfilled") {
      const res = dirListRes.value;
      if (
        (res.Contents && 0 < res.Contents.length) ||
        (res.CommonPrefixes && 0 < res.CommonPrefixes.length)
      ) {
        return {};
      }
    }
    let dirListReason: unknown | undefined;
    if (dirListRes.status === "rejected") {
      dirListReason = dirListRes.reason;
    }
    if (isFile) {
      throw this._error(path, fileHeadRes.reason, false);
    }
    if (isDirectory) {
      if (dirHeadRes.reason) {
        throw this._error(path, dirHeadRes.reason, false);
      }
    }
    throw this._error(path, dirListReason, false);
  }

  public async _patch(
    path: string,
    props: Props,
    _options: PatchOptions // eslint-disable-line
  ): Promise<void> {
    const key = this._getKey(path, props["size"] == null);
    try {
      const cmd = new CopyObjectCommand({
        Bucket: this.bucket,
        CopySource: this.bucket + "/" + key,
        Key: key,
        Metadata: this._createMetadata(props),
      });
      const client = await this._getClient();
      await client.send(cmd);
    } catch (e) {
      throw this._error(path, e, true);
    }
  }

  public async _toURL(path: string, options?: URLOptions): Promise<string> {
    try {
      options = { urlType: "GET", expires: 86400, ...options };
      const client = await this._getClient();
      let url: string;
      switch (options.urlType) {
        case "GET": {
          const cmd = new GetObjectCommand(this._createCommand(path, false));
          url = await getSignedUrl(client, cmd, { expiresIn: options.expires });
          break;
        }
        case "PUT":
        case "POST": {
          const cmd = new PutObjectCommand(this._createCommand(path, false));
          url = await getSignedUrl(client, cmd, { expiresIn: options.expires });
          break;
        }
        case "DELETE": {
          const cmd = new DeleteObjectCommand(this._createCommand(path, false));
          url = await getSignedUrl(client, cmd, { expiresIn: options.expires });
          break;
        }
        default:
          throw createError({
            name: NotSupportedError.name,
            repository: this.repository,
            path,
            e: { message: `"${options.urlType}" is not supported` }, // eslint-disable-line
          });
      }
      return url;
    } catch (e) {
      throw this._error(path, e, false);
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
