import {
  DeleteObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandInput,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { AbstractDirectory, joinPaths, NotFoundError } from "univ-fs";
import { S3FileSystem } from "./S3FileSystem";

export class S3Directory extends AbstractDirectory {
  constructor(private s3fs: S3FileSystem, path: string) {
    super(s3fs, path);
  }

  public async _list(): Promise<string[]> {
    const s3FS = this.s3fs;
    const path = this.path;
    const objects: string[] = [];
    try {
      await this._listObjects(
        {
          Bucket: s3FS.bucket,
          Delimiter: "/",
          Prefix: s3FS._getKey(path, true),
        },
        objects
      );
      return objects;
    } catch (e) {
      const err = s3FS._error(path, e, false);
      if (err.name === NotFoundError.name) {
        return objects;
      }
      throw err;
    }
  }

  public async _mkcol(): Promise<void> {
    const s3fs = this.s3fs;
    const path = this.path;

    try {
      const client = await s3fs._getClient();
      const cmd = new PutObjectCommand({
        ...s3fs._createCommand(path, true),
        Body: "",
        ContentLength: 0,
      });
      await client.send(cmd);
    } catch (e) {
      throw s3fs._error(path, e, true);
    }
  }

  public async _rmdir(): Promise<void> {
    const s3fs = this.s3fs;
    const path = this.path;

    try {
      const cmd = new DeleteObjectCommand(s3fs._createCommand(path, true));
      const client = await s3fs._getClient();
      await client.send(cmd);
    } catch (e) {
      throw s3fs._error(path, e, true);
    }
  }

  private async _listObjects(
    params: ListObjectsV2CommandInput,
    objects: string[]
  ) {
    const cmd = new ListObjectsV2Command(params);
    const client = await this.s3fs._getClient();
    const data = await client.send(cmd);
    // Directories
    for (const content of data.CommonPrefixes || []) {
      const prefix = content.Prefix;
      if (!prefix) {
        continue;
      }
      if (prefix === params.Prefix) {
        continue;
      }
      const parts = prefix.split("/");
      const name = parts[parts.length - 2] as string;
      const path = joinPaths(this.path, name) + "/";
      objects.push(path);
    }
    // Files
    for (const content of data.Contents || []) {
      const key = content.Key;
      if (!key) {
        continue;
      }
      if (key === params.Prefix) {
        continue;
      }
      const parts = key.split("/");
      const name = parts[parts.length - 1] as string;
      const path = joinPaths(this.path, name);
      objects.push(path);
    }

    if (data.IsTruncated) {
      params.ContinuationToken = data.NextContinuationToken;
      await this._listObjects(params, objects);
    }
  }
}
