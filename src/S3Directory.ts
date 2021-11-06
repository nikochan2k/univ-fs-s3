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
      const err = s3FS._error(path, e, true);
      if (err.name === NotFoundError.name) {
        return objects;
      }
      throw err;
    }
  }

  public async _mkcol(): Promise<void> {
    const s3fs = this.s3fs;
    const path = this.path;
    const cmd = new PutObjectCommand({
      Bucket: s3fs.bucket,
      Key: s3fs._getKey(path, true),
      Body: "",
    });
    try {
      const client = await s3fs._getClient();
      await client.send(cmd);
    } catch (e) {
      throw s3fs._error(path, e, false);
    }
  }

  public async _rmdir(): Promise<void> {
    const s3fs = this.s3fs;
    const path = this.path;
    const cmd = new DeleteObjectCommand({
      Bucket: s3fs.bucket,
      Key: s3fs._getKey(path, true),
    });
    try {
      const client = await s3fs._getClient();
      await client.send(cmd);
    } catch (e) {
      throw s3fs._error(path, e, false);
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
      if (content.Prefix) {
        const parts = content.Prefix.split("/");
        const name = parts[parts.length - 2] as string;
        const path = joinPaths(this.path, name);
        objects.push(path);
      }
    }
    // Files
    for (const content of data.Contents || []) {
      if (content.Key) {
        const parts = content.Key.split("/");
        const name = parts[parts.length - 1] as string;
        const path = joinPaths(this.path, name);
        objects.push(path);
      }
    }

    if (data.IsTruncated) {
      params.ContinuationToken = data.NextContinuationToken;
      await this._listObjects(params, objects);
    }
  }
}
