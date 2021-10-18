import {
  DeleteObjectCommand,
  ListObjectsV2Command,
  ListObjectsV2CommandInput,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { AbstractDirectory, joinPaths, NotFoundError } from "univ-fs";
import { S3FileSystem } from ".";

export class S3Directory extends AbstractDirectory {
  constructor(private s3FS: S3FileSystem, path: string) {
    super(s3FS, path);
  }

  public async _list(): Promise<string[]> {
    const s3FS = this.s3FS;
    const path = this.path;
    const objects: string[] = [];
    try {
      await this._listObjects(
        {
          Bucket: s3FS.bucket,
          Delimiter: "/",
          Prefix: s3FS._getPrefix(path),
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

  private async _listObjects(
    params: ListObjectsV2CommandInput,
    objects: string[]
  ) {
    const cmd = new ListObjectsV2Command(params);
    var data = await this.s3FS.s3.send(cmd);
    // Directories
    for (const content of data.CommonPrefixes!) {
      const parts = content.Prefix!.split("/");
      const name = parts[parts.length - 2];
      const path = joinPaths(this.path, name!);
      objects.push(path);
    }
    // Files
    for (const content of data.Contents!) {
      const parts = content.Key!.split("/");
      const name = parts[parts.length - 1];
      const path = joinPaths(this.path, name!);
      objects.push(path);
    }

    if (data.IsTruncated) {
      params.ContinuationToken = data.NextContinuationToken;
      await this._listObjects(params, objects);
    }
  }

  public async _mkcol(): Promise<void> {
    const s3FS = this.s3FS;
    const path = this.path;
    const cmd = new PutObjectCommand({
      Bucket: s3FS.bucket,
      Key: s3FS._getKey(path) + "/",
      Body: "",
    });
    try {
      await s3FS.s3.send(cmd);
    } catch (e) {
      throw s3FS._error(path, e, false);
    }
  }

  public async _rmdir(): Promise<void> {
    const s3FS = this.s3FS;
    const path = this.path;
    const cmd = new DeleteObjectCommand({
      Bucket: s3FS.bucket,
      Key: s3FS._getKey(path) + "/",
    });
    try {
      await s3FS.s3.send(cmd);
    } catch (e) {
      throw s3FS._error(path, e, false);
    }
  }
}
