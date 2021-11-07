import {
  DeleteObjectCommand,
  ListObjectsV2Command,
  S3Client,
} from "@aws-sdk/client-s3";
import { S3FileSystem } from "../S3FileSystem";
import config from "./secret.json";

export const fs = new S3FileSystem("univ-fs-test", "test", config);

export const setup = async () => {
  const client = new S3Client(config);
  const listCmd = new ListObjectsV2Command({
    Bucket: "univ-fs-test",
  });
  const data = await client.send(listCmd);
  for (const content of data.Contents || []) {
    const deleteCmd = new DeleteObjectCommand({
      Bucket: "univ-fs-test",
      Key: content.Key,
    });
    await client.send(deleteCmd);
  }
};

export const teardown = async () => {
  fs._dispose();
};
