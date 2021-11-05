import {
  DeleteObjectCommand,
  ListObjectsV2Command,
  S3Client,
  S3ClientConfig,
} from "@aws-sdk/client-s3";
import { S3FileSystem } from "../S3FileSystem";

const config: S3ClientConfig = {
  region: "ap-northeast-1",
  endpoint: "http://127.0.0.1:9000",
  forcePathStyle: true,
  credentials: {
    accessKeyId: "minioadmin",
    secretAccessKey: "minioadmin",
  },
};
export const fs = new S3FileSystem("univ-fs-test", "test", config);

export const init = async () => {
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
