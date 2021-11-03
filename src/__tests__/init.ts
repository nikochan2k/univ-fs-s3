import { NotFoundError } from "univ-fs";
import { S3FileSystem } from "../S3FileSystem";

export const fs = new S3FileSystem("univ-fs-test", "test", {
  region: "ap-northeast-1",
  endpoint: "http://127.0.0.1:9000",
  forcePathStyle: true,
  credentials: {
    accessKeyId: "minioadmin",
    secretAccessKey: "minioadmin",
  },
});

export const init = async (fs: S3FileSystem) => {
  try {
    await fs.delete("/", { force: true, recursive: true, ignoreHook: true });
  } catch (e) {
    if (e.name !== NotFoundError.name) {
      throw e;
    }
  }
};
