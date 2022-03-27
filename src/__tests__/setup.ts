import { NotFoundError } from "univ-fs";
import { S3FileSystem } from "../S3FileSystem";
import config from "./secret.json";

export const fs = new S3FileSystem("univ-fs-test", "test", config);

export const setup = async () => {
  try {
    const root = await fs._getDirectory("/");
    await root.rm({ force: true, recursive: true, ignoreHook: true });
    await root.mkdir({ force: true, recursive: false, ignoreHook: true });
  } catch (e) {
    if (e.name !== NotFoundError.name) {
      throw e;
    }
  }
};

export const teardown = async () => {
  fs._dispose();
};
