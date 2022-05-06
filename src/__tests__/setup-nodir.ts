import { OnExists, OnNoParent, OnNotExist } from "univ-fs";
import { S3FileSystem } from "../S3FileSystem";
import config from "./secret.json";

export const fs = new S3FileSystem("univ-fs-test", "test-nodir", config, {
  canCreateDirectory: false,
});

export const setup = async () => {
  const root = await fs.getDirectory("/");
  await root.rm({
    onNotExist: OnNotExist.Ignore,
    recursive: true,
    ignoreHook: true,
  });
  await root.mkdir({
    onExists: OnExists.Ignore,
    onNoParent: OnNoParent.Error,
    ignoreHook: true,
  });
};

export const teardown = async () => {
  fs._dispose();
};
