import { S3FileSystem } from "../S3FileSystem";
import { getRootDir } from "./init";
import { testAll } from "univ-fs/lib/__tests__/list";

const fs = new S3FileSystem(getRootDir());
testAll(fs);
