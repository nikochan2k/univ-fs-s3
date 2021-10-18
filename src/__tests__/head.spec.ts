import { S3FileSystem } from "../S3FileSystem";
import { testAll } from "univ-fs/lib/__tests__/head";
import { getRootDir } from "./init";

const fs = new S3FileSystem(getRootDir());
testAll(fs);
