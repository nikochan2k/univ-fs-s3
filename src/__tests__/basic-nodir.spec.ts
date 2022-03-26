import { testAll } from "univ-fs/lib/__tests__/basic";
import { fs, setup, teardown } from "./setup-nodir";

testAll(fs, { setup, teardown });
