import { testAll } from "univ-fs/lib/__tests__/list";
import { fs, setup, teardown } from "./setup";

testAll(fs, { setup, teardown });
