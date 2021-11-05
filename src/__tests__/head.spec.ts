import { testAll } from "univ-fs/lib/__tests__/head";
import { fs, init } from "./init";

testAll(fs, () => init());
