import { testAll } from "univ-fs/lib/__tests__/basic";
import { fs, init } from "./init";

testAll(fs, () => init());
