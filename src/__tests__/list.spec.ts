import { testAll } from "univ-fs/lib/__tests__/list";
import { fs, init } from "./init";

testAll(fs, () => init());
