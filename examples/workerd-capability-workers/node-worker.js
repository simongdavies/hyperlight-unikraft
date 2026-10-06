import legacy from "legacy.cjs";
import { Buffer } from "node:buffer";

export default {
  fetch() {
    return Response.json({
      commonjs: legacy.answer,
      bufferHex: Buffer.from("hyperlight").toString("hex"),
    });
  },
};
