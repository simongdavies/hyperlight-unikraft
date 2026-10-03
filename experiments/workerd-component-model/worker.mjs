import componentCore from "./generated/component.core.wasm";
import { instantiate } from "./generated/component.js";

const { add } = instantiate(() => componentCore, {});

const MAX_REQUEST_BYTES = 1024;
const MIN_I32 = -2147483648;
const MAX_I32 = 2147483647;

function readI32(params, name) {
  const raw = params.get(name);
  if (raw === null || !/^-?\d+$/.test(raw)) {
    throw new TypeError(`${name} must be a base-10 integer`);
  }

  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < MIN_I32 || value > MAX_I32) {
    throw new RangeError(`${name} must fit in a signed 32-bit integer`);
  }
  return value;
}

function json(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" }
  });
}

export default {
  async fetch(request) {
    if (request.url.length > MAX_REQUEST_BYTES) {
      return json({ error: "request exceeds policy limit" }, 413);
    }

    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/health") {
      return json({ ok: true, path: "jco-transpiled-component" });
    }
    if (request.method === "GET" && url.pathname === "/network-probe") {
      try {
        await fetch("http://example.com/");
        return json({ blocked: false }, 500);
      } catch (error) {
        return json({ blocked: true, error: error.message });
      }
    }
    if (request.method !== "GET" || url.pathname !== "/add") {
      return json({ error: "not found" }, 404);
    }

    try {
      const left = readI32(url.searchParams, "left");
      const right = readI32(url.searchParams, "right");
      return json({ result: add(left, right) });
    } catch (error) {
      return json({ error: error.message }, 400);
    }
  }
};
