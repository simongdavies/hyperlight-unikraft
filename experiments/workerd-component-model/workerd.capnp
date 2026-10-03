using Workerd = import "/workerd/workerd.capnp";

const componentEvaluation :Workerd.Config = (
  services = [
    (name = "main", worker = .componentWorker),
    # Defining the reserved global-outbound service with an empty allow list
    # prevents workerd from synthesizing its default public internet service.
    (name = "internet", network = (allow = []))
  ],
  sockets = [
    (name = "http", address = "127.0.0.1:8787", http = (), service = "main")
  ]
);

const componentWorker :Workerd.Worker = (
  modules = [
    (name = "worker.mjs", esModule = embed "worker.mjs"),
    (name = "generated/component.js", esModule = embed "generated/component.js"),
    (name = "generated/component.core.wasm", wasm = embed "generated/component.core.wasm")
  ],
  compatibilityDate = "2026-09-25",
  globalOutbound = "internet"
);
