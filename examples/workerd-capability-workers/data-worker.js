export default {
  async fetch(request, env) {
    const input = await request.json();
    const { kind, ...operationFields } = input.operation;
    const response = await env[input.binding].fetch("https://logical.invalid/", {
      method: "POST",
      body: JSON.stringify({
        version: 2,
        request_id: input.request_id,
        binding: input.binding,
        operation: { kind, ...operationFields },
      }),
    });
    return new Response(await response.text(), {
      status: response.status,
      headers: { "content-type": "application/json" },
    });
  },
};
