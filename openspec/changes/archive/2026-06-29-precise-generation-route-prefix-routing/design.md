## Context

The new B200 gateway uses `experimental/sgl-router` from `san-tian/sglang@deploy-b200`. Its cache-aware policy can combine prefix locality with worker load, but only routes that provide prompt tokens can participate in prefix-tree routing. `/v1/chat/completions` already derives chat-template tokens; `/v1/messages` and `/v1/responses` are passthrough routes and currently either omit tokens or use only partial approximations.

The worker can serve native `/v1/messages` and `/v1/responses`. The gateway must therefore keep request bodies unchanged while constructing an internal routing-only view that matches the worker prompt construction closely enough for route-history prefix matching.

## Goals / Non-Goals

**Goals:**

- Derive route tokens for `/v1/messages` from an OpenAI-chat-shaped view equivalent to the SGLang Anthropic conversion path.
- Derive route tokens for `/v1/responses` from an OpenAI-chat-shaped view equivalent to the non-harmony SGLang Responses conversion path.
- Include tool definitions in chat-template rendering when they affect prompt bytes.
- Never inject `input_ids` or otherwise mutate worker-facing bodies for passthrough routes.
- Avoid route-history pollution by falling back to load-only routing when the router cannot deterministically mirror worker prompt construction.

**Non-Goals:**

- Reimplement worker-side response-store lookup for `previous_response_id`.
- Reimplement GPT-OSS Harmony rendering in the router.
- Add a new runtime dependency or call workers to pre-tokenize before routing.
- Change priority filtering, alias fallback, sticky routing, worker health, or active-load accounting.

## Decisions

1. Build routing-only chat views and keep passthrough bodies unchanged.

   The gateway will parse the incoming body, build a normalized `{ messages, tools }` value for routing, and pass the original body bytes to the selected worker. This preserves native endpoint semantics while giving the cache-aware policy the same prompt prefix shape used by the worker.

2. Treat unsupported or stateful cases as ineligible for prefix-tree feeding.

   `/v1/responses` requests with `previous_response_id` require worker-local response store state. GPT-OSS Harmony requests use a separate renderer that the Rust router does not have. Multimodal or unknown content forms can affect prompt bytes in model-specific ways. For these cases the route handler will pass `None` request tokens so selection uses load and health only, and route-history will not learn a false prefix.

3. Extend tokenizer rendering with optional tools.

   Some chat templates include tool schemas near the start of the prompt. The tokenizer registry will expose a tool-aware chat rendering path. Routes that produce chat views with tools will use that path; routes without tools keep current behavior.

4. Keep `/v1/messages/count_tokens` and other non-generation token-counting routes out of route history.

   Count-only routes do not create worker KV cache. Feeding their prefixes into route history would make the router believe a worker has reusable KV that does not exist.

## Risks / Trade-offs

- Exactness depends on prompt renderer parity -> mitigate by only marking route tokens engine-equivalent when chat-template rendering succeeds with the same messages/tools view the worker uses.
- Some valid API requests will not get prefix-aware routing -> this is preferable to false cache locality because it preserves correctness and avoids hurting later routing decisions.
- Tool rendering changes affect shared chat-token derivation -> mitigate with unit tests for both tool-free and tool-bearing chat templates and passthrough proxy tests that assert bodies remain unchanged.

## Migration Plan

1. Add spec and task artifacts for the route-prefix capability.
2. Update tokenizer/chat-template helpers to accept optional tools.
3. Implement `/v1/messages` and `/v1/responses` routing-token builders.
4. Add unit and proxy tests for exact routing views, fallback cases, and unchanged passthrough bodies.
5. Validate locally with targeted Rust tests. Production rollout remains a normal router image build/deploy and is outside this local code change.
