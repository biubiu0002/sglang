## Why

Low-priority internal traffic that uses `/v1/messages` or `/v1/responses` currently does not consistently benefit from cache-aware prefix routing. The router has a prefix-tree plus load policy, but generation route handlers must provide engine-equivalent or safely route-equivalent prompt tokens for that policy to work.

## What Changes

- Add precise routing-token derivation for `/v1/messages` using the same prompt construction as the SGLang Anthropic serving path.
- Add precise routing-token derivation for `/v1/responses` using the same prompt construction as the SGLang Responses serving path.
- Keep passthrough semantics: the router must not modify the worker-facing request body or inject `input_ids` for these routes.
- Exclude non-generation token-counting routes from route-history prefix-tree feeding.
- Preserve priority filtering, sticky routing, alias fallback, and active-load accounting behavior.

## Capabilities

### New Capabilities

- `generation-route-prefix-routing`: Cache-aware routing for generation endpoints whose HTTP schemas differ from OpenAI chat completions.

### Modified Capabilities

None.

## Impact

- Affected code: `experimental/sgl-router/src/server/routes/messages.rs`, `experimental/sgl-router/src/server/routes/responses.rs`, shared route-token helpers if introduced, and proxy tests.
- Affected APIs: router behavior for `/v1/messages` and `/v1/responses`; external request and response schemas remain unchanged.
- Dependencies: no new runtime service dependency; implementation may reuse existing tokenizer/chat-template support and mirror existing Python serving conversion logic.
