## Implementation

- [x] Extend router chat-template tokenization to accept optional tool schemas while preserving tool-free behavior.
- [x] Implement `/v1/messages` routing-token construction for supported Anthropic text and tool interactions without changing forwarded bodies.
- [x] Implement `/v1/responses` routing-token construction for supported stateless non-harmony inputs without changing forwarded bodies.
- [x] Ensure unsupported/stateful/token-counting requests do not feed route-history prefixes.
- [x] Add focused unit and proxy tests for messages, responses, tools, fallback cases, and passthrough body preservation.
- [x] Run strict OpenSpec validation and targeted router tests.
