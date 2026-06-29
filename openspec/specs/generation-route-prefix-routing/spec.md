# generation-route-prefix-routing Specification

## Purpose
TBD - created by archiving change precise-generation-route-prefix-routing. Update Purpose after archive.
## Requirements
### Requirement: Messages generation route supplies route-equivalent prefix tokens
The router SHALL derive cache-aware routing tokens for `/v1/messages` generation requests from an internal OpenAI-chat-shaped view that matches the SGLang Anthropic request conversion for supported text and tool interactions.

#### Scenario: Text messages with system content
- **WHEN** a `/v1/messages` request contains top-level system content, conversation messages, metadata, priority, and sampling fields
- **THEN** the router SHALL route using tokens derived from the system and conversation prompt content and SHALL ignore non-prompt fields such as metadata and priority for token derivation

#### Scenario: Anthropic tool interactions
- **WHEN** a `/v1/messages` request contains supported Anthropic tool definitions, `tool_use`, and `tool_result` content blocks
- **THEN** the router SHALL derive route tokens from the corresponding OpenAI chat messages and tool schemas used by the worker prompt construction

#### Scenario: Messages passthrough body
- **WHEN** the router forwards a `/v1/messages` generation request after deriving route tokens
- **THEN** the forwarded worker request body SHALL remain byte-for-byte schema-equivalent to the original native Messages request and SHALL NOT include router-generated `input_ids`

### Requirement: Responses generation route supplies route-equivalent prefix tokens
The router SHALL derive cache-aware routing tokens for `/v1/responses` generation requests from an internal OpenAI-chat-shaped view that matches the SGLang non-harmony Responses request conversion for supported stateless inputs.

#### Scenario: Instructions and stateless input
- **WHEN** a `/v1/responses` request contains `instructions` and stateless string or text-message input
- **THEN** the router SHALL derive route tokens from the leading system message and normalized conversation messages that the worker would pass to chat prompt processing

#### Scenario: Response function tools and tool calls
- **WHEN** a `/v1/responses` request contains supported function tools, function call items, and function call output items
- **THEN** the router SHALL derive route tokens from the normalized chat tool schemas, assistant tool calls, and tool result messages used by the worker prompt construction

#### Scenario: Responses passthrough body
- **WHEN** the router forwards a `/v1/responses` generation request after deriving route tokens
- **THEN** the forwarded worker request body SHALL remain byte-for-byte schema-equivalent to the original native Responses request and SHALL NOT include router-generated `input_ids`

### Requirement: Unsupported generation prompt forms do not pollute route history
The router SHALL avoid feeding route-history prefix data when it cannot deterministically construct route-equivalent prompt tokens for a generation request.

#### Scenario: Stateful Responses request
- **WHEN** a `/v1/responses` request depends on `previous_response_id`
- **THEN** the router SHALL select a worker without request-token prefix data and SHALL NOT add a synthetic prefix for that request to route history

#### Scenario: Unsupported prompt renderer
- **WHEN** a request requires a prompt renderer that the router cannot reproduce, such as GPT-OSS Harmony rendering or unsupported multimodal prompt content
- **THEN** the router SHALL select a worker without request-token prefix data and SHALL NOT add an approximate prefix for that request to route history

#### Scenario: Token-counting route
- **WHEN** a `/v1/messages/count_tokens` request or another non-generation token-counting request is routed
- **THEN** the router SHALL NOT feed route-history prefix data because the worker does not create reusable generation KV cache

### Requirement: Tool schemas participate in chat-template route tokenization
The router SHALL include supported chat tool schemas in chat-template rendering when deriving route tokens for chat-shaped generation requests.

#### Scenario: Tool-aware prompt template
- **WHEN** a supported chat-shaped generation request contains function tool schemas and the selected model chat template renders tools
- **THEN** the derived route tokens SHALL reflect the rendered tool schema prompt prefix

#### Scenario: Tool-free prompt template compatibility
- **WHEN** a supported chat-shaped generation request has no tools
- **THEN** the derived route tokens SHALL remain compatible with the existing tool-free chat-template path

