# Workforce AI MVC POC

A proof of concept where a manager describes a shift assignment in plain
English (e.g. "assign EMP99 to the Cashier shift on 2026-07-06") over an
API-key-gated SSE endpoint. A local LLM (Llama 3.1 via Ollama) extracts the
structured fields and calls a tool that writes the assignment to Postgres;
once that write commits, an audit event is published to Kafka via a
transactional outbox, with no risk of event loss from a crash or broker drop
at commit time.

## Quick start

Bring up Postgres and Kafka:

```bash
docker compose -f compose.yaml up -d
```

Make sure Ollama is running locally with the model pulled:

```bash
ollama pull llama3.1
```

Run the app (starts on port 8081):

```bash
./mvnw spring-boot:run
```

Send a test request against the SSE endpoint (`-N` disables buffering so you
see events as they stream):

```bash
curl -N -H "X-API-Key: sk_wfm_poc_2026_secure_keystring" \
  "http://localhost:8081/api/wfm/schedule-stream?conversationId=conv_test_1&prompt=Assign%20EMP99%20to%20the%20Cashier%20shift%20on%202026-07-06"
```

You should see interleaved `chat-progress` events (raw model tokens) and a
final `tool-result` event carrying the authoritative JSON result of the
database write.

## Ports / access

- App: `http://localhost:8081`
- Postgres: `localhost:54321` (db `wfm_db`, user `wfm_manager`) — non-default
  host port, mapped from the container's `5432`
- Kafka: `localhost:9094` (KRaft mode, no ZooKeeper) — non-default host port
- Ollama: `http://localhost:11434` (run natively on the host, not in compose)

## Docs

- [docs/SPEC.md](docs/SPEC.md) — full requirements, hard constraints, and stack rationale
- [docs/STATE.md](docs/STATE.md) — build/session history: what's been verified vs. assumed, known API gotchas
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — sequence diagram and deployment topology as actually implemented
- [docs/LEARNINGS.md](docs/LEARNINGS.md) — non-obvious Spring Boot / Spring AI lessons and gotchas discovered while building this

## Design decisions

- **SSE, not plain REST** — scheduling requires streaming both the model's live prose and a separate authoritative tool-result event, which a single request/response can't express. See [docs/SPEC.md](docs/SPEC.md).
- **Spring MVC + virtual threads, not WebFlux/R2DBC** — Spring AI's VectorStore is JDBC-based and blocking framework-wide, so a reactive stack would just smuggle blocking calls across the boundary for no benefit at this concurrency scale. See [docs/SPEC.md](docs/SPEC.md).
- **Transactional outbox, not fire-after-commit** — the audit event is written to an `outbox_events` row in the same DB transaction as the shift assignment, then published by a separate poller, closing the gap where a crash between commit and publish would lose the event. See [docs/SPEC.md](docs/SPEC.md).
- **Semaphore-throttled Ollama calls** — virtual threads remove the web-tier thread ceiling, so the LLM entry point is bounded explicitly to avoid flooding a local Ollama instance and causing latency spikes/503s. See [docs/SPEC.md](docs/SPEC.md).
- **Non-default host ports for Postgres/Kafka** — avoids silently colliding with an unrelated local project on the default ports, a failure mode that previously produced a misleading auth error. See [docs/SPEC.md](docs/SPEC.md).

## Status

Build-order steps 1–4 from SPEC.md are complete and E2E-verified directly
against Postgres and Kafka (not just app logs). The transactional-outbox
atomicity bugs (proxy-bypass and swallowed-exception cases) found during
verification have been fixed and re-confirmed via failure-injection testing.
Step 5 (chat-memory advisors, pgvector-backed RAG) has not been started.
