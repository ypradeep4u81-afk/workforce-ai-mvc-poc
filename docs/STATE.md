# Project State — Session Handoff Notes

Last updated: 2026-07-04 (manual build, pre-Claude Code)

## What's been done (manually, not via Claude Code)

Build-order steps 1–4 from SPEC.md are complete and were E2E tested successfully.

## Stack (as actually implemented)
- Java 21, Spring Boot 4.1.0, Spring MVC, plain JDBC (JdbcTemplate)
- Spring Kafka, Spring AI 2.0.0 GA
- spring.threads.virtual.enabled=true
- Base package: com.wfm.poc (consolidated from an earlier underscored structure)

## Infrastructure (compose.yaml)
- Postgres: ankane/pgvector:latest, host port 54321 (non-default, intentional)
- Kafka: KRaft mode, host port 9094 (non-default, intentional)
- App server: port 8081 (moved off 8080 to avoid OrbStack dashboard proxy collision)
- Kafka health check: removed shell-based health check — apache/kafka:latest 
  slim image lacks the shell interpreter needed for it

## Spring AI 2.0.0 GA — breaking changes hit and fixed
These are real API changes in 2.0.0 GA, not bugs in our code. If Claude Code 
suggests reverting any of these to match older Spring AI patterns from 
training data, that would be wrong — do not revert.

1. **Tool registration**: `ToolCallingAdvisor` constructor pattern is retired 
   in 2.0.0. Tools are now registered via `.defaultTools(schedulingTools)` 
   on `ChatClient.Builder`.
2. **Jackson**: `jackson-databind` is not pulled in transitively as expected — 
   added explicitly to pom.xml to restore `ObjectMapper` scope.
3. **Heartbeat lambda signature**: SSE heartbeat cancellation required explicit 
   boolean parameter declaration (`() -> heartbeatFuture.cancel(true)`) — 
   primitive signature mismatch otherwise.
4. **Embedding model**: overrode 
   `spring.ai.ollama.embedding.options.model=llama3.1` in application.properties 
   to stop vector store startup checks from crashing on a missing default 
   embedding model.
   ⚠️ Open question: llama3.1 is a chat/completion model, not a dedicated 
   embedding model. This works for now (no RAG built yet) but should be 
   reconsidered before Step 5 (pgvector RAG) — likely swap to a proper 
   embedding model like nomic-embed-text.

## Verified end-to-end flow
- Endpoint: POST /api/wfm/schedule-stream
- Validates X-API-Key header (static, placeholder value — see Security Posture 
  in SPEC.md addendum)
- Throttles Ollama calls via Semaphore
- Streams two SSE event types: chat-progress (raw tokens) and tool-result 
  (authoritative JSON)
- Tool writes shift_assignments + outbox_events in one transaction; logs 
  ground-truth return value server-side unconditionally
- Outbox poller: virtual-thread scheduled task, runs every 500ms, publishes to 
  Kafka, marks PROCESSED only on broker ack
- cURL test: assigned EMP99, described as successful by app logs / response

## ⚠️ NOT independently verified yet
The above E2E result is based on app-level logs/response, not direct inspection. 
Per SPEC.md's "verify state directly" requirement, these still need to be 
checked directly before being trusted as a baseline:
- [ ] psql: confirm actual shift_assignments row for EMP99 (values, timestamp)
- [ ] psql: confirm outbox_events row is marked PROCESSED, not just inserted
- [ ] kafka-console-consumer: confirm the actual audit payload on the topic 
      matches the DB row
- [ ] mvn clean install: confirm a full clean build with no errors/warnings 
      of concern

## Explicitly NOT done yet
- Spring AI chat-memory advisors (Step 5, part 1)
- pgvector-backed RAG (Step 5, part 2)

## Security posture
See SPEC.md addendum — placeholder credentials, intentional POC scope, 
documented tradeoff.

## Git
- Baseline committed and tagged: `baseline-e2e-working`
- Repo: https://github.com/ypradeep4u81-afk/workforce-ai-mvc-poc