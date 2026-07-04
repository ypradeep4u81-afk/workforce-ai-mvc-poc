# Architecture — as implemented

This document reflects the code actually present in this repo as of 2026-07-04
(post transactional-proxy fix and exception-propagation fix, `mvn clean
install` green — see [STATE.md](STATE.md)). It is not a description of SPEC.md's aspirational
design; every element below was confirmed by reading
[WfmSchedulingController.java](../src/main/java/com/wfm/poc/controller/WfmSchedulingController.java),
[WfmSchedulingTools.java](../src/main/java/com/wfm/poc/tool/WfmSchedulingTools.java),
[WfmRepository.java](../src/main/java/com/wfm/poc/repository/WfmRepository.java),
[TransactionalOutboxPoller.java](../src/main/java/com/wfm/poc/outbox/TransactionalOutboxPoller.java),
[application.properties](../src/main/resources/application.properties), and
[compose.yaml](../compose.yaml).

## Proxy-bypass bugs — found and fixed

A prior version of this document flagged two bugs where the transactional
boundary wasn't real:
- `WfmSchedulingTools` was constructed with plain `new`, bypassing the Spring
  AOP proxy that `@Transactional` depends on.
- `TransactionalOutboxPoller.processOutboxQueue()` had no `@Transactional` at
  all, so its row lock didn't span the full poll cycle.

Both are fixed in the current code: `WfmSchedulingTools` is now `@Service`
and constructor-injected into the controller as a real Spring bean, and
`processOutboxQueue()` now carries `@Transactional`. A follow-up audit
confirmed no other manual-instantiation-bypasses-proxy instances exist in
the codebase. See STATE.md's "Transactional bug fix" and "AOP proxy audit"
sections for the full detail, including the grep methodology used to rule
out further instances.

## Exception propagation gap — found and fixed

A prior version of this document flagged an open gap: `assignShift`'s
`try { ... } catch (Exception e) { ...; return errorJson; }` swallowed
exceptions instead of rethrowing them, so Spring's `@Transactional` proxy
always saw a "successful" invocation and committed — including any partial
write made before the failure. A live failure-injection test at the time
confirmed this: a thrown exception between the two inserts left a committed
`shift_assignments` row with no matching `outbox_events` row.

This is now fixed, via two changes that were both necessary:
- `WfmSchedulingTools.assignShift`'s catch block no longer builds an
  `errorJson` string and returns it. It logs the failure (unconditionally,
  per SPEC.md's ground-truth-logging requirement) and rethrows — the
  original exception if it's already a `RuntimeException`, otherwise
  wrapped in an `IllegalStateException` — so the exception propagates out
  of the proxied method and the `@Transactional` proxy actually observes
  the failure and rolls back.
- `WfmSchedulingController` explicitly builds a `ToolCallingManager` with
  `DefaultToolExecutionExceptionProcessor.builder().alwaysThrow(true).build()`
  and wires it into `ChatClient.builder(...)` via a `ToolCallingAdvisor`.
  This was required because the controller's `ChatClient` isn't built from
  an autoconfigured `ChatClient.Builder` bean — the 5-arg static factory it
  uses falls back to a fresh `ToolCallingManager` with the library default
  `alwaysThrow=false`, which would otherwise catch the rethrown exception
  and convert it back into a plain string tool-response, silently
  defeating the rethrow fix above.

A failure-injection re-test (documented in STATE.md's "Exception
propagation fix" section) confirmed **zero rows** in both
`shift_assignments` and `outbox_events` after an injected failure — full
atomicity, not partial. See STATE.md for the full detail, including the
`ToolExecutionException`/`ChatClient` API investigation behind the second
change.

## 1. Functional flow (request lifecycle)

A manager opens the SSE endpoint with a static API-key header, a caller-supplied
`conversationId`, and a natural-language `prompt`. The controller streams model
tokens as they arrive, then — once the model's tool call has actually returned —
streams the tool's authoritative JSON as a second, distinct event type, per
`WfmSchedulingController.streamScheduling` and `WfmSchedulingTools.assignShift`.
`WfmSchedulingTools` is a constructor-injected `@Service` bean, so
`@Transactional` on `assignShift` runs through a real AOP proxy. Outbox
publication to Kafka happens on a separate 500ms poll loop
(`TransactionalOutboxPoller.processOutboxQueue`, itself now `@Transactional`),
fully decoupled from the request/response cycle.

```mermaid
sequenceDiagram
    actor Manager
    participant Controller as WfmSchedulingController
    participant Ollama as ChatClient / Ollama (llama3.1)
    participant Tools as WfmSchedulingTools (@Service, proxied)
    participant DB as Postgres (shift_assignments + outbox_events)
    participant Poller as TransactionalOutboxPoller (@Transactional)
    participant Kafka as Kafka (wfm.audit.shifts)

    Manager->>Controller: GET /api/wfm/schedule-stream?conversationId&prompt (X-API-Key header)
    Controller->>Controller: compare header to hardcoded configuredApiKey
    alt key mismatch
        Controller-->>Manager: 403 Forbidden
    else key OK
        Controller->>Controller: open SseEmitter(300s), registerSession(conversationId) -> "NO_CALL"
        Controller-->>Manager: SSE stream open, comment "ping-heartbeat" every 15s
        Controller->>Ollama: chatClient.prompt(prompt).stream() [virtual thread, ollamaLimiter permit 1 of 4]
        loop token stream
            Ollama-->>Controller: next token
            Controller-->>Manager: SSE event "chat-progress" (raw token)
        end
        Ollama->>Tools: invoke assignShift(employeeId, shiftDate, role, conversationId) [via AOP proxy, @Transactional starts]
        Tools->>Tools: acquire dbThrottler permit (1 of 25)
        Tools->>DB: INSERT INTO shift_assignments
        alt saveShiftAndOutbox succeeds
            Tools->>DB: INSERT INTO outbox_events (status=PENDING)
            DB-->>Tools: rows written
            Tools->>Tools: sessionExecutionResults.put(conversationId, successJson)
            Tools-->>Ollama: return successJson (ground truth, logged server-side unconditionally)
            Note over Tools,DB: proxy sees normal return -> COMMIT (both inserts)
            Ollama-->>Controller: stream onComplete
            Controller->>Tools: getExecutionResult(conversationId)
            Controller-->>Manager: SSE event "tool-result" (successJson)
            Controller->>Manager: emitter.complete()
        else saveShiftAndOutbox throws (e.g. constraint violation)
            Tools->>Tools: catch(Exception) logs failure, RETHROWS (does not return normally)
            Note over Tools,DB: exception propagates out of proxied method -> ROLLBACK<br/>(both inserts undone, no partial write)
            Tools--xOllama: exception propagates via MethodToolCallback / DefaultToolCallingManager<br/>(alwaysThrow(true) processor - no swallow-to-string fallback)
            Ollama--xController: stream error callback fires (not onComplete)
            Controller->>Controller: findToolExecutionException(error) -> build {"status":"FAILED","error":...}
            Controller->>Tools: getExecutionResult(conversationId) [discard stale NO_CALL placeholder]
            Controller-->>Manager: SSE event "tool-result" (FAILED json)
            Controller->>Manager: emitter.complete()
        end
    end

    par background loop, decoupled from the request above
        loop every 500ms, single @Transactional method
            Poller->>DB: SELECT * FROM outbox_events WHERE status='PENDING' FOR UPDATE SKIP LOCKED
            DB-->>Poller: pending rows (locks held for full cycle)
            Poller->>Kafka: send(wfm.audit.shifts, aggregateId, payload) [acks=all]
            Kafka-->>Poller: broker ack
            Poller->>DB: UPDATE outbox_events SET status='PROCESSED'
        end
    end
```

## 2. Deployment architecture

Only Postgres and Kafka are containerized in `compose.yaml`; the Spring Boot
app and Ollama both run directly on the host. Postgres and Kafka each publish a
non-default host port to avoid colliding with other local services (per
SPEC.md's hard requirement) — `54321` for Postgres and `9094` for Kafka's
external listener. Kafka runs single-node KRaft mode (combined broker +
controller role, no ZooKeeper), with a separate internal `PLAINTEXT` listener
(`kafka-wfm:9092`) that the app does not use — the app connects only via the
external listener on `localhost:9094`.

```mermaid
flowchart LR
    subgraph Host["Developer host"]
        Manager["Manager<br/>(HTTP/SSE client)"]

        subgraph App["Spring Boot app — :8081 (./mvnw, virtual threads on)"]
            Controller["WfmSchedulingController<br/>SSE endpoint + static X-API-Key check<br/>constructor-injects WfmSchedulingTools bean"]
            ChatClientNode["ChatClient (Spring AI 2.0.0)"]
            ToolsNode["WfmSchedulingTools<br/>@Service bean, AOP-proxied @Transactional<br/>(constructor-injected, not new'd)"]
            Repo["WfmRepository<br/>(JdbcTemplate)"]
            Poller["TransactionalOutboxPoller<br/>@Component, @Scheduled fixedDelay=500ms<br/>processOutboxQueue() is @Transactional"]
        end

        Ollama["Ollama — :11434<br/>model: llama3.1 (local process, not in compose)"]
    end

    subgraph Compose["compose.yaml (docker)"]
        Postgres["postgres-wfm-poc<br/>ankane/pgvector:latest<br/>host 54321 -> container 5432"]
        Kafka["kafka-wfm-poc<br/>apache/kafka:latest, KRaft (broker+controller combined)<br/>EXTERNAL host 9094 / internal PLAINTEXT kafka-wfm:9092"]
    end

    Manager -->|"GET /api/wfm/schedule-stream<br/>X-API-Key, conversationId, prompt"| Controller
    Controller -->|SSE: chat-progress, tool-result| Manager
    Controller --> ChatClientNode
    ChatClientNode <-->|"HTTP :11434"| Ollama
    ChatClientNode -->|"tool call (via Spring AOP proxy)"| ToolsNode
    ToolsNode --> Repo
    Repo -->|"jdbc:postgresql://localhost:54321/wfm_db<br/>INSERT shift_assignments + outbox_events<br/>(atomic on both success and failure - see Exception propagation gap)"| Postgres
    Poller -->|"SELECT ... FOR UPDATE SKIP LOCKED<br/>(within @Transactional method)"| Postgres
    Poller -->|"UPDATE status=PROCESSED"| Postgres
    Poller -->|"produce acks=all<br/>bootstrap-servers=localhost:9094<br/>topic wfm.audit.shifts"| Kafka
```
