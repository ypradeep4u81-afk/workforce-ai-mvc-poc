# Architecture — as implemented

This document reflects the code actually present in this repo as of 2026-07-04
(post-cleanup, `mvn clean install` green — see [STATE.md](STATE.md)). It is not
a description of SPEC.md's aspirational design; every element below was
confirmed by reading
[WfmSchedulingController.java](../src/main/java/com/wfm/poc/controller/WfmSchedulingController.java),
[WfmSchedulingTools.java](../src/main/java/com/wfm/poc/tool/WfmSchedulingTools.java),
[WfmRepository.java](../src/main/java/com/wfm/poc/repository/WfmRepository.java),
[TransactionalOutboxPoller.java](../src/main/java/com/wfm/poc/outbox/TransactionalOutboxPoller.java),
[application.properties](../src/main/resources/application.properties), and
[compose.yaml](../compose.yaml).

Two implementation details worth flagging up front because they affect how to
read the diagrams below:
- `WfmSchedulingTools` is constructed with plain `new WfmSchedulingTools(repository)`
  in the controller constructor ([WfmSchedulingController.java:30](../src/main/java/com/wfm/poc/controller/WfmSchedulingController.java#L30)),
  not injected as a Spring bean. Its `@Transactional` annotation on `assignShift`
  is therefore not backed by a Spring AOP proxy — the two inserts in
  `saveShiftAndOutbox` run as two independent auto-committed statements, not a
  single atomic transaction.
- `TransactionalOutboxPoller.processOutboxQueue()` is not itself `@Transactional`;
  its `SELECT ... FOR UPDATE SKIP LOCKED` executes and releases its row lock
  within that single auto-committed statement.

## 1. Functional flow (request lifecycle)

A manager opens the SSE endpoint with a static API-key header, a caller-supplied
`conversationId`, and a natural-language `prompt`. The controller streams model
tokens as they arrive, then — once the model's tool call has actually returned —
streams the tool's authoritative JSON as a second, distinct event type, per
`WfmSchedulingController.streamScheduling` and `WfmSchedulingTools.assignShift`.
Outbox publication to Kafka happens on a separate 500ms poll loop, fully
decoupled from the request/response cycle.

```mermaid
sequenceDiagram
    actor Manager
    participant Controller as WfmSchedulingController
    participant Ollama as ChatClient / Ollama (llama3.1)
    participant Tools as WfmSchedulingTools
    participant DB as Postgres (shift_assignments + outbox_events)
    participant Poller as TransactionalOutboxPoller
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
        Ollama->>Tools: invoke assignShift(employeeId, shiftDate, role, conversationId)
        Tools->>Tools: acquire dbThrottler permit (1 of 25)
        Tools->>DB: INSERT INTO shift_assignments
        Tools->>DB: INSERT INTO outbox_events (status=PENDING)
        DB-->>Tools: rows written
        Tools->>Tools: sessionExecutionResults.put(conversationId, successJson)
        Tools-->>Ollama: return successJson (ground truth, logged server-side unconditionally)
        Ollama-->>Controller: stream onComplete
        Controller->>Tools: getExecutionResult(conversationId)
        Controller-->>Manager: SSE event "tool-result" (authoritative JSON)
        Controller->>Manager: emitter.complete()
    end

    par background loop, decoupled from the request above
        loop every 500ms
            Poller->>DB: SELECT * FROM outbox_events WHERE status='PENDING' FOR UPDATE SKIP LOCKED
            DB-->>Poller: pending rows
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
            Controller["WfmSchedulingController<br/>SSE endpoint + static X-API-Key check"]
            ChatClientNode["ChatClient (Spring AI 2.0.0)"]
            ToolsNode["WfmSchedulingTools<br/>(plain POJO, new'd by controller)"]
            Repo["WfmRepository<br/>(JdbcTemplate)"]
            Poller["TransactionalOutboxPoller<br/>@Scheduled fixedDelay=500ms"]
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
    ChatClientNode -->|tool call| ToolsNode
    ToolsNode --> Repo
    Repo -->|"jdbc:postgresql://localhost:54321/wfm_db<br/>INSERT shift_assignments + outbox_events"| Postgres
    Poller -->|"SELECT ... FOR UPDATE SKIP LOCKED"| Postgres
    Poller -->|"UPDATE status=PROCESSED"| Postgres
    Poller -->|"produce acks=all<br/>bootstrap-servers=localhost:9094<br/>topic wfm.audit.shifts"| Kafka
```
