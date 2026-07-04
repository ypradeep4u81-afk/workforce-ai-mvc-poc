Agentic AI POC for Workforce Scheduling — Production-Hardened Build
Context: prior POC
/Users/yalamanchili/Development/workforce-ai-poc (this repo's root) contains an archived prior POC on WebFlux + R2DBC. Its README.md documents why that stack was retired (Spring AI's VectorStore is JDBC-based framework-wide, forcing blocking calls across a reactive boundary) and carries forward the requirements below. Treat that repo as reference only — do not build on top of it or modify it. This is a fresh project.

Project location: /Users/yalamanchili/Development/workforce-ai-mvc-poc (this directory).

Objective
A manager sends a natural-language request (e.g. "assign EMP99 to the Cashier shift on 2026-07-06") to an SSE endpoint gated by a simple static API-key header (e.g. X-API-Key checked against a configured value) — enough to demonstrate the endpoint isn't wide open, without building full Spring Security/OAuth2 for a POC. A local LLM (Llama 3.1 via Ollama) extracts employeeId, date, and role, and invokes a @Tool-annotated method to persist the shift assignment in Postgres. Upon a confirmed database commit, an audit event is reliably published to Kafka with zero risk of event loss from a transient broker drop or app crash at commit time.

Stack — verified against Maven Central / spring.io directly on 2026-07-03, do not trust from memory
Re-verify these live before pinning if this session runs more than a few weeks after 2026-07-03 — versions and artifact IDs have changed across Spring AI major versions before and caused real bugs in the prior POC (wrong BOM version + wrong Ollama artifact ID).

Java 21+ (LTS) — Spring Boot 4.1 itself only requires Java 17, but spring.threads.virtual.enabled relies on the virtual-thread API introduced in Java 21. Pin <java.version>21</java.version> (or a later LTS) explicitly; do not assume 17 is enough.
Spring Boot 4.1.0 — confirmed GA on Maven Central (org.springframework.boot:spring-boot:4.1.0).
Spring MVC (spring-boot-starter-web), NOT WebFlux.
spring.threads.virtual.enabled=true — virtual threads as the uniform concurrency model.
Plain JDBC (Spring Data JPA or JdbcTemplate) against Postgres, NOT R2DBC.
Spring AI 2.0.0 — confirmed GA on Maven Central (org.springframework.ai:spring-ai-bom:2.0.0), compatible with Spring Boot 4.0.x/4.1.x per the Spring AI reference docs.
BOM:
<dependencyManagement>
  <dependencies>
    <dependency>
      <groupId>org.springframework.ai</groupId>
      <artifactId>spring-ai-bom</artifactId>
      <version>2.0.0</version>
      <type>pom</type>
      <scope>import</scope>
    </dependency>
  </dependencies>
</dependencyManagement>
Ollama starter: org.springframework.ai:spring-ai-starter-model-ollama
PGVector starter: org.springframework.ai:spring-ai-starter-vector-store-pgvector
Tool calling: @Tool / @ToolParam from org.springframework.ai.tool.annotation, registered via ChatClient.create(chatModel).tools(new XTools()). Spring AI 2.0 moved the tool-execution loop into the advisor chain (ToolCallingAdvisor) as a composable, observable component — this replaces 1.x's per-model private tool loop and the internalToolExecutionEnabled option, which was removed.
Spring Kafka for audit events (version managed by the Spring Boot BOM, no separate pin needed).
Kafka via a KRaft-mode image (e.g. apache/kafka or confluentinc/cp-kafka in KRaft mode) — no separate ZooKeeper container, one less service to align ports/health-checks for in docker-compose.yml.
Postgres via ankane/pgvector image (JDBC/PgVectorStore, spring.ai.vectorstore.pgvector.initialize-schema=true).
Why not WebFlux/R2DBC
Spring AI's VectorStore (any provider, any version, confirmed as of 2.0.0 GA — no artifact anywhere under org.springframework.ai has "r2dbc" or "reactive" in its name) is JDBC-based and blocking, framework-wide. Forcing it into WebFlux/R2DBC means smuggling blocking calls across the reactive boundary via Schedulers.boundedElastic() or a hand-rolled virtual-thread executor, with real risk of thread pinning depending on JDBC driver/connection-pool internals. This app's actual concurrency needs (dozens to low hundreds of concurrent manager SSE streams, not internet-scale fan-out) don't need WebFlux's benefits. Spring MVC + virtual threads gives a single, uniform concurrency model — simpler code, plain stack traces, no reactive/blocking impedance mismatch.

Hard requirements — build these in from day one, not as a retrofit
Never trust the LLM's streamed/narrated text as ground truth for tool success. Llama 3.1 has been observed reporting success when a tool call actually failed, and reporting failure when a tool call actually succeeded. Unconditionally log the tool's actual return value server-side (not just on error). The SSE endpoint must stream two distinct event types: a chat-progress event with the model's raw prose/tokens, and a structured tool-result event carrying the exact authoritative JSON the Java method returned — so any consumer can read ground truth without parsing LLM prose.
Transactional Outbox pattern for Kafka publish — not @TransactionalEventListener, not fire-after-commit. In the same DB transaction that writes the shift assignment, write the audit event payload to an outbox_events table. A separate background virtual-thread loop (or scheduler) polls unpublished rows, publishes to Kafka, and marks them processed only on confirmed broker acknowledgment. This closes the split-brain gap where a DB write commits but the app crashes before the Kafka publish — a real risk with a naive "publish right after commit" approach.
Concurrency safeguards, since virtual threads remove the web-tier thread ceiling and shift the bottleneck downstream:
Bound and protect the HikariCP pool (~20–50 connections); throttle the LLM tool-execution path with something like a Semaphore so excess DB demand queues in memory instead of exhausting the pool.
Rate-limit the entry point into ChatClient/Ollama — flooding a local Ollama instance via virtual threads causes latency spikes or 503s.
Resilient SSE lifecycle. Proxies/load balancers kill idle HTTP connections after 30–60s of silence:
Broadcast a heartbeat/ping comment every 15–20s across all active streams.
Hook emitter.onCompletion() / onTimeout() / onError() to purge dead client references immediately (avoid leaks).
Thread a unique conversationId/session id through the SSE endpoint from the start — required for chat-memory advisors later; there is no clean bolt-on afterward.
Align docker-compose.yml and application.yml exactly (DB name, credentials, ports) and verify end-to-end by actually connecting — don't assume. A silent mismatch previously produced a misleading password-auth error that was really a DB-name mismatch.
Use non-default host ports for Postgres/Kafka in docker-compose.yml (e.g. not bare 5432/9092) — a port collision with an unrelated local project previously caused this app to silently connect to the wrong database with different credentials.
Verify state directly, not through the LLM's response. After any live end-to-end test, check the actual DB row (psql) and actual Kafka topic contents (kafka-console-consumer) before declaring success.
Run a full mvn clean install (or equivalent) and confirm it's clean before calling any milestone done.
Suggested build order
Environment setup: bring up docker-compose.yml with non-default ports; connect directly via CLI tools (psql, Kafka console tools) to confirm credentials/db name match application.yml before writing any app code. Also confirm Ollama is installed and running locally (ollama list) and that llama3.1 is pulled (ollama pull llama3.1) — do this before step 2, since a missing model surfaces as a confusing tool-calling failure rather than a clear startup error.
Minimal vertical slice: SSE endpoint (with a required conversationId) → ChatClient + @Tool-calling → single transaction writing both the shifts row and the outbox_events row. Validate end-to-end against real Postgres before adding anything else.
Outbox poller + dual-event SSE stream: background virtual-thread loop publishing outbox rows to Kafka; wire the controller to emit both chat-progress and structured tool-result SSE events. Validate directly in psql and Kafka console tools.
Resilience layer: Semaphore-based DB pool protection, Ollama/ChatClient rate limiting, SSE heartbeat broadcaster.
Agent expansion: Spring AI chat-memory advisors using the already-threaded conversationId, then pgvector-backed RAG.
*Addendum (post-baseline):*
## Security posture (intentional, POC scope)
- DB credentials, Kafka config, and static API key are placeholder dev 
  values only — not rotated, not secrets, safe as committed to this repo.
- Tech stack itself (Spring Boot 4.1, virtual threads, outbox pattern, 
  concurrency safeguards) is built to production-grade patterns per SPEC.md 
  hard requirements — only credential handling is POC-simplified.
- If this repo is ever extended beyond local POC use, revisit before any 
  real deployment.