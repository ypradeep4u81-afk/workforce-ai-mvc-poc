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

## Verification — Pre-Session 5
Date: 2026-07-04. Verification-only session (no code changes made). Ran against the containers already up from the prior manual E2E test (uptime ~2h at start of this session), not a fresh bring-up.

### 1. `mvn clean install` — FAILS (discrepancy vs. STATE.md assumption)
Command:
```
./mvnw clean install
```
Result: **BUILD FAILURE**, not clean. Main compile and resource steps succeed (8 source files compile fine), but the test phase fails:
```
[ERROR] Tests run: 1, Failures: 0, Errors: 1, Skipped: 0
[ERROR] workforce_ai_mvc_poc.WorkforceAiMvcPocApplicationTests » IllegalState
  Unable to find a @SpringBootConfiguration by searching packages upwards from the test.
```
Root cause: [src/test/java/workforce_ai_mvc_poc/WorkforceAiMvcPocApplicationTests.java](../src/test/java/workforce_ai_mvc_poc/WorkforceAiMvcPocApplicationTests.java) is still in the **old underscored package** (`workforce_ai_mvc_poc`), left behind by the "consolidated from an earlier underscored structure" refactor mentioned above. The real `@SpringBootApplication` class now lives at `com.wfm.poc.WorkforceAiMvcPocApplication` ([src/main/java/com/wfm/poc/WorkforceAiMvcPocApplication.java](../src/main/java/com/wfm/poc/WorkforceAiMvcPocApplication.java)), so Spring Boot's upward package search from the test's package never finds it.
Also found a second orphaned leftover in the same old package: [src/main/java/workforce_ai_mvc_poc/domain/ShiftAssignment.java](../src/main/java/workforce_ai_mvc_poc/domain/ShiftAssignment.java) — an unused duplicate record of `com.wfm.poc.domain.ShiftAssignment`. It compiles (harmless dead code) but confirms the package consolidation was incomplete.
Per session instructions this was verification-only — **not fixed**, just documented. Needs cleanup before Session 5 work starts.

### 2. Infrastructure reachability — CONFIRMED
Containers were already running (not started fresh this session):
```
docker compose -f compose.yaml ps
```
```
NAME               IMAGE                    STATUS                       PORTS
kafka-wfm-poc      apache/kafka:latest      Up About an hour             0.0.0.0:9094->9094/tcp
postgres-wfm-poc   ankane/pgvector:latest   Up About an hour (healthy)   0.0.0.0:54321->5432/tcp
```
Port reachability confirmed directly (not inferred from app logs):
```
nc -zv localhost 9094   → Connection to localhost port 9094 [tcp/*] succeeded!
nc -zv localhost 54321  → Connection to localhost port 54321 [tcp/*] succeeded!
```
Postgres query reachability (host has no local `psql`; used `docker exec` into the running container instead):
```
docker exec postgres-wfm-poc psql -U wfm_manager -d wfm_db -c "SELECT 1 AS reachable;"
```
```
 reachable
-----------
         1
```

### 3. Postgres direct inspection — CONFIRMED, matches STATE.md claim
```
docker exec postgres-wfm-poc psql -U wfm_manager -d wfm_db -c "SELECT * FROM shift_assignments WHERE employee_id = 'EMP99';"
```
```
 id | employee_id | shift_date |  role   |         created_at
----+-------------+------------+---------+----------------------------
  1 | EMP99       | 2026-07-06 | Cashier | 2026-07-04 10:05:22.810458
```
```
docker exec postgres-wfm-poc psql -U wfm_manager -d wfm_db -c "SELECT * FROM outbox_events;"
```
```
                  id                  |  aggregate_type  | aggregate_id |  event_type   |                                              payload                                              |  status   |         created_at
--------------------------------------+------------------+--------------+---------------+---------------------------------------------------------------------------------------------------+-----------+----------------------------
 537a2c3f-ebbc-46a4-9e3d-7e9a1e31edbf | SHIFT_ASSIGNMENT | EMP99        | SHIFT_CREATED | {"role":"Cashier","conversationId":"conv_2026_07_04_01","employeeId":"EMP99","date":"2026-07-06"} | PROCESSED | 2026-07-04 10:05:22.804751
```
Confirmed: row exists with correct `employee_id`/`shift_date`/`role`, and the outbox row status is `PROCESSED` (not just inserted), matching the shift row's `aggregate_id`.

### 4. Kafka direct inspection — CONFIRMED, payload matches DB row exactly
No local kafka CLI tools on host (`kafka-console-consumer.sh` not found), so ran the consumer inside the broker container instead:
```
docker exec kafka-wfm-poc /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```
```
__consumer_offsets
wfm.audit.shifts
```
```
docker exec kafka-wfm-poc /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic wfm.audit.shifts --from-beginning --property print.key=true --property key.separator=" | " --timeout-ms 8000
```
```
EMP99 | {"role":"Cashier","conversationId":"conv_2026_07_04_01","employeeId":"EMP99","date":"2026-07-06"}
Processed a total of 1 messages
```
Kafka record key (`EMP99`) matches `outbox_events.aggregate_id`; record value is byte-for-byte identical to `outbox_events.payload`. This is a genuine field-by-field comparison, not just "a message exists."

### Summary of discrepancies found vs. STATE.md
- ❌ **`mvn clean install` is NOT clean** — fails in the test phase due to a leftover test class in the pre-consolidation `workforce_ai_mvc_poc` package that can't locate the `com.wfm.poc` Spring Boot configuration. Main/resources compilation itself is fine.
- ⚠️ A second leftover file (`src/main/java/workforce_ai_mvc_poc/domain/ShiftAssignment.java`) is unused dead code from the same incomplete package consolidation — compiles but should be removed.
- ✅ Postgres `shift_assignments` EMP99 row: confirmed, values match.
- ✅ Postgres `outbox_events` row: confirmed `PROCESSED`, payload matches.
- ✅ Kafka `wfm.audit.shifts` topic: confirmed message exists, key and payload match the DB row exactly.
- ℹ️ Infra containers were already running at session start (~1hr uptime) — this session did not perform a fresh `docker compose up`; reachability was still independently re-verified via `nc` and direct queries rather than assumed.

**Action needed before Session 5**: delete/fix the two leftover `workforce_ai_mvc_poc`-package files so `mvn clean install` passes cleanly, per SPEC.md's "run a full mvn clean install and confirm it's clean before calling any milestone done" requirement.

## Post-cleanup verification
Date: 2026-07-04. Cleanup-only session, scoped exactly to the build failure documented above. No config, ports, or application logic touched.

### What was confirmed before deleting
- `grep -rn "workforce_ai_mvc_poc.domain.ShiftAssignment\|import workforce_ai_mvc_poc"` across all `.java` files (excluding `target/`) returned **zero matches** outside the two stray files themselves — confirmed `workforce_ai_mvc_poc.domain.ShiftAssignment` was truly unreferenced dead code, not silently relied on elsewhere.
- `find src/test -iname "*.java"` showed `WorkforceAiMvcPocApplicationTests.java` was the **only** test file in the repo — so deleting it without replacement would drop test coverage to zero. A minimal replacement was required, not optional.

### What was deleted
- `src/test/java/workforce_ai_mvc_poc/WorkforceAiMvcPocApplicationTests.java` (leftover pre-consolidation test, wrong package for `@SpringBootConfiguration` discovery)
- `src/main/java/workforce_ai_mvc_poc/domain/ShiftAssignment.java` (unreferenced duplicate of `com.wfm.poc.domain.ShiftAssignment`)
- The now-empty leftover directories: `src/test/java/workforce_ai_mvc_poc/`, `src/main/java/workforce_ai_mvc_poc/domain/`, `src/main/java/workforce_ai_mvc_poc/`

### What was added
- `src/test/java/com/wfm/poc/WorkforceAiMvcPocApplicationTests.java` — minimal `@SpringBootTest` context-load smoke test (`contextLoads()`), identical in intent to the deleted one but in the correct package (`com.wfm.poc`) so Spring Boot's upward package search finds `com.wfm.poc.WorkforceAiMvcPocApplication`.

### `./mvnw clean install` — confirmed BUILD SUCCESS
```
[INFO] Running com.wfm.poc.WorkforceAiMvcPocApplicationTests
...
[INFO] Found @SpringBootConfiguration com.wfm.poc.WorkforceAiMvcPocApplication for test class com.wfm.poc.WorkforceAiMvcPocApplicationTests
...
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
[INFO]
[INFO] --- jar:3.5.0:jar (default-jar) @ workforce-ai-mvc-poc ---
[INFO] Building jar: /Users/yalamanchili/Development/workforce-ai-mvc-poc/target/workforce-ai-mvc-poc-0.0.1-SNAPSHOT.jar
[INFO]
[INFO] --- spring-boot:4.1.0:repackage (repackage) @ workforce-ai-mvc-poc ---
[INFO] Replacing main artifact ... with repackaged archive, adding nested dependencies in BOOT-INF/.
[INFO]
[INFO] --- install:3.1.4:install (default-install) @ workforce-ai-mvc-poc ---
[INFO] Installing .../workforce-ai-mvc-poc-0.0.1-SNAPSHOT.jar to /Users/yalamanchili/.m2/repository/com/wfm/poc/workforce-ai-mvc-poc/0.0.1-SNAPSHOT/workforce-ai-mvc-poc-0.0.1-SNAPSHOT.jar
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time:  3.806 s
```
The context-load test actually connected the full app context to the real running Postgres (HikariCP pool started, PGVectorStore schema check ran against the live `vector_store` table) — this was not a mocked/sliced test, it's a genuine full Spring context boot. `mvn clean install` is now clean end to end.