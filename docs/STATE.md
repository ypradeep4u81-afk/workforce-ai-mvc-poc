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

## Architecture — documented
Date: 2026-07-04. See [ARCHITECTURE.md](ARCHITECTURE.md) for a sequence diagram
of the actual request lifecycle and a flowchart of the actual deployment
topology, both drawn from the real controller/tool/poller/config code (not
SPEC.md's prose). Notes two implementation gaps found while documenting:
`WfmSchedulingTools` is a plain `new`'d POJO (not a Spring bean), so its
`@Transactional` on `assignShift` isn't proxied/enforced; and
`TransactionalOutboxPoller.processOutboxQueue()` isn't itself `@Transactional`.

## Transactional bug fix
Date: 2026-07-04. Scoped exactly to the two bugs documented in ARCHITECTURE.md. No other files touched.

### What changed
1. **[WfmSchedulingTools.java](../src/main/java/com/wfm/poc/tool/WfmSchedulingTools.java)** — added `@Service`. It's now a Spring-managed bean, so `@Transactional` on `assignShift` is backed by a real AOP proxy.
2. **[WfmSchedulingController.java](../src/main/java/com/wfm/poc/controller/WfmSchedulingController.java)** — constructor now takes `WfmSchedulingTools` as an injected parameter instead of calling `new WfmSchedulingTools(repository)`. This was the actual proxy-bypass: even with step 1's annotation, constructing the object manually inside the controller would still skip the container/proxy entirely.
3. **[TransactionalOutboxPoller.java](../src/main/java/com/wfm/poc/outbox/TransactionalOutboxPoller.java)** — added `@Transactional` to `processOutboxQueue()`, so the `FOR UPDATE SKIP LOCKED` row locks are held for the full poll cycle (fetch → Kafka publish → status update) instead of being released the instant the initial `SELECT` statement auto-committed.

### Confirmed: no other manual-instantiation-bypasses-proxy issues
`grep -rn "@Transactional"` across `src/main/java` found exactly one annotated method both before and after this fix (`WfmSchedulingTools.assignShift`) — `TransactionalOutboxPoller` had no prior `@Transactional` to bypass, it simply lacked the annotation. A second grep for `new [A-Z]` across `src/main/java` turned up only plain data/utility objects (`OutboxEvent`, `Semaphore`, `SseEmitter`, `ObjectMapper`) — none of these are Spring-managed components with their own annotations to bypass. No other instances of this bug class exist in the codebase.

### `mvn clean install` — BUILD SUCCESS, confirmed clean (re-run after revert of the temporary test hook below).

### E2E re-verification (happy path)
Restarted the running app (an older instance from before this fix was still bound to :8081 and had to be killed first — otherwise the fix wouldn't have been exercised). Ran the cURL test against a fresh employee/conversation pair (`EMP77`, then `EMP88` after the rollback test), then independently checked ground truth:
- **psql**: `shift_assignments` row present, correct values.
- **psql**: `outbox_events` row present, `status = PROCESSED`, `aggregate_id` matches.
- **kafka-console-consumer**: message present on `wfm.audit.shifts`, key/payload byte-for-byte match the DB row.

This confirms presence of correct data post-fix, consistent with the Pre-Session 5 verification. It does **not** by itself prove atomicity — that required the failure-case test below.

### Failure-case test — atomicity is only partially real; a second bug was found
Per session instructions, temporarily added `if (true) { throw new RuntimeException(...); }` in [WfmRepository.saveShiftAndOutbox](../src/main/java/com/wfm/poc/repository/WfmRepository.java) between the `shift_assignments` insert and the `outbox_events` insert, rebuilt, restarted the app, and called the endpoint with a new employee (`EMP_ROLLBACK_TEST`).

**Expected** (if the transaction boundary from this session's fix were fully real): the `shift_assignments` insert rolls back along with the missing `outbox_events` insert — zero rows in either table for `EMP_ROLLBACK_TEST`.

**Actual result**:
```
docker exec postgres-wfm-poc psql -U wfm_manager -d wfm_db -c "SELECT * FROM shift_assignments WHERE employee_id = 'EMP_ROLLBACK_TEST';"
 id |    employee_id    | shift_date |  role   |         created_at
----+-------------------+------------+---------+----------------------------
  4 | EMP_ROLLBACK_TEST | 2026-07-11 | Cashier | 2026-07-04 12:02:25.982871
(1 row)

docker exec postgres-wfm-poc psql -U wfm_manager -d wfm_db -c "SELECT * FROM outbox_events WHERE aggregate_id = 'EMP_ROLLBACK_TEST';"
(0 rows)
```
The `shift_assignments` row **committed anyway**, with no matching outbox row — exactly the split-brain state the Transactional Outbox pattern exists to prevent.

**Root cause**: `WfmSchedulingTools.assignShift` wraps its body in `try { ... } catch (Exception e) { ...; return errorJson; }`. Spring's `@Transactional` proxy only triggers a rollback when an (unchecked) exception propagates *out of* the proxied method. Because `assignShift` catches the exception internally and returns a normal `errorJson` string instead of rethrowing, the proxy sees what looks like a successful, exception-free invocation and commits the transaction — including the partial write made before the throw. This is a **separate, pre-existing bug** from the two this session was scoped to fix: it exists independently of whether `WfmSchedulingTools` is a Spring bean, and would reproduce with any DB exception thrown inside `saveShiftAndOutbox` (not just this synthetic test hook) — e.g. a constraint violation on the second insert would currently leave the first insert committed.

The test hook was fully reverted immediately after this observation (`git diff` on `WfmRepository.java` confirmed a clean no-op diff post-revert), the app was rebuilt and re-verified clean (`mvn clean install` + a second happy-path E2E run for `EMP88`, matched in psql/Kafka), and the `EMP_ROLLBACK_TEST` row was deleted from `shift_assignments` to leave the DB in the state it was in before this test.

**Not fixed in this session** (out of scope per session instructions to touch only the two documented bugs) — flagging for a follow-up: `assignShift`'s catch block should rethrow (or the method should not catch broadly at all) so the `@Transactional` proxy actually sees the failure and rolls back. As it stands today, the fixes in this session make the proxy *exist*, but the try/catch means the failure path still isn't atomic.

## AOP proxy audit
Date: 2026-07-04. Verification-only pass, no code changes (the one prior instance of this bug pattern was already fixed above).

Ran the requested search plus two broader ones to be thorough:
```
grep -rn "new.*Tools\|new.*Service\|new.*Repository\|new.*Poller" src/main/java --include="*.java"
```
→ **zero matches.** The only historical hit for this pattern was `new WfmSchedulingTools(repository)` in `WfmSchedulingController`, already fixed earlier in this session (injected as a bean instead).

Broader sweep, to catch cases the naming-convention grep above would miss (a manually-`new`'d class doesn't have to have "Tools/Service/Repository/Poller" in its name):
```
grep -rln "@Transactional\|@Async\|@Cacheable\|@CacheEvict\|@CachePut\|@Retryable\|@PreAuthorize\|@PostAuthorize\|@Secured" src/main/java --include="*.java"
```
→ Exactly two classes carry any AOP-relevant annotation in the entire codebase: `WfmSchedulingTools` (`@Transactional`) and `TransactionalOutboxPoller` (`@Transactional`). Both are now `@Service`/`@Component` beans obtained via constructor injection — confirmed no other file constructs either with `new`.
```
grep -rn "@Autowired" src/main/java --include="*.java"
```
→ Zero matches. All dependency injection in the codebase is constructor-based; there are no `@Autowired` fields/setters that could be silently null if a class were manually instantiated instead of container-managed.
```
grep -rn "new [A-Z][A-Za-z]*(" src/main/java --include="*.java"
```
→ Six remaining manual-`new` call sites, all of them out of scope for AOP proxying:
- `OutboxEvent` (×2, in `WfmRepository` and `WfmSchedulingTools`) — confirmed via direct read of [OutboxEvent.java](../src/main/java/com/wfm/poc/domain/OutboxEvent.java): a plain POJO/data-holder, no annotations, no Spring stereotype, not a bean anywhere in the app. Manual `new` is correct here — it's a DTO, not a service.
- `Semaphore` (×2) — JDK `java.util.concurrent` class, not Spring-managed, no annotations possible.
- `SseEmitter` (×1) — Spring MVC framework class deliberately created per-HTTP-request (`SseEmitter(300_000L)`); it isn't meant to be a singleton bean by design, this is the framework's intended usage pattern.
- `ObjectMapper` (×1) — Jackson utility class, no AOP-relevant annotations, correctly stateless and instance-per-use here.

**Conclusion**: the two bugs fixed earlier in this session were the only instances of this bug class in the codebase. No further proxy-bypass issues found; nothing else required fixing.