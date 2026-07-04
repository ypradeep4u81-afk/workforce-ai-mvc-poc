# Learnings — Spring Boot / Spring AI gotchas

This document is distinct from [ARCHITECTURE.md](ARCHITECTURE.md). ARCHITECTURE.md
describes what this system looks like *now*. This document captures the
non-obvious lessons discovered while getting there — written so they're useful
to anyone building a Spring Boot / Spring AI app, not just someone auditing
this repo.

## 1. `@Transactional` needs a real Spring AOP proxy — `new` bypasses it silently

**Lesson:** `@Transactional` (and `@Async`, `@Cacheable`, `@Retryable`, `@PreAuthorize`,
and every other AOP-backed annotation) only takes effect on beans obtained
*through the Spring container*. If you construct the object yourself with
`new`, you get a plain Java object with no interceptor chain — the annotation
is silently ignored. There's no warning or error; the code compiles and runs,
it just doesn't do the thing the annotation promises. The fix is always the
same shape: make the class a bean (`@Service`/`@Component`) and get it via
constructor injection, never `new` it up at the call site.

**What happened here:** `WfmSchedulingTools` was instantiated with
`new WfmSchedulingTools(repository)` directly inside the controller. Adding
`@Transactional` to `assignShift` did nothing on its own — the class also had
to become an injected `@Service` bean before the annotation had any proxy to
run through.

**Detail:** see STATE.md → "Transactional bug fix" and "AOP proxy audit".

## 2. A swallowed exception defeats `@Transactional` rollback even with a real proxy

**Lesson:** Spring's transactional proxy decides commit-vs-rollback purely by
watching whether an (unchecked) exception *propagates out of* the proxied
method — it has no visibility into what happened inside a `try/catch` that
recovers locally. A method that catches an exception and returns a normal
value (even an "error" DTO or error-shaped JSON string) looks exactly like a
successful call to the proxy, so it commits. This means input validation,
partial-write handling, and error signaling inside a `@Transactional` method
must be designed around "let it throw," not "catch and report."

**What happened here:** `assignShift` wrapped its body in
`try { ... } catch (Exception e) { ...; return errorJson; }`. A failure
injected between the two inserts left the `shift_assignments` row committed
with no matching `outbox_events` row — the exact split-brain state the
Transactional Outbox pattern exists to prevent — because the catch block
absorbed the exception and returned normally instead of rethrowing.

**Detail:** see STATE.md → "Transactional bug fix" (failure-case test) and
"Exception propagation fix".

## 3. Not every `ChatClient` construction path goes through Spring AI autoconfiguration

**Lesson:** Spring AI properties like `spring.ai.tools.throw-exception-on-error`
are wired into autoconfigured beans (`ToolCallingManager`, `ChatClient.Builder`,
etc.) via `@ConditionalOnMissingBean` autoconfiguration. If your code builds a
`ChatClient` through a static factory method instead of injecting the
autoconfigured `ChatClient.Builder`, you can silently skip that
autoconfiguration entirely — the factory falls back to library defaults, and
a property that looks like it should control behavior does nothing. The
general lesson: when a Spring Boot property "isn't working," check whether the
object it's supposed to configure is actually the container-managed instance,
or one your own code assembled by hand.

**What happened here:** the controller built its `ChatClient` via the static
`ChatClient.builder(chatModel)` factory. That path passes a `null`
`ToolCallingAdvisor.Builder`, which falls back to
`ToolCallingManager.builder().build()` — a fresh instance with the library
default `alwaysThrow=false` — completely bypassing the
`spring.ai.tools.throw-exception-on-error` property. Even after fixing #2
above (rethrowing in `assignShift`), the tool-calling manager caught that
rethrown exception and converted it back into a plain string tool response,
silently undoing the fix. The real fix was constructing the
`ToolCallingManager` explicitly with `alwaysThrow(true)` and wiring it into
the `ChatClient` via the multi-arg builder overload.

**Detail:** see STATE.md → "Exception propagation fix" ("API gotcha found and
worked around").

## 4. SSE is a response-streaming convention, not an alternative to REST

**Lesson:** Server-Sent Events isn't a separate protocol or a different kind
of endpoint — it's a plain HTTP response with
`Content-Type: text/event-stream` whose body is written incrementally instead
of all at once. Everything else about the request (method, headers, routing,
status codes) is ordinary REST/HTTP. It's easy to over-think SSE endpoints as
needing special routing or a different mental model; in Spring MVC it's just
a normal `@GetMapping` returning an `SseEmitter` instead of a POJO.

**What happened here:** the scheduling endpoint is a completely standard
`@GetMapping("/schedule-stream")` with `produces = MediaType.TEXT_EVENT_STREAM_VALUE`,
taking ordinary query parameters (`conversationId`, `prompt`) and an ordinary
header (`X-API-Key`). The only thing distinguishing it from any other REST GET
is that the response body streams multiple named events (`chat-progress`,
`tool-result`) instead of returning one JSON payload.

**Detail:** see STATE.md → "Verified end-to-end flow" for the endpoint as
actually implemented.

## 5. Browser `EventSource` can't set custom headers or use non-GET methods

**Lesson:** the standard browser `EventSource` API used to consume SSE from
JavaScript is deliberately minimal: it only issues `GET` requests and gives
you no way to set custom request headers (no `X-API-Key`, no `Authorization`
beyond what a session cookie already provides). This is a constraint of the
browser API itself, not of SSE or of any particular server framework. If a UI
ever needs to consume an SSE endpoint that's gated by a custom header, the
options are: move the credential into a query parameter or cookie, front the
endpoint with a proxy that injects the header, or use `fetch` with a
streaming response reader instead of `EventSource`.

**What happened here:** this endpoint is currently exercised with `curl`,
which can set arbitrary headers freely — so the `X-API-Key` header check has
never been exercised through an actual browser client. This is a real
constraint to plan around before building any browser-based UI against
`/api/wfm/schedule-stream`, since `EventSource` alone won't be able to send
that header.

**Detail:** see STATE.md → "Verified end-to-end flow" for how the API key is
currently checked (static header, tested via curl only).
