package com.wfm.poc.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wfm.poc.repository.WfmRepository;
import com.wfm.poc.tool.WfmSchedulingTools;
import io.micrometer.observation.ObservationRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.client.advisor.ToolCallingAdvisor;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.model.tool.ToolCallingManager;
import org.springframework.ai.tool.execution.DefaultToolExecutionExceptionProcessor;
import org.springframework.ai.tool.execution.ToolExecutionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

@RestController
@RequestMapping("/api/wfm")
public class WfmSchedulingController {

    private static final Logger log = LoggerFactory.getLogger(WfmSchedulingController.class);

    private final ChatModel chatModel;
    private final WfmSchedulingTools schedulingTools;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String configuredApiKey = "sk_wfm_poc_2026_secure_keystring";
    private final Semaphore ollamaLimiter = new Semaphore(4);
    private final ExecutorService heartbeatScheduler = Executors.newVirtualThreadPerTaskExecutor();

    // ChatClient.builder(chatModel) (single-arg factory) builds its own internal
    // ToolCallingManager from scratch and does NOT consult Spring's autoconfigured
    // spring.ai.tools.* properties or beans - confirmed by reading DefaultChatClientBuilder
    // (it passes a null ToolCallingAdvisor.Builder, which falls back to
    // ToolCallingManager.builder().build(), default alwaysThrow=false). That default swallows a
    // tool's thrown exception into a plain string tool-response instead of letting it propagate,
    // which would silently defeat the assignShift rollback fix. Explicitly wiring an
    // alwaysThrow(true) processor here is required so the exception actually reaches the
    // ChatClient stream's error callback.
    private final ToolCallingManager toolCallingManager = ToolCallingManager.builder()
            .toolExecutionExceptionProcessor(DefaultToolExecutionExceptionProcessor.builder().alwaysThrow(true).build())
            .build();

    public WfmSchedulingController(ChatModel chatModel, WfmRepository repository, WfmSchedulingTools schedulingTools) {
        this.chatModel = chatModel;
        this.schedulingTools = schedulingTools;
        repository.initTables();
    }

    @GetMapping(value = "/schedule-stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public ResponseEntity<SseEmitter> streamScheduling(
            @RequestHeader("X-API-Key") String apiKey,
            @RequestParam("conversationId") String conversationId,
            @RequestParam("prompt") String prompt) {

        if (!configuredApiKey.equals(apiKey)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }

        SseEmitter emitter = new SseEmitter(300_000L);
        schedulingTools.registerSession(conversationId);

        var heartbeatFuture = heartbeatScheduler.submit(() -> {
            try {
                while (true) {
                    Thread.sleep(15_000);
                    emitter.send(SseEmitter.event().comment("ping-heartbeat"));
                }
            } catch (InterruptedException | IOException e) {
                // Connection closed or task finished naturally
            }
        });

        emitter.onCompletion(() -> heartbeatFuture.cancel(true));
        emitter.onTimeout(() -> heartbeatFuture.cancel(true));
        emitter.onError((e) -> heartbeatFuture.cancel(true));

        Thread.startVirtualThread(() -> {
            ollamaLimiter.acquireUninterruptibly();
            try {
                // FIXED FOR SPRING AI 2.0.0 GA:
                // Use defaultTools to safely register POJO method tool schemas into the chat client builder.
                // Pass the explicit alwaysThrow(true) ToolCallingAdvisor.Builder (see toolCallingManager
                // field javadoc) so a thrown tool exception propagates to this stream instead of being
                // swallowed into a string tool-response.
                ChatClient chatClient = ChatClient.builder(chatModel, ObservationRegistry.NOOP, null, null,
                                ToolCallingAdvisor.builder().toolCallingManager(toolCallingManager))
                        .defaultTools(schedulingTools)
                        .build();

                chatClient.prompt()
                        .user(String.format("Prompt: '%s'. Pass this conversationId to the tool exactly: '%s'", prompt, conversationId))
                        .stream()
                        .content()
                        .subscribe(
                            token -> {
                                try {
                                    emitter.send(SseEmitter.event().name("chat-progress").data(token));
                                } catch (Exception ignored) {}
                            },
                            error -> {
                                try {
                                    // Call site for error-to-JSON conversion, now that assignShift
                                    // rethrows instead of swallowing: by the time this callback
                                    // fires, the @Transactional proxy has already rolled back (or
                                    // never partially committed in the first place). Only synthesize
                                    // the tool's structured FAILED contract for genuine tool-call
                                    // failures - leave unrelated stream errors (e.g. Ollama down) on
                                    // the existing generic "error" event.
                                    Throwable toolFailure = findToolExecutionException(error);
                                    schedulingTools.getExecutionResult(conversationId); // discard stale NO_CALL placeholder, avoid map leak
                                    if (toolFailure != null) {
                                        Throwable rootCause = toolFailure.getCause() != null ? toolFailure.getCause() : toolFailure;
                                        String message = rootCause.getMessage() != null ? rootCause.getMessage() : rootCause.toString();
                                        String errorJson = objectMapper.writeValueAsString(Map.of("status", "FAILED", "error", message));
                                        log.error("[GROUND_TRUTH_TOOL_CRITICAL_FAILURE] Session: {} -> {}", conversationId, errorJson);
                                        emitter.send(SseEmitter.event().name("tool-result").data(errorJson));
                                    } else {
                                        emitter.send(SseEmitter.event().name("error").data(error.getMessage()));
                                    }
                                    emitter.complete();
                                } catch (Exception ignored) {}
                            },
                            () -> {
                                try {
                                    String truthPayload = schedulingTools.getExecutionResult(conversationId);
                                    if (truthPayload != null) {
                                        emitter.send(SseEmitter.event().name("tool-result").data(truthPayload));
                                    }
                                    emitter.complete();
                                } catch (Exception e) {
                                    emitter.completeWithError(e);
                                }
                            }
                        );
            } finally {
                ollamaLimiter.release();
            }
        });

        return ResponseEntity.ok(emitter);
    }

    private static ToolExecutionException findToolExecutionException(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof ToolExecutionException tee) {
                return tee;
            }
            current = current.getCause();
        }
        return null;
    }
}
