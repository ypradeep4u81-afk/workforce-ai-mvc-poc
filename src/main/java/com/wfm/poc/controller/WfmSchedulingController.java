package com.wfm.poc.controller;

import com.wfm.poc.repository.WfmRepository;
import com.wfm.poc.tool.WfmSchedulingTools;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

@RestController
@RequestMapping("/api/wfm")
public class WfmSchedulingController {

    private final ChatModel chatModel;
    private final WfmSchedulingTools schedulingTools;
    private final String configuredApiKey = "sk_wfm_poc_2026_secure_keystring";
    private final Semaphore ollamaLimiter = new Semaphore(4); 
    private final ExecutorService heartbeatScheduler = Executors.newVirtualThreadPerTaskExecutor();

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
                // Use defaultTools to safely register POJO method tool schemas into the chat client builder
                ChatClient chatClient = ChatClient.builder(chatModel)
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
                                    emitter.send(SseEmitter.event().name("error").data(error.getMessage()));
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
}
