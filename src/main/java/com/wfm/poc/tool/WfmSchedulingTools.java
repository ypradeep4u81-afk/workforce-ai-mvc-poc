package com.wfm.poc.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wfm.poc.domain.OutboxEvent;
import com.wfm.poc.repository.WfmRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.ai.tool.annotation.ToolParam;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

public class WfmSchedulingTools {
    private static final Logger log = LoggerFactory.getLogger(WfmSchedulingTools.class);
    private final WfmRepository repository;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Semaphore dbThrottler = new Semaphore(25); 
    private final Map<String, String> sessionExecutionResults = new ConcurrentHashMap<>();

    public WfmSchedulingTools(WfmRepository repository) {
        this.repository = repository;
    }

    public void registerSession(String conversationId) {
        sessionExecutionResults.put(conversationId, "{\"status\":\"NO_CALL\"}");
    }

    public String getExecutionResult(String conversationId) {
        return sessionExecutionResults.remove(conversationId);
    }

    @Tool(description = "Assigns an employee to a specific work role shift on a precise calendar date.")
    @Transactional
    public String assignShift(
            @ToolParam(description = "The unique identifier of the employee, e.g. EMP99") String employeeId,
            @ToolParam(description = "The date target for the shift in ISO standard format YYYY-MM-DD") String shiftDate,
            @ToolParam(description = "The deployment role, e.g. Cashier, Manager") String role,
            @ToolParam(description = "The pass-through tracking conversation session identifier") String conversationId) {
        
        dbThrottler.acquireUninterruptibly();
        try {
            LocalDate parsedDate = LocalDate.parse(shiftDate);
            
            OutboxEvent event = new OutboxEvent();
            event.setId(UUID.randomUUID());
            event.setAggregateType("SHIFT_ASSIGNMENT");
            event.setAggregateId(employeeId);
            event.setEventType("SHIFT_CREATED");
            
            Map<String, Object> payloadMap = Map.of("employeeId", employeeId, "date", shiftDate, "role", role, "conversationId", conversationId);
            event.setPayload(objectMapper.writeValueAsString(payloadMap));
            event.setStatus("PENDING");
            event.setCreatedAt(LocalDateTime.now());

            repository.saveShiftAndOutbox(employeeId, parsedDate, role, event);
            
            String successJson = String.format("{\"status\":\"SUCCESS\",\"employeeId\":\"%s\",\"date\":\"%s\",\"role\":\"%s\"}", employeeId, shiftDate, role);
            log.info("[GROUND_TRUTH_TOOL_SUCCESS] Session: {} -> {}", conversationId, successJson);
            sessionExecutionResults.put(conversationId, successJson);
            return successJson;
            
        } catch (Exception e) {
            String errorJson = String.format("{\"status\":\"FAILED\",\"error\":\"%s\"}", e.getMessage());
            log.error("[GROUND_TRUTH_TOOL_CRITICAL_FAILURE] Session: {}", conversationId, e);
            sessionExecutionResults.put(conversationId, errorJson);
            return errorJson;
        } finally {
            dbThrottler.release();
        }
    }
}
