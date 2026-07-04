package com.wfm.poc.outbox;

import com.wfm.poc.domain.OutboxEvent;
import com.wfm.poc.repository.WfmRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Component
@EnableScheduling
public class TransactionalOutboxPoller {
    private static final Logger log = LoggerFactory.getLogger(TransactionalOutboxPoller.class);

    private final WfmRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final String TOPIC = "wfm.audit.shifts";

    public TransactionalOutboxPoller(WfmRepository repository, KafkaTemplate<String, String> kafkaTemplate) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void processOutboxQueue() {
        List<OutboxEvent> pendingEvents = repository.fetchPendingEvents();
        
        for (OutboxEvent event : pendingEvents) {
            try {
                kafkaTemplate.send(TOPIC, event.getAggregateId(), event.getPayload()).get();
                repository.updateOutboxStatus(event.getId(), "PROCESSED");
                log.info("[OUTBOX_SUCCESS] Routed event {} to Kafka topic {}", event.getId(), TOPIC);
            } catch (Exception e) {
                log.error("[OUTBOX_RETRY] Failed to dispatch outbox event {}. Retrying next loop.", event.getId(), e);
                repository.updateOutboxStatus(event.getId(), "FAILED");
            }
        }
    }
}
