package com.wfm.poc.repository;

import com.wfm.poc.domain.OutboxEvent;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@Repository
public class WfmRepository {
    private final JdbcTemplate jdbcTemplate;

    public WfmRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void initTables() {
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS shift_assignments (
                id SERIAL PRIMARY KEY,
                employee_id VARCHAR(50) NOT NULL,
                shift_date DATE NOT NULL,
                role VARCHAR(100) NOT NULL,
                created_at TIMESTAMP NOT NULL
            );
        """);
        jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS outbox_events (
                id UUID PRIMARY KEY,
                aggregate_type VARCHAR(100) NOT NULL,
                aggregate_id VARCHAR(100) NOT NULL,
                event_type VARCHAR(100) NOT NULL,
                payload TEXT NOT NULL,
                status VARCHAR(20) NOT NULL,
                created_at TIMESTAMP NOT NULL
            );
        """);
    }

    public void saveShiftAndOutbox(String empId, LocalDate date, String role, OutboxEvent event) {
        jdbcTemplate.update(
            "INSERT INTO shift_assignments (employee_id, shift_date, role, created_at) VALUES (?, ?, ?, NOW())",
            empId, date, role
        );
        jdbcTemplate.update(
            "INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            event.getId(), event.getAggregateType(), event.getAggregateId(), event.getEventType(), event.getPayload(), event.getStatus(), Timestamp.valueOf(event.getCreatedAt())
        );
    }

    public List<OutboxEvent> fetchPendingEvents() {
        return jdbcTemplate.query("SELECT * FROM outbox_events WHERE status = 'PENDING' FOR UPDATE SKIP LOCKED", (rs, rowNum) -> {
            OutboxEvent event = new OutboxEvent();
            event.setId(UUID.fromString(rs.getString("id")));
            event.setAggregateType(rs.getString("aggregate_type"));
            event.setAggregateId(rs.getString("aggregate_id"));
            event.setEventType(rs.getString("event_type"));
            event.setPayload(rs.getString("payload"));
            event.setStatus(rs.getString("status"));
            return event;
        });
    }

    public void updateOutboxStatus(UUID id, String status) {
        jdbcTemplate.update("UPDATE outbox_events SET status = ? WHERE id = ?", status, id);
    }
}
