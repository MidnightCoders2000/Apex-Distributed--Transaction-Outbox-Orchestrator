package com.apex.messaging;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class OutboxEventEnvelopeParser {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventEnvelopeParser.class);

    private final ObjectMapper objectMapper;

    public OutboxEventEnvelopeParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Optional<OutboxEnvelope> parse(String rawJson) {
        try {
            return Optional.of(objectMapper.readValue(rawJson, OutboxEnvelope.class));
        } catch (Exception e) {
            log.warn("Failed to parse outbox envelope, skipping message: {}", e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Null-safe on {@code op}: with auto-offset-reset=earliest and a fresh
     * consumer group, the first messages read back are old schema-wrapped
     * test records predating the envelope fix, where op parses as null
     * under this shape.
     *
     * {@code "r"} (connector snapshot/re-snapshot read) is accepted on the
     * same footing as {@code "c"} (insert): intentional, so a re-snapshot
     * replays historical *Requested rows through the same idempotency
     * check rather than silently skipping them. This only holds while the
     * idempotency table's rows survive — the bootstrap DDL for those
     * tables is now a tracked Flyway migration (see each service's
     * src/main/resources/db/migration), so a dropped/recreated table would
     * still let a re-snapshot double-publish, just no longer via manual
     * DDL drift.
     */
    public boolean isRelevant(OutboxEnvelope envelope, String eventType) {
        if (envelope.after() == null) {
            return false;
        }
        if (!"c".equals(envelope.op()) && !"r".equals(envelope.op())) {
            return false;
        }
        JsonNode eventTypeNode = envelope.after().get("event_type");
        return eventTypeNode != null && eventTypeNode.asText().equals(eventType);
    }
}
