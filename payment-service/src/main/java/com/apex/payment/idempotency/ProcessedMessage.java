package com.apex.payment.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payment_processed_message")
public class ProcessedMessage {

    @Id
    @Column(name = "message_id")
    private UUID messageId;

    @Column(name = "processed_at", insertable = false, updatable = false)
    private Instant processedAt;

    protected ProcessedMessage() {
    }

    public ProcessedMessage(UUID messageId) {
        this.messageId = messageId;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
