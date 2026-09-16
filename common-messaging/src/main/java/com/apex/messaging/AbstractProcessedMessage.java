package com.apex.messaging;

import jakarta.persistence.Column;
import jakarta.persistence.Id;
import jakarta.persistence.MappedSuperclass;
import jakarta.persistence.PostLoad;
import jakarta.persistence.Transient;
import org.springframework.data.domain.Persistable;

import java.time.Instant;
import java.util.UUID;

/**
 * Implements {@link Persistable} with an eager {@code isNew}, flipped to
 * false only on {@link PostLoad}, so {@code save()} always goes through
 * {@code EntityManager.persist()} for a freshly-constructed instance instead
 * of {@code merge()}. Without this, the assigned {@code @Id} makes Spring
 * Data JPA assume every instance already exists, merge() silently upserts
 * duplicates, and the DataIntegrityViolationException callers catch for
 * redelivery can never actually be thrown.
 *
 * Subclasses add {@code @Entity} + {@code @Table(name = ...)}: each
 * consumer service owns its own table (payment_processed_message,
 * shipment_processed_message, ...), only the mapping/idempotency behavior
 * is shared.
 */
@MappedSuperclass
public abstract class AbstractProcessedMessage implements Persistable<UUID> {

    @Id
    @Column(name = "message_id")
    private UUID messageId;

    @Column(name = "processed_at", insertable = false, updatable = false)
    private Instant processedAt;

    @Transient
    private boolean isNew = true;

    protected AbstractProcessedMessage() {
    }

    protected AbstractProcessedMessage(UUID messageId) {
        this.messageId = messageId;
    }

    @Override
    public UUID getId() {
        return messageId;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    @PostLoad
    void markNotNew() {
        isNew = false;
    }
}
