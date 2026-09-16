package com.apex.payment.idempotency;

import com.apex.messaging.AbstractProcessedMessage;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.util.UUID;

@Entity
@Table(name = "payment_processed_message")
public class ProcessedMessage extends AbstractProcessedMessage {

    protected ProcessedMessage() {
    }

    public ProcessedMessage(UUID messageId) {
        super(messageId);
    }
}
