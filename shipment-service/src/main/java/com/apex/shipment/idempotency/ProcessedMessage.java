package com.apex.shipment.idempotency;

import com.apex.messaging.AbstractProcessedMessage;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;

import java.util.UUID;

@Entity
@Table(name = "shipment_processed_message")
public class ProcessedMessage extends AbstractProcessedMessage {

    protected ProcessedMessage() {
    }

    public ProcessedMessage(UUID messageId) {
        super(messageId);
    }
}
