package com.apex.payment.consumer;

import com.apex.messaging.OutboxEnvelope;
import com.apex.messaging.PayloadDeserializationException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class CancelPaymentCommandPayloadParser {

    private final ObjectMapper objectMapper;

    public CancelPaymentCommandPayloadParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public CancelPaymentCommand parse(OutboxEnvelope envelope) {
        JsonNode payloadNode = envelope.after().get("payload");
        if (payloadNode == null || payloadNode.isNull()) {
            throw new PayloadDeserializationException("Outbox record is missing required field 'payload'", null);
        }
        String payloadJson = payloadNode.asText();
        try {
            return objectMapper.readValue(payloadJson, CancelPaymentCommand.class);
        } catch (Exception e) {
            throw new PayloadDeserializationException("Failed to deserialize CancelPaymentCommand payload", e);
        }
    }
}
