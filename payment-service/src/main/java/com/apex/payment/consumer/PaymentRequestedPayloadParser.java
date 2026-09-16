package com.apex.payment.consumer;

import com.apex.events.PaymentRequested;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class PaymentRequestedPayloadParser {

    private final ObjectMapper objectMapper;

    public PaymentRequestedPayloadParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public PaymentRequested parse(OutboxEnvelope envelope) {
        String payloadJson = envelope.after().get("payload").asText();
        try {
            return objectMapper.readValue(payloadJson, PaymentRequested.class);
        } catch (Exception e) {
            throw new PayloadDeserializationException("Failed to deserialize PaymentRequested payload", e);
        }
    }
}
