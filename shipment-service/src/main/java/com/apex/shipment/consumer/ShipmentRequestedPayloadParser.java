package com.apex.shipment.consumer;

import com.apex.events.ShipmentRequested;
import com.apex.messaging.OutboxEnvelope;
import com.apex.messaging.PayloadDeserializationException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class ShipmentRequestedPayloadParser {

    private final ObjectMapper objectMapper;

    public ShipmentRequestedPayloadParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public ShipmentRequested parse(OutboxEnvelope envelope) {
        JsonNode payloadNode = envelope.after().get("payload");
        if (payloadNode == null || payloadNode.isNull()) {
            throw new PayloadDeserializationException("Outbox record is missing required field 'payload'", null);
        }
        String payloadJson = payloadNode.asText();
        try {
            return objectMapper.readValue(payloadJson, ShipmentRequested.class);
        } catch (Exception e) {
            throw new PayloadDeserializationException("Failed to deserialize ShipmentRequested payload", e);
        }
    }
}
