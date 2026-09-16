package com.apex.shipment.consumer;

import com.apex.events.ShipmentRequested;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

@Component
public class ShipmentRequestedPayloadParser {

    private final ObjectMapper objectMapper;

    public ShipmentRequestedPayloadParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public ShipmentRequested parse(OutboxEnvelope envelope) {
        String payloadJson = envelope.after().get("payload").asText();
        try {
            return objectMapper.readValue(payloadJson, ShipmentRequested.class);
        } catch (Exception e) {
            throw new PayloadDeserializationException("Failed to deserialize ShipmentRequested payload", e);
        }
    }
}
