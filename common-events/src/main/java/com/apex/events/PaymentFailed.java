package com.apex.events;

import com.fasterxml.jackson.annotation.JsonGetter;

public record PaymentFailed(String correlationId, String transactionId, String reason) {
    @JsonGetter("eventType")
    public String eventType() {
        return "PaymentFailed";
    }
}
