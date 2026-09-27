package com.apex.events;

import com.fasterxml.jackson.annotation.JsonGetter;

public record PaymentReserved(String correlationId, String transactionId) {
    @JsonGetter("eventType")
    public String eventType() {
        return "PaymentReserved";
    }
}
