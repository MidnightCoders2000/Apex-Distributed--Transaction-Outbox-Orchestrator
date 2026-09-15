package com.apex.events;

public record PaymentFailed(String correlationId, String transactionId, String reason) {
}
