package com.apex.events;

public record PaymentCompensated(String correlationId, String transactionId) {
}
