package com.apex.events;

public record ShipmentRequested(String correlationId, String transactionId, String accountId) {
}
