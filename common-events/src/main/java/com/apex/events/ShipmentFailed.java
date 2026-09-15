package com.apex.events;

public record ShipmentFailed(String correlationId, String transactionId, String reason) {
}
