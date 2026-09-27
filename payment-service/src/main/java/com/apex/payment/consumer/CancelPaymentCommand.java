package com.apex.payment.consumer;

/**
 * Payload shape currently emitted by orchestrator-service's SagaOrchestrator
 * for the "CancelPaymentCommand" outbox event type ({"transactionId": ...}).
 * Not part of common-events since Track A publishes it ad hoc rather than via
 * the shared module; kept local until the two tracks formalize it.
 */
public record CancelPaymentCommand(String transactionId) {
}
