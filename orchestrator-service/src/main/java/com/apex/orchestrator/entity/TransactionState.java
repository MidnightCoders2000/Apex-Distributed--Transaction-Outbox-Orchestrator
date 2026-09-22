package com.apex.orchestrator.entity;

public enum TransactionState {
    STARTED,
    PAYMENT_RESERVED,
    SHIPMENT_RESERVED,
    COMPLETED,
    COMPENSATING,
    REVERSED
}
