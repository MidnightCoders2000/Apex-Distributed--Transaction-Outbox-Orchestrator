package com.apex.events;

import java.math.BigDecimal;

public record PaymentRequested(String correlationId, String transactionId, String accountId, BigDecimal amount) {
}
