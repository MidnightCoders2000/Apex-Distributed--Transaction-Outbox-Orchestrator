package com.apex.orchestrator.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;

public record TransactionRequest(
        @NotNull(message = "Account ID is required")
        @Min(value = 1, message = "Account ID must be grater than 0")
        Long accountId,

        @NotNull(message = "Amount is required")
        @Min(value = 0, message = "Amount cannot be negative")
        BigDecimal amount
){}



