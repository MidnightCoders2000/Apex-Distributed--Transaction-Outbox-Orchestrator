package com.apex.payment.idempotency;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface PaymentProcessedMessageRepository extends JpaRepository<ProcessedMessage, UUID> {
}
