package com.apex.shipment.idempotency;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface ShipmentProcessedMessageRepository extends JpaRepository<ProcessedMessage, UUID> {
}
