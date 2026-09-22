package com.apex.orchestrator.saga;

import com.apex.events.PaymentReserved;
import com.apex.events.ShipmentFailed;
import com.apex.orchestrator.entity.OutboxEvent;
import com.apex.orchestrator.entity.Transaction;
import com.apex.orchestrator.entity.TransactionState;
import com.apex.orchestrator.repository.OutboxEventRepository;
import com.apex.orchestrator.repository.TransactionRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.transaction.Transactional;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;

@Component
@RequiredArgsConstructor
public class SagaOrchestrator {
    private final TransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public record PaymentReservedEvent(Long transactionId) {}
    public record ShipmentFailedEvent(Long transactionId) {}

    @EventListener
    @Transactional
    public void onPaymentReserved(PaymentReservedEvent event) throws JsonProcessingException {
        Transaction tx = transactionRepository.findById(event.transactionId()).orElseThrow();
        tx.setState(TransactionState.PAYMENT_RESERVED);
        transactionRepository.save(tx);

        OutboxEvent outbox = new OutboxEvent();
        outbox.setAggregateId(tx.getId().toString());
        outbox.setAggregateType("Transaction");
        outbox.setEventType("ReservedShipmentCommand");

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("transactionId", tx.getId());
        outbox.setPayload(objectMapper.writeValueAsString(payload));

        outboxEventRepository.save(outbox);
    }

    @EventListener
    @Transactional
    public void onShipmnetFailed(ShipmentFailedEvent event) throws JsonProcessingException {
        Transaction tx = transactionRepository.findById(event.transactionId()).orElseThrow();
        tx.setState(TransactionState.COMPENSATING);
        transactionRepository.save(tx);

        OutboxEvent outbox = new OutboxEvent();
        outbox.setAggregateId(tx.getId().toString());
        outbox.setAggregateType("Transaction");
        outbox.setEventType("CancelPaymentCommand");

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("transactionId", tx.getId());
        outbox.setPayload(objectMapper.writeValueAsString(payload));

        outboxEventRepository.save(outbox);
    }
}
