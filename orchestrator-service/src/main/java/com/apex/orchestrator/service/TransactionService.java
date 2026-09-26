package com.apex.orchestrator.service;

import com.apex.orchestrator.dto.TransactionRequest;
import com.apex.orchestrator.entity.OutboxEvent;
import com.apex.orchestrator.entity.Transaction;
import com.apex.orchestrator.entity.TransactionState;
import com.apex.orchestrator.exception.DuplicateRequestException;
import com.apex.orchestrator.repository.OutboxEventRepository;
import com.apex.orchestrator.repository.TransactionRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
public class TransactionService {

    private final StringRedisTemplate redisTemplate;
    private final TransactionRepository transactionRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void processTransaction(String idempotencyKey, TransactionRequest request){
        Boolean isNew = redisTemplate.opsForValue()
                .setIfAbsent("idempotency:" + idempotencyKey, "Processing",Duration.ofMinutes(10));

        if(Boolean.FALSE.equals(isNew)) {
            throw new DuplicateRequestException("Transaction with this key is already processing.");
        }

        Transaction tx = new Transaction();
        tx.setAccountId(request.accountId());
        tx.setAmount(request.amount());
        tx.setState(TransactionState.STARTED);
        tx = transactionRepository.save(tx);

        try{

            OutboxEvent event = new OutboxEvent();
            event.setAggregateId(tx.getId().toString());
            event.setAggregateType("Transaction");
            event.setEventType("TransactionStartedEvent");

            ObjectNode payloadNode = objectMapper.createObjectNode();
            payloadNode.put("transactionId",tx.getId());
            payloadNode.put("accountId",tx.getAccountId());
            payloadNode.put("amount",tx.getAmount());

            event.setPayload(objectMapper.writeValueAsString(payloadNode));
            outboxEventRepository.save(event);

            OutboxEvent paymentCommand = new OutboxEvent();
            paymentCommand.setAggregateId(tx.getId().toString());
            paymentCommand.setAggregateType("payment");
            paymentCommand.setEventType("PaymentRequested");

            paymentCommand.setPayload(objectMapper.writeValueAsString(payloadNode));
            outboxEventRepository.save(paymentCommand);
        }catch (JsonProcessingException e){
            throw new RuntimeException("Failed to serialize outbox payload", e);
        }
    }
}
