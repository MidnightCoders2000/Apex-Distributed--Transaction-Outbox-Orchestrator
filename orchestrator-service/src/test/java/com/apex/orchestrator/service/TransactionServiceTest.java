package com.apex.orchestrator.service;

import com.apex.orchestrator.dto.TransactionRequest;
import com.apex.orchestrator.entity.Transaction;
import com.apex.orchestrator.repository.OutboxEventRepository;
import com.apex.orchestrator.repository.TransactionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;


import java.math.BigDecimal;

@SpringBootTest
public class TransactionServiceTest {
    @Autowired
    private TransactionService transactionService;

    @Autowired
    private TransactionRepository transactionRepository;

    @MockBean
    private OutboxEventRepository outboxEventRepository;

    @Test
    void processTransaction_shouldRollback_whenOutboxFails(){
        when(outboxEventRepository.save(any())).thenThrow(new RuntimeException("DB Connection lost"));

        TransactionRequest request = new TransactionRequest(123L, BigDecimal.valueOf(50.00));

        assertThrows(RuntimeException.class, () ->
                transactionService.processTransaction("test-uuid-key-1",request)
        );

        assertEquals(0,transactionRepository.count());

    }
}
