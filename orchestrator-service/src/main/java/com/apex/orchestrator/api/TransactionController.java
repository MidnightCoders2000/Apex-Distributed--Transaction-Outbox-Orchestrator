package com.apex.orchestrator.api;

import com.apex.orchestrator.dto.TransactionRequest;
import com.apex.orchestrator.service.TransactionService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import lombok.RequiredArgsConstructor;


@RestController
@RequestMapping("/api/transactions")
@RequiredArgsConstructor

public class TransactionController {
    private final TransactionService transactionService;

    @PostMapping
    public ResponseEntity<Void> createTransaction(
            @RequestHeader("Idempotency-Key") String idempotencyKey,
            @Valid @RequestBody TransactionRequest request){
        transactionService.processTransaction(idempotencyKey,request);
        return ResponseEntity.accepted().build();
    }
}



