package com.apex.orchestrator.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;


import java.math.BigDecimal;

@Setter
@Getter
@Entity
@Table(name = "transactions")

public class Transaction {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Long accountId;
    private BigDecimal amount;

    public Transaction(){ //hibernate
    }

    public Transaction(Long accountId, BigDecimal amount){ //for a new save
        this.accountId = accountId;
        this.amount = amount;
    }

    public Transaction(Long id, Long accountId, BigDecimal amount){
        this.id = id;
        this.accountId = accountId;
        this.amount = amount;
    }

    @Enumerated(EnumType.STRING)
    private TransactionState state;
}
