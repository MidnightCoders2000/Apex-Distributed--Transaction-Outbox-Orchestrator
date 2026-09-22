package com.apex.orchestrator.entity;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;
import java.time.Instant;


@Getter
@Setter
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {
    @Id @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    private String aggregateType;
    private String aggregateId;
    private String eventType;
    private String payload;
    private String correlationId;

    @Column(name = "created_at", insertable = false, updatable = false)
    private Instant createdAt;

    public OutboxEvent(){
    }

    private OutboxEvent(String aggregateType, String aggregateId, String eventType, String payload){
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.eventType = eventType;
        this.payload = payload;
    }
}
