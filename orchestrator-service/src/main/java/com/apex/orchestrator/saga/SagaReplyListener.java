package com.apex.orchestrator.saga;

import lombok.extern.slf4j.Slf4j;
import lombok.RequiredArgsConstructor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@RequiredArgsConstructor
public class SagaReplyListener {

    private final ObjectMapper objectMapper;
    private final ApplicationEventPublisher eventPublisher;

    @KafkaListener(topics = {"apex.payment.events","apex.shipment.events"}, groupId = "saga-orchestrator-group")
    public void consumeReply(String message) {
        try{
            JsonNode root = objectMapper.readTree(message);
            String eventType = root.get("eventType").asText();

            JsonNode payload = root.get("payload");
            if(payload == null || !payload.has("transactionId")) return;

            Long transactionId = payload.get("transactionId").asLong();
            log.info("Received Kafka event: {} for transaction: {}",eventType,transactionId);

            switch(eventType){
                case "PaymentReserved":
                    eventPublisher.publishEvent(new SagaOrchestrator.PaymentReservedEvent(transactionId));
                    break;
                case "PaymentFailed":
                    eventPublisher.publishEvent(new SagaOrchestrator.PaymentFailedEvent(transactionId));
                    break;
                case "ShipmentFailed":
                    eventPublisher.publishEvent(new SagaOrchestrator.ShipmentFailedEvent(transactionId));
                    break;
                case "ShipmentReserved":
                    eventPublisher.publishEvent(new SagaOrchestrator.ShipmentReservedEvent(transactionId));
                    break;
                default:
                    log.debug("Ignored event type: {}",eventType);
            }
        }catch(Exception e){
            log.error("Failed to process Kafka message: {}",message, e);
        }
    }
}
