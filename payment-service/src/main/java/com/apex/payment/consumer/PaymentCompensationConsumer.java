package com.apex.payment.consumer;

import com.apex.events.PaymentCompensated;
import com.apex.messaging.OutboxEnvelope;
import com.apex.messaging.OutboxEventEnvelopeParser;
import com.apex.payment.idempotency.PaymentProcessedMessageRepository;
import com.apex.payment.idempotency.ProcessedMessage;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Consumes the "CancelPaymentCommand" outbox event that SagaOrchestrator
 * emits on rollback (see orchestrator-service's SagaOrchestrator#onShipmentFailed)
 * and publishes PaymentCompensated back on apex.payment.events so the saga can
 * transition out of COMPENSATING.
 *
 * Runs in its own consumer group (distinct from PaymentOutboxConsumer's) so it
 * gets a full copy of apex.public.outbox_event instead of splitting partitions
 * with the PaymentRequested consumer — same pattern already used to let
 * payment-service and shipment-service both read the same topic independently.
 *
 * correlationId is currently null: the orchestrator's CancelPaymentCommand
 * payload only carries transactionId, not a correlationId (gap tracked in
 * docs/integration-status-epic-a.md — needs a Track A fix, not a Track B one).
 */
@Component
public class PaymentCompensationConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentCompensationConsumer.class);
    private static final long PUBLISH_TIMEOUT_SECONDS = 10L;

    private final OutboxEventEnvelopeParser envelopeParser;
    private final CancelPaymentCommandPayloadParser payloadParser;
    private final PaymentProcessedMessageRepository processedRepo;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String eventType;
    private final String eventsTopic;

    public PaymentCompensationConsumer(OutboxEventEnvelopeParser envelopeParser,
                                        CancelPaymentCommandPayloadParser payloadParser,
                                        PaymentProcessedMessageRepository processedRepo,
                                        KafkaTemplate<String, Object> kafkaTemplate,
                                        @Value("${apex.consumer.compensation-event-type}") String eventType,
                                        @Value("${apex.consumer.events-topic}") String eventsTopic) {
        this.envelopeParser = envelopeParser;
        this.payloadParser = payloadParser;
        this.processedRepo = processedRepo;
        this.kafkaTemplate = kafkaTemplate;
        this.eventType = eventType;
        this.eventsTopic = eventsTopic;
    }

    @KafkaListener(topics = "${apex.consumer.topic}", groupId = "apex-payment-service-compensation")
    public void onMessage(ConsumerRecord<String, String> record) {
        Optional<OutboxEnvelope> envelopeOpt = envelopeParser.parse(record.value());
        if (envelopeOpt.isEmpty()) {
            log.warn("Skipping unparseable outbox record");
            return;
        }
        OutboxEnvelope envelope = envelopeOpt.get();
        if (!envelopeParser.isRelevant(envelope, eventType)) {
            return;
        }

        JsonNode idNode = envelope.after().get("id");
        if (idNode == null || idNode.isNull()) {
            throw new IllegalStateException("Outbox record for event type " + eventType + " is missing required field 'id'");
        }
        UUID messageId = UUID.fromString(idNode.asText());
        if (processedRepo.existsById(messageId)) {
            log.debug("Message {} already processed, skipping (best-effort check)", messageId);
            return;
        }

        CancelPaymentCommand command = payloadParser.parse(envelope);
        PaymentCompensated event = new PaymentCompensated(null, command.transactionId());
        publish(event, command.transactionId());
        log.info("Published {} for transaction {}", event.getClass().getSimpleName(), command.transactionId());

        try {
            processedRepo.save(new ProcessedMessage(messageId));
        } catch (DataIntegrityViolationException e) {
            log.debug("Message {} marked processed concurrently or on redelivery, ignoring", messageId);
        }
    }

    private void publish(Object event, String transactionId) {
        try {
            kafkaTemplate.send(eventsTopic, transactionId, event).get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing " + event.getClass().getSimpleName()
                    + " for transaction " + transactionId, e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to publish " + event.getClass().getSimpleName()
                    + " for transaction " + transactionId, e);
        }
    }
}
