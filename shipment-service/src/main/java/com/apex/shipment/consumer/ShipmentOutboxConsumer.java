package com.apex.shipment.consumer;

import com.apex.events.ShipmentFailed;
import com.apex.events.ShipmentRequested;
import com.apex.events.ShipmentReserved;
import com.apex.shipment.idempotency.ProcessedMessage;
import com.apex.shipment.idempotency.ShipmentProcessedMessageRepository;
import com.apex.shipment.injection.ShipmentFailureInjectionPolicy;
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

@Component
public class ShipmentOutboxConsumer {

    private static final Logger log = LoggerFactory.getLogger(ShipmentOutboxConsumer.class);
    private static final String EVENTS_TOPIC = "apex.shipment.events";
    private static final long PUBLISH_TIMEOUT_SECONDS = 10L;

    private final OutboxEventEnvelopeParser envelopeParser;
    private final ShipmentRequestedPayloadParser payloadParser;
    private final ShipmentProcessedMessageRepository processedRepo;
    private final ShipmentFailureInjectionPolicy failureInjectionPolicy;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String eventType;

    public ShipmentOutboxConsumer(OutboxEventEnvelopeParser envelopeParser,
                                   ShipmentRequestedPayloadParser payloadParser,
                                   ShipmentProcessedMessageRepository processedRepo,
                                   ShipmentFailureInjectionPolicy failureInjectionPolicy,
                                   KafkaTemplate<String, Object> kafkaTemplate,
                                   @Value("${apex.consumer.event-type}") String eventType) {
        this.envelopeParser = envelopeParser;
        this.payloadParser = payloadParser;
        this.processedRepo = processedRepo;
        this.failureInjectionPolicy = failureInjectionPolicy;
        this.kafkaTemplate = kafkaTemplate;
        this.eventType = eventType;
    }

    /**
     * Publish-before-mark (see PLAN.md decision #6): a crash or publish
     * failure between the two risks a duplicate publish, not a lost event —
     * the deliberate tradeoff for a saga where a dropped terminal event
     * hangs forever. This only holds if a failed publish is actually
     * observed, which is why the send below is joined synchronously instead
     * of fired-and-forgotten. Only the DataIntegrityViolationException catch
     * below is intentional; everything else propagates to the container's
     * DefaultErrorHandler (see KafkaConsumerConfig), which is the only
     * retry/DLT policy for this listener.
     */
    @KafkaListener(topics = "${apex.consumer.topic}", groupId = "apex-shipment-service")
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

        ShipmentRequested requested = payloadParser.parse(envelope);
        boolean fail = failureInjectionPolicy.shouldFail(requested.accountId());

        Object event = fail
                ? new ShipmentFailed(requested.correlationId(), requested.transactionId(), "injected-failure")
                : new ShipmentReserved(requested.correlationId(), requested.transactionId());
        publish(event, requested.transactionId());
        log.info("Published {} for transaction {}", event.getClass().getSimpleName(), requested.transactionId());

        try {
            processedRepo.save(new ProcessedMessage(messageId));
        } catch (DataIntegrityViolationException e) {
            log.debug("Message {} marked processed concurrently or on redelivery, ignoring", messageId);
        }
    }

    private void publish(Object event, String transactionId) {
        try {
            kafkaTemplate.send(EVENTS_TOPIC, transactionId, event).get(PUBLISH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
