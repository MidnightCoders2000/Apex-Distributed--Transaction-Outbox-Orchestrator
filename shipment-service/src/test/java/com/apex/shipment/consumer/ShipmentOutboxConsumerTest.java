package com.apex.shipment.consumer;

import com.apex.events.ShipmentFailed;
import com.apex.events.ShipmentReserved;
import com.apex.shipment.idempotency.ProcessedMessage;
import com.apex.shipment.idempotency.ShipmentProcessedMessageRepository;
import com.apex.shipment.injection.ShipmentFailureInjectionPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ShipmentOutboxConsumerTest {

    private static final UUID MESSAGE_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OutboxEventEnvelopeParser envelopeParser = new OutboxEventEnvelopeParser(objectMapper);
    private final ShipmentRequestedPayloadParser payloadParser = new ShipmentRequestedPayloadParser(objectMapper);

    private ShipmentProcessedMessageRepository processedRepo;
    private ShipmentFailureInjectionPolicy failureInjectionPolicy;
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
    private ShipmentOutboxConsumer consumer;

    @BeforeEach
    void setUp() {
        processedRepo = mock(ShipmentProcessedMessageRepository.class);
        failureInjectionPolicy = mock(ShipmentFailureInjectionPolicy.class);
        consumer = new ShipmentOutboxConsumer(envelopeParser, payloadParser, processedRepo,
                failureInjectionPolicy, kafkaTemplate, "ShipmentRequested");
    }

    private ConsumerRecord<String, String> recordFor(String eventType, String payloadJson) throws Exception {
        String payloadEscaped = objectMapper.writeValueAsString(payloadJson);
        String value = "{\"after\":{\"id\":\"" + MESSAGE_ID + "\",\"event_type\":\"" + eventType
                + "\",\"payload\":" + payloadEscaped + "},\"op\":\"c\"}";
        return new ConsumerRecord<>("apex.public.outbox_event", 0, 0, "key", value);
    }

    @Test
    void irrelevantEventTypeCausesNoInteraction() throws Exception {
        ConsumerRecord<String, String> record = recordFor("PaymentRequested", "{}");

        consumer.onMessage(record);

        verifyNoInteractions(processedRepo, kafkaTemplate, failureInjectionPolicy);
    }

    @Test
    void knownProcessedMessageIsSkipped() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(true);
        ConsumerRecord<String, String> record = recordFor("ShipmentRequested",
                "{\"correlationId\":\"c1\",\"transactionId\":\"t1\",\"accountId\":\"a1\"}");

        consumer.onMessage(record);

        verify(processedRepo, never()).save(any());
        verifyNoInteractions(kafkaTemplate, failureInjectionPolicy);
    }

    @Test
    void newMessageWithSuccessPublishesReservedThenSaves() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(false);
        when(failureInjectionPolicy.shouldFail("a1")).thenReturn(false);
        ConsumerRecord<String, String> record = recordFor("ShipmentRequested",
                "{\"correlationId\":\"c1\",\"transactionId\":\"t1\",\"accountId\":\"a1\"}");

        consumer.onMessage(record);

        verify(kafkaTemplate).send(eq("apex.shipment.events"), eq("t1"), any(ShipmentReserved.class));
        verify(processedRepo).save(any(ProcessedMessage.class));
    }

    @Test
    void newMessageWithFailurePublishesFailedThenSaves() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(false);
        when(failureInjectionPolicy.shouldFail("a1")).thenReturn(true);
        ConsumerRecord<String, String> record = recordFor("ShipmentRequested",
                "{\"correlationId\":\"c1\",\"transactionId\":\"t1\",\"accountId\":\"a1\"}");

        consumer.onMessage(record);

        verify(kafkaTemplate).send(eq("apex.shipment.events"), eq("t1"), any(ShipmentFailed.class));
        verify(processedRepo).save(any(ProcessedMessage.class));
    }

    @Test
    void duplicateMarkExceptionIsSwallowedNotPropagated() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(false);
        when(failureInjectionPolicy.shouldFail("a1")).thenReturn(false);
        doThrow(new DataIntegrityViolationException("dup")).when(processedRepo).save(any());
        ConsumerRecord<String, String> record = recordFor("ShipmentRequested",
                "{\"correlationId\":\"c1\",\"transactionId\":\"t1\",\"accountId\":\"a1\"}");

        consumer.onMessage(record);

        verify(kafkaTemplate).send(eq("apex.shipment.events"), eq("t1"), any(ShipmentReserved.class));
    }
}
