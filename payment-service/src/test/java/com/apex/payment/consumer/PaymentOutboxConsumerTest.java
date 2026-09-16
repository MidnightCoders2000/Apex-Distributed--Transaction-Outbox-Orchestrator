package com.apex.payment.consumer;

import com.apex.events.PaymentFailed;
import com.apex.events.PaymentReserved;
import com.apex.messaging.OutboxEventEnvelopeParser;
import com.apex.messaging.PayloadDeserializationException;
import com.apex.payment.idempotency.PaymentProcessedMessageRepository;
import com.apex.payment.idempotency.ProcessedMessage;
import com.apex.payment.injection.PaymentFailureInjectionPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class PaymentOutboxConsumerTest {

    private static final UUID MESSAGE_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OutboxEventEnvelopeParser envelopeParser = new OutboxEventEnvelopeParser(objectMapper);
    private final PaymentRequestedPayloadParser payloadParser = new PaymentRequestedPayloadParser(objectMapper);

    private PaymentProcessedMessageRepository processedRepo;
    private PaymentFailureInjectionPolicy failureInjectionPolicy;
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
    private PaymentOutboxConsumer consumer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        processedRepo = mock(PaymentProcessedMessageRepository.class);
        failureInjectionPolicy = mock(PaymentFailureInjectionPolicy.class);
        SendResult<String, Object> sendResult = mock(SendResult.class);
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(sendResult));
        consumer = new PaymentOutboxConsumer(envelopeParser, payloadParser, processedRepo,
                failureInjectionPolicy, kafkaTemplate, "PaymentRequested");
    }

    private ConsumerRecord<String, String> recordFor(String eventType, String payloadJson) throws Exception {
        String payloadEscaped = objectMapper.writeValueAsString(payloadJson);
        String value = "{\"after\":{\"id\":\"" + MESSAGE_ID + "\",\"event_type\":\"" + eventType
                + "\",\"payload\":" + payloadEscaped + "},\"op\":\"c\"}";
        return new ConsumerRecord<>("apex.public.outbox_event", 0, 0, "key", value);
    }

    @Test
    void irrelevantEventTypeCausesNoInteraction() throws Exception {
        ConsumerRecord<String, String> record = recordFor("ShipmentRequested", "{}");

        consumer.onMessage(record);

        verifyNoInteractions(processedRepo, kafkaTemplate, failureInjectionPolicy);
    }

    @Test
    void knownProcessedMessageIsSkipped() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(true);
        ConsumerRecord<String, String> record = recordFor("PaymentRequested",
                "{\"correlationId\":\"c1\",\"transactionId\":\"t1\",\"accountId\":\"a1\",\"amount\":1}");

        consumer.onMessage(record);

        verify(processedRepo, never()).save(any());
        verifyNoInteractions(kafkaTemplate, failureInjectionPolicy);
    }

    @Test
    void newMessageWithSuccessPublishesReservedThenSaves() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(false);
        when(failureInjectionPolicy.shouldFail("a1")).thenReturn(false);
        ConsumerRecord<String, String> record = recordFor("PaymentRequested",
                "{\"correlationId\":\"c1\",\"transactionId\":\"t1\",\"accountId\":\"a1\",\"amount\":1}");

        consumer.onMessage(record);

        verify(kafkaTemplate).send(eq("apex.payment.events"), eq("t1"), any(PaymentReserved.class));
        verify(processedRepo).save(any(ProcessedMessage.class));
    }

    @Test
    void newMessageWithFailurePublishesFailedThenSaves() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(false);
        when(failureInjectionPolicy.shouldFail("a1")).thenReturn(true);
        ConsumerRecord<String, String> record = recordFor("PaymentRequested",
                "{\"correlationId\":\"c1\",\"transactionId\":\"t1\",\"accountId\":\"a1\",\"amount\":1}");

        consumer.onMessage(record);

        verify(kafkaTemplate).send(eq("apex.payment.events"), eq("t1"), any(PaymentFailed.class));
        verify(processedRepo).save(any(ProcessedMessage.class));
    }

    @Test
    void duplicateMarkExceptionIsSwallowedNotPropagated() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(false);
        when(failureInjectionPolicy.shouldFail("a1")).thenReturn(false);
        doThrow(new DataIntegrityViolationException("dup")).when(processedRepo).save(any());
        ConsumerRecord<String, String> record = recordFor("PaymentRequested",
                "{\"correlationId\":\"c1\",\"transactionId\":\"t1\",\"accountId\":\"a1\",\"amount\":1}");

        consumer.onMessage(record);

        verify(kafkaTemplate).send(eq("apex.payment.events"), eq("t1"), any(PaymentReserved.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void failedPublishPropagatesAndDoesNotMarkProcessed() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(false);
        when(failureInjectionPolicy.shouldFail("a1")).thenReturn(false);
        CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("broker unavailable"));
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(failedFuture);
        ConsumerRecord<String, String> record = recordFor("PaymentRequested",
                "{\"correlationId\":\"c1\",\"transactionId\":\"t1\",\"accountId\":\"a1\",\"amount\":1}");

        assertThatThrownBy(() -> consumer.onMessage(record)).isInstanceOf(IllegalStateException.class);

        verify(processedRepo, never()).save(any());
    }

    @Test
    void missingIdFieldThrowsInsteadOfNpe() {
        String value = "{\"after\":{\"event_type\":\"PaymentRequested\",\"payload\":\"{}\"},\"op\":\"c\"}";
        ConsumerRecord<String, String> record = new ConsumerRecord<>("apex.public.outbox_event", 0, 0, "key", value);

        assertThatThrownBy(() -> consumer.onMessage(record)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void missingPayloadFieldThrowsInsteadOfNpe() {
        String value = "{\"after\":{\"id\":\"" + MESSAGE_ID + "\",\"event_type\":\"PaymentRequested\"},\"op\":\"c\"}";
        ConsumerRecord<String, String> record = new ConsumerRecord<>("apex.public.outbox_event", 0, 0, "key", value);

        assertThatThrownBy(() -> consumer.onMessage(record)).isInstanceOf(PayloadDeserializationException.class);
    }
}
