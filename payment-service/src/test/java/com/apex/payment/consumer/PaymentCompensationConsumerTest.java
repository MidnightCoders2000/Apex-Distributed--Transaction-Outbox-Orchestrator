package com.apex.payment.consumer;

import com.apex.events.PaymentCompensated;
import com.apex.messaging.OutboxEventEnvelopeParser;
import com.apex.messaging.PayloadDeserializationException;
import com.apex.payment.idempotency.PaymentProcessedMessageRepository;
import com.apex.payment.idempotency.ProcessedMessage;
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

class PaymentCompensationConsumerTest {

    private static final UUID MESSAGE_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OutboxEventEnvelopeParser envelopeParser = new OutboxEventEnvelopeParser(objectMapper);
    private final CancelPaymentCommandPayloadParser payloadParser = new CancelPaymentCommandPayloadParser(objectMapper);

    private PaymentProcessedMessageRepository processedRepo;
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
    private PaymentCompensationConsumer consumer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        processedRepo = mock(PaymentProcessedMessageRepository.class);
        SendResult<String, Object> sendResult = mock(SendResult.class);
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(CompletableFuture.completedFuture(sendResult));
        consumer = new PaymentCompensationConsumer(envelopeParser, payloadParser, processedRepo,
                kafkaTemplate, "CancelPaymentCommand", "apex.payment.events");
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

        verifyNoInteractions(processedRepo, kafkaTemplate);
    }

    @Test
    void knownProcessedMessageIsSkipped() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(true);
        ConsumerRecord<String, String> record = recordFor("CancelPaymentCommand",
                "{\"transactionId\":\"t1\"}");

        consumer.onMessage(record);

        verify(processedRepo, never()).save(any());
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    void newCommandPublishesCompensatedThenSaves() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(false);
        ConsumerRecord<String, String> record = recordFor("CancelPaymentCommand",
                "{\"transactionId\":\"t1\"}");

        consumer.onMessage(record);

        verify(kafkaTemplate).send(eq("apex.payment.events"), eq("t1"), any(PaymentCompensated.class));
        verify(processedRepo).save(any(ProcessedMessage.class));
    }

    @Test
    void duplicateMarkExceptionIsSwallowedNotPropagated() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(false);
        doThrow(new DataIntegrityViolationException("dup")).when(processedRepo).save(any());
        ConsumerRecord<String, String> record = recordFor("CancelPaymentCommand",
                "{\"transactionId\":\"t1\"}");

        consumer.onMessage(record);

        verify(kafkaTemplate).send(eq("apex.payment.events"), eq("t1"), any(PaymentCompensated.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void failedPublishPropagatesAndDoesNotMarkProcessed() throws Exception {
        when(processedRepo.existsById(MESSAGE_ID)).thenReturn(false);
        CompletableFuture<SendResult<String, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("broker unavailable"));
        when(kafkaTemplate.send(any(), any(), any())).thenReturn(failedFuture);
        ConsumerRecord<String, String> record = recordFor("CancelPaymentCommand",
                "{\"transactionId\":\"t1\"}");

        assertThatThrownBy(() -> consumer.onMessage(record)).isInstanceOf(IllegalStateException.class);

        verify(processedRepo, never()).save(any());
    }

    @Test
    void missingIdFieldThrowsInsteadOfNpe() {
        String value = "{\"after\":{\"event_type\":\"CancelPaymentCommand\",\"payload\":\"{}\"},\"op\":\"c\"}";
        ConsumerRecord<String, String> record = new ConsumerRecord<>("apex.public.outbox_event", 0, 0, "key", value);

        assertThatThrownBy(() -> consumer.onMessage(record)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void missingPayloadFieldThrowsInsteadOfNpe() {
        String value = "{\"after\":{\"id\":\"" + MESSAGE_ID + "\",\"event_type\":\"CancelPaymentCommand\"},\"op\":\"c\"}";
        ConsumerRecord<String, String> record = new ConsumerRecord<>("apex.public.outbox_event", 0, 0, "key", value);

        assertThatThrownBy(() -> consumer.onMessage(record)).isInstanceOf(PayloadDeserializationException.class);
    }
}
