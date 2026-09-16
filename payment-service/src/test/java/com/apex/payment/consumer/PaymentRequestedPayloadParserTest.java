package com.apex.payment.consumer;

import com.apex.events.PaymentRequested;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaymentRequestedPayloadParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final PaymentRequestedPayloadParser parser = new PaymentRequestedPayloadParser(objectMapper);

    private OutboxEnvelope envelopeWithPayload(String payloadJson) throws Exception {
        String envelopeJson = "{\"after\":{\"payload\":" + objectMapper.writeValueAsString(payloadJson) + "},\"op\":\"c\"}";
        return objectMapper.readValue(envelopeJson, OutboxEnvelope.class);
    }

    @Test
    void parsesDoubleEncodedJsonbPayload() throws Exception {
        String payload = "{\"correlationId\":\"corr-1\",\"transactionId\":\"tx-1\",\"accountId\":\"acct-1\",\"amount\":10.00}";
        OutboxEnvelope envelope = envelopeWithPayload(payload);

        PaymentRequested requested = parser.parse(envelope);

        assertThat(requested.correlationId()).isEqualTo("corr-1");
        assertThat(requested.transactionId()).isEqualTo("tx-1");
        assertThat(requested.accountId()).isEqualTo("acct-1");
    }

    @Test
    void malformedPayloadThrows() throws Exception {
        OutboxEnvelope envelope = envelopeWithPayload("not valid json");

        assertThatThrownBy(() -> parser.parse(envelope))
                .isInstanceOf(PayloadDeserializationException.class);
    }

    @Test
    void missingPayloadFieldThrowsInsteadOfNpe() throws Exception {
        OutboxEnvelope envelope = objectMapper.readValue(
                "{\"after\":{\"event_type\":\"PaymentRequested\"},\"op\":\"c\"}", OutboxEnvelope.class);

        assertThatThrownBy(() -> parser.parse(envelope))
                .isInstanceOf(PayloadDeserializationException.class);
    }
}
