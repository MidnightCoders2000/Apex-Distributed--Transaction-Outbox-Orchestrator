package com.apex.payment.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxEventEnvelopeParserTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final OutboxEventEnvelopeParser parser = new OutboxEventEnvelopeParser(objectMapper);

    @Test
    void relevantEventTypeIsRecognized() {
        String json = """
                {"before":null,"after":{"id":"11111111-1111-1111-1111-111111111111","event_type":"PaymentRequested","payload":"{}"},"op":"c"}
                """;
        Optional<OutboxEnvelope> envelope = parser.parse(json);
        assertThat(envelope).isPresent();
        assertThat(parser.isRelevant(envelope.get(), "PaymentRequested")).isTrue();
    }

    @Test
    void irrelevantEventTypeIsFiltered() {
        String json = """
                {"before":null,"after":{"id":"11111111-1111-1111-1111-111111111111","event_type":"ShipmentRequested","payload":"{}"},"op":"c"}
                """;
        Optional<OutboxEnvelope> envelope = parser.parse(json);
        assertThat(envelope).isPresent();
        assertThat(parser.isRelevant(envelope.get(), "PaymentRequested")).isFalse();
    }

    @Test
    void malformedJsonIsSkipped() {
        Optional<OutboxEnvelope> envelope = parser.parse("not json");
        assertThat(envelope).isEmpty();
    }

    @Test
    void missingAfterIsNotRelevant() {
        String json = """
                {"before":{"id":"11111111-1111-1111-1111-111111111111"},"after":null,"op":"d"}
                """;
        Optional<OutboxEnvelope> envelope = parser.parse(json);
        assertThat(envelope).isPresent();
        assertThat(parser.isRelevant(envelope.get(), "PaymentRequested")).isFalse();
    }

    @Test
    void nullOpIsNotRelevant() {
        String json = """
                {"before":null,"after":{"id":"11111111-1111-1111-1111-111111111111","event_type":"PaymentRequested","payload":"{}"},"op":null}
                """;
        Optional<OutboxEnvelope> envelope = parser.parse(json);
        assertThat(envelope).isPresent();
        assertThat(parser.isRelevant(envelope.get(), "PaymentRequested")).isFalse();
    }
}
