package com.apex.messaging;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;

@JsonIgnoreProperties(ignoreUnknown = true)
public record OutboxEnvelope(JsonNode before, JsonNode after, JsonNode source, String op, Long tsMs) {
}
