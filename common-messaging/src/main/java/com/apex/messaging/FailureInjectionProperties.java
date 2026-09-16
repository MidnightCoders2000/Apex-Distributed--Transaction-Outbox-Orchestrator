package com.apex.messaging;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "apex.failure-injection")
public record FailureInjectionProperties(double rate, List<String> triggerAccountIds) {

    public FailureInjectionProperties {
        triggerAccountIds = triggerAccountIds == null
                ? List.of()
                : triggerAccountIds.stream().filter(id -> id != null && !id.isBlank()).toList();
    }
}
