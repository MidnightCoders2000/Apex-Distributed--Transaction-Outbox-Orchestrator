package com.apex.payment.injection;

import org.springframework.stereotype.Component;

import java.util.random.RandomGenerator;

@Component
public class PaymentFailureInjectionPolicy {

    private final FailureInjectionProperties properties;
    private final RandomGenerator random;

    public PaymentFailureInjectionPolicy(FailureInjectionProperties properties, RandomGenerator random) {
        this.properties = properties;
        this.random = random;
    }

    public boolean shouldFail(String accountId) {
        if (properties.triggerAccountIds().contains(accountId)) {
            return true;
        }
        return random.nextDouble() < properties.rate();
    }
}
