package com.apex.shipment.injection;

import com.apex.messaging.FailureInjectionProperties;
import org.springframework.stereotype.Component;

import java.util.random.RandomGenerator;

@Component
public class ShipmentFailureInjectionPolicy {

    private final FailureInjectionProperties properties;
    private final RandomGenerator random;

    public ShipmentFailureInjectionPolicy(FailureInjectionProperties properties, RandomGenerator random) {
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
