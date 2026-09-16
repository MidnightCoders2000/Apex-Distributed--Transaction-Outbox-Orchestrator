package com.apex.payment.injection;

import com.apex.messaging.FailureInjectionProperties;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.random.RandomGenerator;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentFailureInjectionPolicyTest {

    private static RandomGenerator fixed(double value) {
        return new RandomGenerator() {
            @Override
            public long nextLong() {
                return 0L;
            }

            @Override
            public double nextDouble() {
                return value;
            }
        };
    }

    @Test
    void triggerAccountAlwaysFailsRegardlessOfRate() {
        FailureInjectionProperties props = new FailureInjectionProperties(0.0, List.of("acct-1"));
        PaymentFailureInjectionPolicy policy = new PaymentFailureInjectionPolicy(props, fixed(0.99));

        assertThat(policy.shouldFail("acct-1")).isTrue();
    }

    @Test
    void nonTriggerAccountFailsWhenRandomBelowRate() {
        FailureInjectionProperties props = new FailureInjectionProperties(0.5, List.of());
        PaymentFailureInjectionPolicy policy = new PaymentFailureInjectionPolicy(props, fixed(0.3));

        assertThat(policy.shouldFail("acct-2")).isTrue();
    }

    @Test
    void nonTriggerAccountSucceedsWhenRandomAtOrAboveRate() {
        FailureInjectionProperties props = new FailureInjectionProperties(0.5, List.of());
        PaymentFailureInjectionPolicy policy = new PaymentFailureInjectionPolicy(props, fixed(0.5));

        assertThat(policy.shouldFail("acct-2")).isFalse();
    }

    @Test
    void zeroRateAndNoTriggerNeverFails() {
        FailureInjectionProperties props = new FailureInjectionProperties(0.0, List.of());
        PaymentFailureInjectionPolicy policy = new PaymentFailureInjectionPolicy(props, fixed(0.0));

        assertThat(policy.shouldFail("acct-3")).isFalse();
    }

    @Test
    void blankTriggerAccountIdsAreFilteredOut() {
        FailureInjectionProperties props = new FailureInjectionProperties(0.0, List.of(""));

        assertThat(props.triggerAccountIds()).isEmpty();
    }
}
