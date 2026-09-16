package com.apex.messaging;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: KafkaConsumerConfig's dead-letter producer was briefly
 * exposed as its own {@code @Bean KafkaTemplate<String, String>}.
 * KafkaAutoConfiguration guards its business KafkaTemplate bean with
 * {@code @ConditionalOnMissingBean(KafkaTemplate.class)}, which matches by
 * raw type regardless of generics, so that second bean silently disabled
 * the autoconfigured one — breaking every constructor in a consuming
 * service that wants {@code KafkaTemplate<String, Object>} with an
 * UnsatisfiedDependencyException at startup.
 */
class KafkaConsumerConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KafkaAutoConfiguration.class))
            .withUserConfiguration(KafkaConsumerConfig.class, BusinessTemplateConsumer.class)
            .withPropertyValues(
                    "spring.kafka.bootstrap-servers=localhost:29092",
                    "apex.consumer.dlt-topic=apex.test.events.DLT");

    @Test
    void businessKafkaTemplateInjectionPointStillResolvesAlongsideDltRecoverer() {
        contextRunner.run(context -> assertThat(context).hasNotFailed());
    }

    @Configuration(proxyBeanMethods = false)
    static class BusinessTemplateConsumer {

        BusinessTemplateConsumer(KafkaTemplate<String, Object> kafkaTemplate) {
        }
    }
}
