package com.apex.payment.consumer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class PaymentEventTopicConfig {

    @Bean
    public NewTopic paymentEventsTopic(@Value("${apex.consumer.events-topic}") String eventsTopic) {
        return TopicBuilder.name(eventsTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }

    /**
     * Matches apex.consumer.dlt-topic (see application.yml and
     * com.apex.messaging.KafkaConsumerConfig). Declared explicitly rather
     * than relying on auto.create.topics.enable, for the same reason the
     * primary topic above is: consistent behavior regardless of the
     * broker's auto-create setting.
     */
    @Bean
    public NewTopic paymentEventsDeadLetterTopic(@Value("${apex.consumer.dlt-topic}") String dltTopic) {
        return TopicBuilder.name(dltTopic)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
