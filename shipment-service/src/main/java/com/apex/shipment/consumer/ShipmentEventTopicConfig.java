package com.apex.shipment.consumer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class ShipmentEventTopicConfig {

    @Bean
    public NewTopic shipmentEventsTopic() {
        return TopicBuilder.name("apex.shipment.events")
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
    public NewTopic shipmentEventsDeadLetterTopic() {
        return TopicBuilder.name("apex.shipment.events.DLT")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
