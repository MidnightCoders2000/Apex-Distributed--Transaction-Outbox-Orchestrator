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
}
