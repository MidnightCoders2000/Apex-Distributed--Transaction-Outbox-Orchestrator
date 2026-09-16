package com.apex.shipment.consumer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

/**
 * Overrides Spring Boot's autoconfigured "kafkaListenerContainerFactory" bean
 * (same name, so this replaces it) to attach a bounded retry-then-dead-letter
 * error handler. Without it, an exception from the listener (including a
 * failed publish under the publish-before-mark ordering — see
 * ShipmentOutboxConsumer) would retry the same record forever and stall the
 * partition; with retry-then-skip and no dead letter, a record that
 * exhausts retries would be dropped with nothing but a log line.
 *
 * The dead-letter producer is built inline, NOT exposed as a {@code @Bean}:
 * KafkaAutoConfiguration guards its own business KafkaTemplate bean with
 * {@code @ConditionalOnMissingBean(KafkaTemplate.class)}, which matches by
 * raw type regardless of generics, so a second {@code KafkaTemplate} bean
 * of any generic shape silently disables the autoconfigured one — breaking
 * every other constructor in this service that wants
 * {@code KafkaTemplate<String, Object>} (see ShipmentOutboxConsumer). It is
 * still a dedicated String/String producer, not the business
 * KafkaTemplate<String, Object> (which serializes values as JSON): the
 * value being recovered here is the raw CDC envelope string consumed off
 * apex.public.outbox_event, not a domain event, so a JSON serializer would
 * double-encode it. See KafkaConsumerConfigTest for the regression test.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final String DLT_TOPIC = "apex.shipment.events.DLT";

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory,
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers) {
        Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        KafkaTemplate<String, String> dltTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));

        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                dltTemplate, (record, ex) -> new TopicPartition(DLT_TOPIC, -1));
        factory.setCommonErrorHandler(new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2)));
        return factory;
    }
}
