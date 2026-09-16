package com.apex.payment;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Full-context smoke test, the one thing the Kafka-focused
 * KafkaConsumerConfigTest doesn't cover: that Flyway's
 * V1__create_payment_processed_message.sql actually produces a table that
 * matches the JPA/ProcessedMessage mapping under ddl-auto: validate.
 * @ServiceConnection wires the container's JDBC coordinates in, overriding
 * the ${DB_URL}/${DB_USERNAME}/${DB_PASSWORD} placeholders that only
 * resolve against the real (Neon) database outside a container. The
 * container starts with no seed DDL so Flyway is the only thing that can
 * create the table; a broken or missing migration fails this test.
 * Kafka's bootstrap-servers is pointed at a closed port on purpose: the
 * listener container connects lazily off the startup thread, so context
 * refresh doesn't need a broker, and this keeps the test from silently
 * depending on whatever's already running in docker-compose.
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=localhost:1")
@Testcontainers
class PaymentServiceApplicationTests {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @Test
    void contextLoads() {
    }
}
