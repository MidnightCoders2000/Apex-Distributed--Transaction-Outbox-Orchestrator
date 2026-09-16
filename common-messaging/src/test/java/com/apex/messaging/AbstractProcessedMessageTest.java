package com.apex.messaging;

import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class AbstractProcessedMessageTest {

    @Entity
    @Table(name = "test_processed_message")
    static class TestProcessedMessage extends AbstractProcessedMessage {

        protected TestProcessedMessage() {
        }

        TestProcessedMessage(UUID messageId) {
            super(messageId);
        }
    }

    @Test
    void freshlyConstructedInstanceIsNew() {
        TestProcessedMessage message = new TestProcessedMessage(UUID.randomUUID());

        assertThat(message.isNew()).isTrue();
    }

    @Test
    void isNotNewAfterPostLoadCallback() throws Exception {
        TestProcessedMessage message = new TestProcessedMessage(UUID.randomUUID());

        var callback = AbstractProcessedMessage.class.getDeclaredMethod("markNotNew");
        callback.setAccessible(true);
        callback.invoke(message);

        assertThat(message.isNew()).isFalse();
    }
}
