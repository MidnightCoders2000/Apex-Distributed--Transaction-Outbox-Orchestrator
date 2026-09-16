package com.apex.shipment.idempotency;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ProcessedMessageTest {

    @Test
    void freshlyConstructedInstanceIsNew() {
        ProcessedMessage message = new ProcessedMessage(UUID.randomUUID());

        assertThat(message.isNew()).isTrue();
    }

    @Test
    void isNotNewAfterPostLoadCallback() throws Exception {
        ProcessedMessage message = new ProcessedMessage(UUID.randomUUID());

        var callback = ProcessedMessage.class.getDeclaredMethod("markNotNew");
        callback.setAccessible(true);
        callback.invoke(message);

        assertThat(message.isNew()).isFalse();
    }
}
