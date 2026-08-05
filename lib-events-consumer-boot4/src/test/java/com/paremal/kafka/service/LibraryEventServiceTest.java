package com.paremal.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LibraryEventServiceTest {

    private final LibraryEventService libraryEventService = new LibraryEventService(new ObjectMapper());

    @Test
    void processEventDeserializesValidPayload() {
        var consumerRecord = new ConsumerRecord<>(
                "library-events",
                0,
                0L,
                1,
                "{\"libraryEventId\":1,\"eventType\":\"ADD\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka\",\"bookAuthor\":\"Dilip\"}}");

        assertDoesNotThrow(() -> libraryEventService.processEvent(consumerRecord));
    }

    @Test
    void processEventThrowsForMalformedJson() {
        var consumerRecord = new ConsumerRecord<>("library-events", 0, 0L, 1, "{\"eventType\":\"ADD\"");

        assertThrows(JacksonException.class, () -> libraryEventService.processEvent(consumerRecord));
    }
}
