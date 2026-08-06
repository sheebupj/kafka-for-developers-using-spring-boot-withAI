package com.paremal.kafka.service;

import com.paremal.kafka.domain.EventType;
import com.paremal.kafka.dto.BookDto;
import com.paremal.kafka.dto.LibraryEventDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class LibraryEventServiceTest {

    private final LibraryEventService libraryEventService = new LibraryEventService();

    @Test
    void processEventAcceptsTypedPayload() {
        var consumerRecord = new ConsumerRecord<>(
                "library-events",
                0,
                0L,
                1,
                new LibraryEventDto(1, EventType.ADD, new BookDto(123, "Kafka", "Dilip")));

        assertDoesNotThrow(() -> libraryEventService.processEvent(consumerRecord));
    }

    @Test
    void processEventAcceptsNullPayload() {
        var consumerRecord = new ConsumerRecord<Integer, LibraryEventDto>("library-events", 0, 0L, 1, null);
        assertDoesNotThrow(() -> libraryEventService.processEvent(consumerRecord));
    }
}
