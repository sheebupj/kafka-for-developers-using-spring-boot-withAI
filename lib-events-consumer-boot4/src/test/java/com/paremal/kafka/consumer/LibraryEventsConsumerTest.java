package com.paremal.kafka.consumer;

import com.paremal.kafka.domain.EventType;
import com.paremal.kafka.dto.BookDto;
import com.paremal.kafka.dto.LibraryEventDto;
import com.paremal.kafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;

class LibraryEventsConsumerTest {

    @Test
    void onMessageDelegatesToLibraryEventService() {
        var libraryEventService = new CapturingLibraryEventService();
        var libraryEventsConsumer = new LibraryEventsConsumer(libraryEventService);
        var consumerRecord = new ConsumerRecord<>(
                "library-events",
                0,
                0L,
                1,
                new LibraryEventDto(1, EventType.ADD, new BookDto(123, "Kafka", "Dilip")));

        libraryEventsConsumer.onMessage(consumerRecord);

        assertSame(consumerRecord, libraryEventService.consumerRecord);
    }

    private static final class CapturingLibraryEventService extends LibraryEventService {

        private ConsumerRecord<Integer, LibraryEventDto> consumerRecord;

        @Override
        public void processEvent(ConsumerRecord<Integer, LibraryEventDto> consumerRecord) {
            this.consumerRecord = consumerRecord;
        }
    }
}
