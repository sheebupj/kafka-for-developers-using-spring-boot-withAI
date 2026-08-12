package com.paremal.kafka.consumer;

import com.paremal.kafka.domain.EventType;
import com.paremal.kafka.dto.BookDto;
import com.paremal.kafka.dto.LibraryEventDto;
import com.paremal.kafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class LibraryEventsConsumerTest {

    @Mock
    private LibraryEventService libraryEventService;

    @Test
    void onMessageDelegatesToLibraryEventService() {
        var libraryEventsConsumer = new LibraryEventsConsumer(libraryEventService);
        var consumerRecord = new ConsumerRecord<>(
                "library-events",
                0,
                0L,
                1,
                new LibraryEventDto(1, EventType.ADD, new BookDto(123, "Kafka", "Dilip")));

        libraryEventsConsumer.onMessage(consumerRecord);

        verify(libraryEventService).processEvent(consumerRecord);
    }
}
