package com.paremal.kafka.service;

import com.paremal.kafka.model.Book;
import com.paremal.kafka.model.EventType;
import com.paremal.kafka.model.LibraryEvent;
import com.paremal.kafka.producer.LibraryEventsProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LibraryEventServiceTest {

    @Mock
    private LibraryEventsProducer producer;

    @InjectMocks
    private LibraryEventService libraryEventService;

    @Test
    void publishAdd_setsTimestampAndDelegatesToProducer() {
        LibraryEvent event = new LibraryEvent(1L, EventType.ADD, new Book(10, "Kafka", "Dilip"), null);
        RecordMetadata metadata = org.mockito.Mockito.mock(RecordMetadata.class);
        Instant beforeCall = Instant.now();

        when(producer.send(eq(1), any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(metadata));

        CompletableFuture<RecordMetadata> future = libraryEventService.publishAdd(event);

        assertThat(event.getTimestamp()).isNotNull();
        assertThat(event.getTimestamp()).isAfterOrEqualTo(beforeCall);
        assertThat(future.join()).isEqualTo(metadata);
        verify(producer).send(eq(1), any(LibraryEvent.class));
    }

    @Test
    void updateLibraryEvent_preservesExistingTimestampAndDelegatesToProducer() {
        Instant timestamp = Instant.parse("2026-07-31T10:15:30Z");
        LibraryEvent event = new LibraryEvent(2L, EventType.UPDATE, new Book(11, "Spring", "Dilip"), timestamp);
        RecordMetadata metadata = org.mockito.Mockito.mock(RecordMetadata.class);

        when(producer.send(eq(2), any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(metadata));

        CompletableFuture<RecordMetadata> future = libraryEventService.updateLibraryEvent(event);

        assertThat(event.getTimestamp()).isEqualTo(timestamp);
        assertThat(future.join()).isEqualTo(metadata);
        verify(producer).send(eq(2), any(LibraryEvent.class));
    }
}
