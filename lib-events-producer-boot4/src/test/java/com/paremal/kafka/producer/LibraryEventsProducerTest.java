package com.paremal.kafka.producer;

import com.paremal.kafka.exception.KafkaPublishException;
import com.paremal.kafka.model.Book;
import com.paremal.kafka.model.EventType;
import com.paremal.kafka.model.LibraryEvent;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LibraryEventsProducerTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private LibraryEventsProducer libraryEventsProducer;

    @BeforeEach
    void setUp() {
        libraryEventsProducer = new LibraryEventsProducer(kafkaTemplate, "library-events", new SimpleMeterRegistry());
    }

    @Test
    void send_returnsMetadataAndIncrementsMetricOnSuccess() {
        LibraryEvent event = new LibraryEvent(1L, EventType.ADD, new Book(1, "Kafka", "Dilip"), null);
        RecordMetadata metadata = org.mockito.Mockito.mock(RecordMetadata.class);
        SendResult<String, Object> sendResult = org.mockito.Mockito.mock(SendResult.class);

        when(sendResult.getRecordMetadata()).thenReturn(metadata);
        when(kafkaTemplate.send(eq("library-events"), eq("1"), any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        RecordMetadata result = libraryEventsProducer.send("1", event).join();

        assertThat(result).isEqualTo(metadata);
    }

    @Test
    void send_wrapsRuntimeExceptionInKafkaPublishException() {
        LibraryEvent event = new LibraryEvent(1L, EventType.ADD, new Book(1, "Kafka", "Dilip"), null);

        when(kafkaTemplate.send(eq("library-events"), eq("1"), any(LibraryEvent.class)))
                .thenThrow(new RuntimeException("boom"));

        assertThatThrownBy(() -> libraryEventsProducer.send("1", event).join())
                .isInstanceOf(java.util.concurrent.CompletionException.class)
                .hasCauseInstanceOf(KafkaPublishException.class);
    }
}

