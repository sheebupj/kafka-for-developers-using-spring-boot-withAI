package com.paremal.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class LibraryEventsConsumerConfigTest {

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void deadLetterRecovererPublishesToConfiguredTopicAndOriginalPartition() {
        KafkaOperations<Object, Object> kafkaOperations = mock(KafkaOperations.class);
        DeadLetterPublishingRecoverer recoverer =
                new LibraryEventsConsumerConfig().deadLetterPublishingRecoverer(kafkaOperations);
        ConsumerRecord<Integer, String> record = new ConsumerRecord<>("library-events", 2, 10L, 5, "event");

        recoverer.accept(record, new IllegalStateException("processing failed"));

        var producerRecordCaptor = org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaOperations).send(producerRecordCaptor.capture());
        assertEquals("library-events.DLT", producerRecordCaptor.getValue().topic());
        assertEquals(2, producerRecordCaptor.getValue().partition());
    }

    @Test
    void bothRecovererAttemptsFailureTableAndDeadLetterRecovery() {
        ConsumerRecordRecoverer failureTableRecoverer = mock(ConsumerRecordRecoverer.class);
        ConsumerRecordRecoverer deadLetterRecoverer = mock(ConsumerRecordRecoverer.class);
        ConsumerRecord<Integer, String> record = new ConsumerRecord<>("library-events", 2, 10L, 5, "event");
        var exception = new IllegalStateException("processing failed");
        var recoverer = LibraryEventsConsumerConfig.bothRecoverer(
                failureTableRecoverer, deadLetterRecoverer);

        recoverer.accept(record, exception);

        verify(failureTableRecoverer).accept(record, exception);
        verify(deadLetterRecoverer).accept(record, exception);
    }

    @Test
    void bothRecovererAttemptsDeadLetterRecoveryWhenFailureTablePersistenceFails() {
        ConsumerRecordRecoverer failureTableRecoverer = mock(ConsumerRecordRecoverer.class);
        ConsumerRecordRecoverer deadLetterRecoverer = mock(ConsumerRecordRecoverer.class);
        ConsumerRecord<Integer, String> record = new ConsumerRecord<>("library-events", 2, 10L, 5, "event");
        var exception = new IllegalStateException("processing failed");
        var persistenceFailure = new IllegalStateException("database unavailable");
        org.mockito.Mockito.doThrow(persistenceFailure)
                .when(failureTableRecoverer).accept(record, exception);
        var recoverer = LibraryEventsConsumerConfig.bothRecoverer(
                failureTableRecoverer, deadLetterRecoverer);

        var thrown = assertThrows(IllegalStateException.class, () -> recoverer.accept(record, exception));

        assertSame(persistenceFailure, thrown);
        verify(deadLetterRecoverer).accept(record, exception);
    }
}
