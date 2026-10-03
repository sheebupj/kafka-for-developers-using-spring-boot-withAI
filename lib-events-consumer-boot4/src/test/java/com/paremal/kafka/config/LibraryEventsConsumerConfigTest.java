package com.paremal.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        assertEquals("library-event.DLT", producerRecordCaptor.getValue().topic());
        assertEquals(2, producerRecordCaptor.getValue().partition());
    }
}
