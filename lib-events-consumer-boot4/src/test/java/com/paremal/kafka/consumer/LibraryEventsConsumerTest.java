package com.paremal.kafka.consumer;

import com.paremal.kafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import static org.junit.jupiter.api.Assertions.assertSame;

class LibraryEventsConsumerTest {

    @Test
    void onMessageDelegatesToLibraryEventService() throws Exception {
        var libraryEventService = new CapturingLibraryEventService();
        var libraryEventsConsumer = new LibraryEventsConsumer(libraryEventService);
        var consumerRecord = new ConsumerRecord<>("library-events", 0, 0L, 1, "{\"eventType\":\"ADD\"}");

        libraryEventsConsumer.onMessage(consumerRecord);

        assertSame(consumerRecord, libraryEventService.consumerRecord);
    }

    private static final class CapturingLibraryEventService extends LibraryEventService {

        private ConsumerRecord<Integer, String> consumerRecord;

        private CapturingLibraryEventService() {
            super(new ObjectMapper());
        }

        @Override
        public void processEvent(ConsumerRecord<Integer, String> consumerRecord) {
            this.consumerRecord = consumerRecord;
        }
    }
}
