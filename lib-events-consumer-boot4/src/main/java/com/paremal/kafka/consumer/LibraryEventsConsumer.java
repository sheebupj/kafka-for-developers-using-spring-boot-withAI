package com.paremal.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.paremal.kafka.service.LibraryEventService;
import tools.jackson.core.JacksonException;

@Component
public class LibraryEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumer.class);
    private final LibraryEventService libraryEventService;

    public LibraryEventsConsumer(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    @KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JacksonException {
        log.info(
                "ConsumerRecord received. topic={}, partition={}, offset={}, key={}, value={}",
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.key(),
                consumerRecord.value());
        libraryEventService.processEvent(consumerRecord);
    }
}
