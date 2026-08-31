package com.paremal.kafka.consumer;

import com.paremal.kafka.dto.LibraryEventDto;
import com.paremal.kafka.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka listener component that receives library event messages and delegates processing.
 */
@Component
public class
LibraryEventsConsumer {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumer.class);
    private final LibraryEventService libraryEventService;

    /**
     * Creates the consumer with the required library event service dependency.
     */
    public LibraryEventsConsumer(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    /**
     * Handles messages from the {@code library-events} topic.
     */
    @KafkaListener(topics = "library-events")
    public void onMessage(ConsumerRecord<Integer, LibraryEventDto> consumerRecord
            //, Acknowledgment acknowledgment
    ) {
        log.info(
                "ConsumerRecord received. topic={}, partition={}, offset={}, key={}, value={}",
                consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.key(),
                consumerRecord.value());
        libraryEventService.processEvent(consumerRecord);
       // acknowledgment.acknowledge();
    }
}
