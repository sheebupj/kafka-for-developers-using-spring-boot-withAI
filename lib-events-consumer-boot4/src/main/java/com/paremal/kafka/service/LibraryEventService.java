package com.paremal.kafka.service;

import com.paremal.kafka.dto.LibraryEventDto;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
public class LibraryEventService {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);

    private final ObjectMapper objectMapper;

    public LibraryEventService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public void processEvent(ConsumerRecord<Integer, String> consumerRecord) throws JacksonException {
        try {
            var libraryEventDto = objectMapper.readValue(consumerRecord.value(), LibraryEventDto.class);
            log.info("LibraryEventDto deserialized. libraryEventDto={}", libraryEventDto);
        } catch (JacksonException exception) {
            log.error(
                    "Failed to deserialize library event. topic={}, partition={}, offset={}, key={}, value={}",
                    consumerRecord.topic(),
                    consumerRecord.partition(),
                    consumerRecord.offset(),
                    consumerRecord.key(),
                    consumerRecord.value(),
                    exception);
            throw exception;
        }
    }
}
