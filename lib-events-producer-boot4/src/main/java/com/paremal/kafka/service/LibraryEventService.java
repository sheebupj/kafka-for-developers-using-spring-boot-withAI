package com.paremal.kafka.service;

import com.paremal.kafka.exception.KafkaPublishException;
import com.paremal.kafka.producer.LibraryEventsProducer;
import com.paremal.kafka.model.LibraryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.RecordMetadata;

@Service
public class LibraryEventService {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);

    private final LibraryEventsProducer producer;

    public LibraryEventService(LibraryEventsProducer producer) {
        this.producer = producer;
    }

    public void publishAdd(LibraryEvent libraryEvent) {
        if (libraryEvent.getTimestamp() == null) {
            libraryEvent.setTimestamp(Instant.now());
        }
        String key = libraryEvent.getLibraryEventId().toString();
        try {
            RecordMetadata md = producer.send(key, libraryEvent).get(10, TimeUnit.SECONDS);
            log.debug("Published ADD event id={} partition={} offset={}", libraryEvent.getLibraryEventId(), md.partition(), md.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Interrupted while publishing message", e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Failed to publish ADD event id={}", libraryEvent.getLibraryEventId(), e);
            throw new KafkaPublishException("Failed to publish message", e);
        }
    }

    public void publishUpdate(LibraryEvent libraryEvent) {
        if (libraryEvent.getTimestamp() == null) {
            libraryEvent.setTimestamp(Instant.now());
        }
        String key = libraryEvent.getLibraryEventId().toString();
        try {
            RecordMetadata md = producer.send(key, libraryEvent).get(10, TimeUnit.SECONDS);
            log.debug("Published UPDATE event id={} partition={} offset={}", libraryEvent.getLibraryEventId(), md.partition(), md.offset());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Interrupted while publishing message", e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Failed to publish UPDATE event id={}", libraryEvent.getLibraryEventId(), e);
            throw new KafkaPublishException("Failed to publish message", e);
        }
    }
}
