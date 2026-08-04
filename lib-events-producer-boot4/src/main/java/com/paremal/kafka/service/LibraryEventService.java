package com.paremal.kafka.service;

import com.paremal.kafka.producer.LibraryEventsProducer;
import com.paremal.kafka.model.LibraryEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.RecordMetadata;

@Service
public class LibraryEventService {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);

    private final LibraryEventsProducer producer;

    public LibraryEventService(LibraryEventsProducer producer) {
        this.producer = producer;
    }

    public CompletableFuture<RecordMetadata> publishAdd(LibraryEvent libraryEvent) {
        if (libraryEvent.getTimestamp() == null) {
            libraryEvent.setTimestamp(Instant.now());
        }
        Integer key = Math.toIntExact(libraryEvent.getLibraryEventId());
        return producer.send(key, libraryEvent)
                .whenComplete((md, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish ADD event id={}", libraryEvent.getLibraryEventId(), ex);
                        return;
                    }
                    log.debug("Published ADD event id={} partition={} offset={}", libraryEvent.getLibraryEventId(), md.partition(), md.offset());
                });
    }

    public CompletableFuture<RecordMetadata> publishUpdate(LibraryEvent libraryEvent) {
        return updateLibraryEvent(libraryEvent);
    }

    public CompletableFuture<RecordMetadata> updateLibraryEvent(LibraryEvent libraryEvent) {
        if (libraryEvent.getTimestamp() == null) {
            libraryEvent.setTimestamp(Instant.now());
        }
        Integer key = Math.toIntExact(libraryEvent.getLibraryEventId());
        return producer.send(key, libraryEvent)
                .whenComplete((md, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish UPDATE event id={}", libraryEvent.getLibraryEventId(), ex);
                        return;
                    }
                    log.debug("Published UPDATE event id={} partition={} offset={}", libraryEvent.getLibraryEventId(), md.partition(), md.offset());
                });
    }
}
