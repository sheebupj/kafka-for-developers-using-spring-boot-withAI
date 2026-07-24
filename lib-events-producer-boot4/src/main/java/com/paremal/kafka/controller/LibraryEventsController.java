package com.paremal.kafka.controller;

import com.paremal.kafka.model.EventType;
import com.paremal.kafka.model.LibraryEvent;
import com.paremal.kafka.service.LibraryEventService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/v1/library-events")
public class LibraryEventsController {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsController.class);

    private final LibraryEventService libraryEventService;

    public LibraryEventsController(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    @PostMapping
    public ResponseEntity<?> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent, BindingResult bindingResult) {
        if (bindingResult.hasErrors()) {
            String errors = bindingResult.getAllErrors().stream()
                    .map(e -> e.getDefaultMessage() != null ? e.getDefaultMessage() : e.toString())
                    .collect(Collectors.joining(", "));
            return ResponseEntity.badRequest().body(errors);
        }

        if (libraryEvent.getEventType() != EventType.ADD) {
            return ResponseEntity.badRequest().body("eventType must be ADD for POST endpoint");
        }

        log.info("Publishing ADD library event: {}", libraryEvent);
        libraryEventService.publishAdd(libraryEvent);
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }

    @PutMapping("/{libraryEventId}")
    public ResponseEntity<?> updateLibraryEvent(@PathVariable("libraryEventId") Long libraryEventId,
                                                @RequestBody @Valid LibraryEvent libraryEvent,
                                                BindingResult bindingResult) {
        if (bindingResult.hasErrors()) {
            String errors = bindingResult.getAllErrors().stream()
                    .map(e -> e.getDefaultMessage() != null ? e.getDefaultMessage() : e.toString())
                    .collect(Collectors.joining(", "));
            return ResponseEntity.badRequest().body(errors);
        }

        if (!libraryEventId.equals(libraryEvent.getLibraryEventId())) {
            return ResponseEntity.badRequest().body("Path libraryEventId must match body.libraryEventId");
        }

        if (libraryEvent.getEventType() != EventType.UPDATE) {
            return ResponseEntity.badRequest().body("eventType must be UPDATE for PUT endpoint");
        }

        log.info("Publishing UPDATE library event: {}", libraryEvent);
        libraryEventService.publishUpdate(libraryEvent);
        return ResponseEntity.ok(libraryEvent);
    }
}
