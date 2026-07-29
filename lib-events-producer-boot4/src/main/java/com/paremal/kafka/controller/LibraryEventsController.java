package com.paremal.kafka.controller;

import com.paremal.kafka.exception.ApiErrorResponse;
import com.paremal.kafka.model.EventType;
import com.paremal.kafka.model.LibraryEvent;
import com.paremal.kafka.service.LibraryEventService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@RestController
public class LibraryEventsController {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsController.class);

    private final LibraryEventService libraryEventService;

    public LibraryEventsController(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    @PostMapping("/api/v1/library-events")
    public CompletableFuture<ResponseEntity<?>> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {
        if (libraryEvent.getEventType() != EventType.ADD) {
            List<ApiErrorResponse.FieldError> errors = new ArrayList<>();
            errors.add(new ApiErrorResponse.FieldError("eventType", "eventType must be ADD for POST endpoint"));
            ApiErrorResponse errorResponse = new ApiErrorResponse(
                    HttpStatus.BAD_REQUEST.value(),
                    "Validation failed",
                    errors
            );
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(errorResponse));
        }

        log.info("Publishing ADD library event: {}", libraryEvent);
        return libraryEventService.publishAdd(libraryEvent)
                .thenApply(ignored -> ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent));
    }

    @PutMapping("/api/v1/library-events/{libraryEventId}")
    public CompletableFuture<ResponseEntity<?>> updateLibraryEvent(@PathVariable("libraryEventId") Long libraryEventId,
                                                                    @RequestBody @Valid LibraryEvent libraryEvent) {
        List<ApiErrorResponse.FieldError> errors = new ArrayList<>();

        if (libraryEvent.getLibraryEventId() == null) {
            errors.add(new ApiErrorResponse.FieldError("libraryEventId", "libraryEventId is required"));
        } else if (!libraryEventId.equals(libraryEvent.getLibraryEventId())) {
            errors.add(new ApiErrorResponse.FieldError("libraryEventId", "Path libraryEventId must match body.libraryEventId"));
        }

        if (libraryEvent.getEventType() != EventType.UPDATE) {
            errors.add(new ApiErrorResponse.FieldError("eventType", "eventType must be UPDATE for PUT endpoint"));
        }

        if (!errors.isEmpty()) {
            ApiErrorResponse errorResponse = new ApiErrorResponse(
                    HttpStatus.BAD_REQUEST.value(),
                    "Validation failed",
                    errors
            );
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(errorResponse));
        }

        log.info("Publishing UPDATE library event: {}", libraryEvent);
        return libraryEventService.updateLibraryEvent(libraryEvent)
                .thenApply(ignored -> ResponseEntity.ok(libraryEvent));
    }

    @PutMapping("/v1/libraryevent")
    public CompletableFuture<ResponseEntity<?>> updateLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {
        List<ApiErrorResponse.FieldError> errors = new ArrayList<>();

        if (libraryEvent.getLibraryEventId() == null) {
            errors.add(new ApiErrorResponse.FieldError("libraryEventId", "libraryEventId is required"));
        }

        if (libraryEvent.getEventType() != EventType.UPDATE) {
            errors.add(new ApiErrorResponse.FieldError("eventType", "eventType must be UPDATE for PUT endpoint"));
        }

        if (!errors.isEmpty()) {
            ApiErrorResponse errorResponse = new ApiErrorResponse(
                    HttpStatus.BAD_REQUEST.value(),
                    "Validation failed",
                    errors
            );
            return CompletableFuture.completedFuture(ResponseEntity.badRequest().body(errorResponse));
        }

        log.info("Publishing UPDATE library event: {}", libraryEvent);
        return libraryEventService.updateLibraryEvent(libraryEvent)
                .thenApply(ignored -> ResponseEntity.ok(libraryEvent));
    }
}
