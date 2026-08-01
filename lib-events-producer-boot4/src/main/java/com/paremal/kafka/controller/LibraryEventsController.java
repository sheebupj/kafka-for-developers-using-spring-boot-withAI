package com.paremal.kafka.controller;

import com.paremal.kafka.exception.ApiErrorResponse;
import com.paremal.kafka.model.EventType;
import com.paremal.kafka.model.LibraryEvent;
import com.paremal.kafka.service.LibraryEventService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
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
@Tag(name = "Library Events", description = "Operations for publishing library events to Kafka")
public class LibraryEventsController {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsController.class);

    private final LibraryEventService libraryEventService;

    public LibraryEventsController(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    @PostMapping("/api/v1/library-events")
    @Operation(summary = "Create library event", description = "Publishes an ADD library event to Kafka.")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(
            required = true,
            description = "Library event payload with eventType=ADD",
            content = @Content(
                    schema = @Schema(implementation = LibraryEvent.class),
                    examples = @ExampleObject(
                            name = "addEvent",
                            value = """
                                    {
                                      "libraryEventId": 1,
                                      "eventType": "ADD",
                                      "book": {
                                        "bookId": 123,
                                        "bookName": "Kafka Using Spring Boot",
                                        "bookAuthor": "Dilip"
                                      }
                                    }
                                    """
                    )
            )
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "201",
                    description = "Event accepted and published",
                    content = @Content(schema = @Schema(implementation = LibraryEvent.class))
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Validation error",
                    content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))
            ),
            @ApiResponse(
                    responseCode = "503",
                    description = "Kafka publish failure",
                    content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))
            )
    })
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
    @Operation(summary = "Update library event", description = "Publishes an UPDATE library event to Kafka.")
    @io.swagger.v3.oas.annotations.parameters.RequestBody(
            required = true,
            description = "Library event payload with eventType=UPDATE and matching libraryEventId",
            content = @Content(
                    schema = @Schema(implementation = LibraryEvent.class),
                    examples = @ExampleObject(
                            name = "updateEvent",
                            value = """
                                    {
                                      "libraryEventId": 10,
                                      "eventType": "UPDATE",
                                      "book": {
                                        "bookId": 200,
                                        "bookName": "Kafka Streams in Action",
                                        "bookAuthor": "Bill Bejeck"
                                      }
                                    }
                                    """
                    )
            )
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Event accepted and published",
                    content = @Content(schema = @Schema(implementation = LibraryEvent.class))
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Validation error",
                    content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))
            ),
            @ApiResponse(
                    responseCode = "503",
                    description = "Kafka publish failure",
                    content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))
            )
    })
    public CompletableFuture<ResponseEntity<?>> updateLibraryEvent(
                                                                    @Parameter(description = "Library event id in path; must match payload libraryEventId", example = "10")
                                                                    @PathVariable("libraryEventId") Long libraryEventId,
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
    @Operation(summary = "Update library event (legacy endpoint)", description = "Legacy update endpoint that publishes an UPDATE event to Kafka.")
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Event accepted and published",
                    content = @Content(schema = @Schema(implementation = LibraryEvent.class))
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Validation error",
                    content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))
            ),
            @ApiResponse(
                    responseCode = "503",
                    description = "Kafka publish failure",
                    content = @Content(schema = @Schema(implementation = ApiErrorResponse.class))
            )
    })
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
