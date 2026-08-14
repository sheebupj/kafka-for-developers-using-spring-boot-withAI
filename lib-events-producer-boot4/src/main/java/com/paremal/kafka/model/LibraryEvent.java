package com.paremal.kafka.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.Instant;

@Schema(description = "Represents a library event to be published to Kafka")
public class LibraryEvent {

    @Schema(description = "Unique identifier of the library event", example = "1", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotNull(message = "libraryEventId is required")
    @Positive(message = "libraryEventId must be a positive number")
    private Long libraryEventId;

    @Schema(description = "Type of library event", example = "ADD", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotNull(message = "eventType is required")
    private EventType eventType;

    @Schema(description = "Book details associated with the event", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotNull(message = "book is required")
    @Valid
    private Book book;

    @Schema(description = "Event timestamp (ISO-8601)", example = "2025-01-01T00:00:00Z", accessMode = Schema.AccessMode.READ_ONLY)
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant timestamp;

    public LibraryEvent() {
    }

    public LibraryEvent(Long libraryEventId, EventType eventType, Book book, Instant timestamp) {
        this.libraryEventId = libraryEventId;
        this.eventType = eventType;
        this.book = book;
        this.timestamp = timestamp;
    }

    public Long getLibraryEventId() {
        return libraryEventId;
    }

    public void setLibraryEventId(Long libraryEventId) {
        this.libraryEventId = libraryEventId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public Book getBook() {
        return book;
    }

    public void setBook(Book book) {
        this.book = book;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LibraryEvent{" +
                "libraryEventId=" + libraryEventId +
                ", eventType=" + eventType +
                ", book=" + book +
                ", timestamp=" + timestamp +
                '}';
    }
}
