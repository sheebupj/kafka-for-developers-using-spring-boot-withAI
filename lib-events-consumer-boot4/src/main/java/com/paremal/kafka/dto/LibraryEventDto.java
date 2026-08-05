package com.paremal.kafka.dto;

import com.paremal.kafka.domain.EventType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

public record LibraryEventDto(
        Integer libraryEventId,
        @NotNull(message = "eventType is required") EventType eventType,
        @NotNull(message = "book is required") @Valid BookDto book) {
}
