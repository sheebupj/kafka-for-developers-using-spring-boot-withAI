package com.paremal.kafka.dto;



import com.paremal.kafka.domain.EventType;

import java.time.LocalDateTime;

/**
 * Response DTO representing library event details exposed by REST APIs.
 */
public record LibraryEventResponseDto(
        Integer libraryEventId,
        EventType eventType,
        BookResponseDto book,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}
