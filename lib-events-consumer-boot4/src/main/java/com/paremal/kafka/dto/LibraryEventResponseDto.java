package com.paremal.kafka.dto;



import com.paremal.kafka.domain.EventType;

import java.time.LocalDateTime;

public record LibraryEventResponseDto(
        Integer libraryEventId,
        EventType eventType,
        BookResponseDto book,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}

