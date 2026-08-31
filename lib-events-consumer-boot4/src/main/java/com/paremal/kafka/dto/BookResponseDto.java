package com.paremal.kafka.dto;

import java.time.LocalDateTime;

/**
 * Response DTO representing book details returned by REST APIs.
 */
public record BookResponseDto(
        Integer bookId,
        String bookName,
        String bookAuthor,
        Integer libraryEventId,
        LocalDateTime createdAt,
        LocalDateTime updatedAt
) {
}
