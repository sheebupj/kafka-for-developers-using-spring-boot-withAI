package com.paremal.kafka.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record BookDto(
        @NotNull(message = "bookId is required") Integer bookId,
        @NotBlank(message = "bookName is required") String bookName,
        @NotBlank(message = "bookAuthor is required") String bookAuthor) {
}
