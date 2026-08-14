package com.paremal.kafka.model;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

@Schema(description = "Book information")
public class Book {

    @Schema(description = "Unique identifier of the book", example = "123", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotNull(message = "bookId is required")
    @Positive(message = "bookId must be a positive number")
    private Integer bookId;

    @Schema(description = "Title of the book", example = "Kafka Using Spring Boot", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotBlank(message = "bookName is required")
    @Size(max = 255, message = "bookName cannot exceed 255 characters")
    private String bookName;

    @Schema(description = "Author of the book", example = "Dilip", requiredMode = Schema.RequiredMode.REQUIRED)
    @NotBlank(message = "bookAuthor is required")
    @Size(max = 255, message = "bookAuthor cannot exceed 255 characters")
    private String bookAuthor;

    public Book() {
    }

    public Book(Integer bookId, String bookName, String bookAuthor) {
        this.bookId = bookId;
        this.bookName = bookName;
        this.bookAuthor = bookAuthor;
    }

    public Integer getBookId() {
        return bookId;
    }

    public void setBookId(Integer bookId) {
        this.bookId = bookId;
    }

    public String getBookName() {
        return bookName;
    }

    public void setBookName(String bookName) {
        this.bookName = bookName;
    }

    public String getBookAuthor() {
        return bookAuthor;
    }

    public void setBookAuthor(String bookAuthor) {
        this.bookAuthor = bookAuthor;
    }

    @Override
    public String toString() {
        return "Book{" +
                "bookId=" + bookId +
                ", bookName='" + bookName + '\'' +
                ", bookAuthor='" + bookAuthor + '\'' +
                '}';
    }
}
