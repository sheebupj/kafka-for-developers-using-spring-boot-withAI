package com.paremal.kafka.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public class Book {

    @NotNull(message = "bookId is required")
    @Positive(message = "bookId must be a positive number")
    private Integer bookId;

    @NotBlank(message = "bookName is required")
    @Size(max = 255, message = "bookName cannot exceed 255 characters")
    private String bookName;

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
