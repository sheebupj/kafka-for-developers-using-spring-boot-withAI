package com.paremal.kafka.mapper;

import com.paremal.kafka.dto.BookDto;
import com.paremal.kafka.dto.BookResponseDto;
import com.paremal.kafka.dto.LibraryEventDto;
import com.paremal.kafka.dto.LibraryEventResponseDto;
import com.paremal.kafka.entity.Book;
import com.paremal.kafka.entity.LibraryEvent;
import org.springframework.stereotype.Component;

/**
 * Maps library event and book objects between DTO and entity layers.
 */
@Component
public class LibraryEventMapper {

    // ── Book ↔ REST layer ────────────────────────────────────────────────────

    /**
     * Converts a {@link Book} entity to a REST response DTO.
     */
    public static BookResponseDto toBookResponseDto(Book book) {
        Integer libraryEventId = book.getLibraryEvent() != null
                ? book.getLibraryEvent().getLibraryEventId()
                : null;
        return new BookResponseDto(
                book.getBookId(),
                book.getBookName(),
                book.getBookAuthor(),
                libraryEventId,
                book.getCreatedAt(),
                book.getUpdatedAt()
        );
    }
    /**
     * Converts a {@link LibraryEvent} entity to a REST response DTO.
     */
    public LibraryEventResponseDto toLibraryEventResponseDto(LibraryEvent libraryEvent) {
        BookResponseDto book = libraryEvent.getBook() != null
                ? toBookResponseDto(libraryEvent.getBook())
                : null;
        return new LibraryEventResponseDto(
                libraryEvent.getLibraryEventId(),
                libraryEvent.getEventType(),
                book,
                libraryEvent.getCreatedAt(),
                libraryEvent.getUpdatedAt()
        );
    }

    /**
     * Converts an incoming {@link BookDto} to a {@link Book} entity.
     */
    public static Book toBookEntity(BookDto dto) {
        Book book = new Book();
        book.setBookId(dto.bookId());
        book.setBookName(dto.bookName());
        book.setBookAuthor(dto.bookAuthor());
        return book;
    }

    // ── Kafka consumer path ──────────────────────────────────────────────────

    /**
     * Creates a new {@link LibraryEvent} entity from a Kafka payload DTO.
     */
    public LibraryEvent toEntity(LibraryEventDto dto) {
        var libraryEvent = new LibraryEvent();
        libraryEvent.setEventType(dto.eventType());

        var book = new Book();
        mapBook(dto.book(), book);
        book.setLibraryEvent(libraryEvent);
        libraryEvent.setBook(book);

        return libraryEvent;
    }

    /**
     * Updates an existing {@link LibraryEvent} from an incoming DTO.
     * Rejects updates where the book ID changes for the same event.
     */
    public void updateEntity(LibraryEventDto dto, LibraryEvent existing) {
        existing.setEventType(dto.eventType());

        var existingBook = existing.getBook();
        if (existingBook == null) {
            existingBook = new Book();
            existingBook.setLibraryEvent(existing);
            existing.setBook(existingBook);
        } else if (existingBook.getBookId() != null && !existingBook.getBookId().equals(dto.book().bookId())) {
            throw new IllegalArgumentException(
                    "bookId cannot be changed for an existing libraryEvent. existingBookId=%d, incomingBookId=%d"
                            .formatted(existingBook.getBookId(), dto.book().bookId()));
        }

        mapBook(dto.book(), existingBook);
    }

    /**
     * Copies book fields from DTO to entity.
     */
    private void mapBook(BookDto source, Book target) {
        target.setBookId(source.bookId());
        target.setBookName(source.bookName());
        target.setBookAuthor(source.bookAuthor());
    }
}
