package com.paremal.kafka.service;

import com.paremal.kafka.dto.BookDto;
import com.paremal.kafka.dto.BookResponseDto;
import com.paremal.kafka.entity.Book;
import com.paremal.kafka.mapper.LibraryEventMapper;
import com.paremal.kafka.repository.BookRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
public class BookService {

    private static final Logger log = LoggerFactory.getLogger(BookService.class);

    private final BookRepository bookRepository;

    public BookService(BookRepository bookRepository) {
        this.bookRepository = bookRepository;
    }

    public List<BookResponseDto> findAll() {
        log.info("Fetching all books");
        return bookRepository.findAll()
                .stream()
                .map(LibraryEventMapper::toBookResponseDto)
                .toList();
    }

    public Optional<BookResponseDto> findById(Integer bookId) {
        log.info("Fetching book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(LibraryEventMapper::toBookResponseDto);
    }

    @Transactional
    public BookResponseDto create(BookDto bookDto) {
        log.info("Creating book: {}", bookDto);
        Book book = LibraryEventMapper.toBookEntity(bookDto);
        Book saved = bookRepository.save(book);
        log.info("Successfully created book: {}", saved.getBookId());
        return LibraryEventMapper.toBookResponseDto(saved);
    }

    @Transactional
    public Optional<BookResponseDto> update(Integer bookId, BookDto bookDto) {
        log.info("Updating book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(existing -> {
                    existing.setBookName(bookDto.bookName());
                    existing.setBookAuthor(bookDto.bookAuthor());
                    Book updated = bookRepository.save(existing);
                    log.info("Successfully updated book with id: {}", bookId);
                    return LibraryEventMapper.toBookResponseDto(updated);
                });
    }

    @Transactional
    public boolean delete(Integer bookId) {
        log.info("Deleting book with id: {}", bookId);
        return bookRepository.findById(bookId)
                .map(book -> {
                    // Break the bidirectional back-reference before deleting to prevent
                    // cascade from re-persisting the book through LibraryEvent
                    if (book.getLibraryEvent() != null) {
                        book.getLibraryEvent().setBook(null);
                    }
                    bookRepository.delete(book);
                    log.info("Successfully deleted book with id: {}", bookId);
                    return true;
                })
                .orElse(false);
    }
}
