package com.paremal.kafka.service;

import com.paremal.kafka.domain.EventType;
import com.paremal.kafka.dto.BookDto;
import com.paremal.kafka.dto.LibraryEventDto;
import com.paremal.kafka.entity.Book;
import com.paremal.kafka.entity.LibraryEvent;
import com.paremal.kafka.mapper.LibraryEventMapper;
import com.paremal.kafka.repository.LibraryEventRepository;
import jakarta.validation.Validation;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LibraryEventServiceTest {

    @Mock
    private LibraryEventRepository libraryEventRepository;

    @Captor
    private ArgumentCaptor<LibraryEvent> libraryEventCaptor;

    private LibraryEventService libraryEventService;

    @BeforeEach
    void setUp() {
        var validator = Validation.buildDefaultValidatorFactory().getValidator();
        libraryEventService = new LibraryEventService(libraryEventRepository, new LibraryEventMapper(), validator);
    }

    @Test
    void processAddEventSavesLibraryEventAndBook() {
        var consumerRecord = new ConsumerRecord<>(
                "library-events",
                0,
                0L,
                1,
                new LibraryEventDto(1, EventType.ADD, new BookDto(123, "Kafka", "Dilip")));

        libraryEventService.processEvent(consumerRecord);

        verify(libraryEventRepository).save(libraryEventCaptor.capture());
        assertEquals(EventType.ADD, libraryEventCaptor.getValue().getEventType());
        assertEquals(123, libraryEventCaptor.getValue().getBook().getBookId());
        assertEquals("Kafka", libraryEventCaptor.getValue().getBook().getBookName());
    }

    @Test
    void processUpdateEventUpdatesExistingEntity() {
        var existing = new LibraryEvent();
        existing.setLibraryEventId(1);
        existing.setEventType(EventType.ADD);
        var existingBook = new Book();
        existingBook.setBookId(123);
        existingBook.setBookName("Old Name");
        existingBook.setBookAuthor("Old Author");
        existingBook.setLibraryEvent(existing);
        existing.setBook(existingBook);
        when(libraryEventRepository.findById(1)).thenReturn(Optional.of(existing));
        when(libraryEventRepository.save(any(LibraryEvent.class))).thenAnswer(invocation -> invocation.getArgument(0));

        var consumerRecord = new ConsumerRecord<>(
                "library-events",
                0,
                0L,
                1,
                new LibraryEventDto(1, EventType.UPDATE, new BookDto(123, "New Name", "New Author")));

        libraryEventService.processEvent(consumerRecord);

        verify(libraryEventRepository).save(libraryEventCaptor.capture());
        assertEquals(EventType.UPDATE, libraryEventCaptor.getValue().getEventType());
        assertEquals("New Name", libraryEventCaptor.getValue().getBook().getBookName());
        assertEquals("New Author", libraryEventCaptor.getValue().getBook().getBookAuthor());
    }

    @Test
    void processUpdateEventRequiresLibraryEventId() {
        var consumerRecord = new ConsumerRecord<>(
                "library-events",
                0,
                0L,
                1,
                new LibraryEventDto(null, EventType.UPDATE, new BookDto(123, "Kafka", "Dilip")));

        var exception = assertThrows(IllegalArgumentException.class, () -> libraryEventService.processEvent(consumerRecord));
        assertEquals("libraryEventId is required for UPDATE events", exception.getMessage());
    }

    @Test
    void processUpdateEventRejectsMissingEntity() {
        when(libraryEventRepository.findById(99)).thenReturn(Optional.empty());

        var consumerRecord = new ConsumerRecord<>(
                "library-events",
                0,
                0L,
                1,
                new LibraryEventDto(99, EventType.UPDATE, new BookDto(123, "Kafka", "Dilip")));

        var exception = assertThrows(IllegalArgumentException.class, () -> libraryEventService.processEvent(consumerRecord));
        assertEquals("LibraryEvent not found for libraryEventId=99", exception.getMessage());
    }

    @Test
    void processEventRejectsNullPayload() {
        var consumerRecord = new ConsumerRecord<Integer, LibraryEventDto>("library-events", 0, 0L, 1, null);

        var exception = assertThrows(IllegalArgumentException.class, () -> libraryEventService.processEvent(consumerRecord));
        assertEquals("libraryEvent payload cannot be null", exception.getMessage());
    }

    @Test
    void processEventRejectsMissingBook() {
        var consumerRecord = new ConsumerRecord<>(
                "library-events",
                0,
                0L,
                1,
                new LibraryEventDto(1, EventType.ADD, null));

        var exception = assertThrows(IllegalArgumentException.class, () -> libraryEventService.processEvent(consumerRecord));
        assertEquals("Validation failed: book book is required", exception.getMessage());
    }
}
