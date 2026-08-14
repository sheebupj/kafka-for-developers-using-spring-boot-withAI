package com.paremal.kafka.service;

import com.paremal.kafka.domain.EventType;
import com.paremal.kafka.dto.BookDto;
import com.paremal.kafka.dto.LibraryEventDto;
import com.paremal.kafka.entity.Book;
import com.paremal.kafka.entity.LibraryEvent;
import com.paremal.kafka.repository.BookRepository;
import com.paremal.kafka.repository.LibraryEventRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"library-events"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@TestPropertySource(properties = {"spring.kafka.consumer.auto-offset-reset=earliest"})
@ImportTestcontainers
class LibraryEventsServiceIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private LibraryEventService libraryEventService;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    @Autowired
    private BookRepository bookRepository;

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();
    }

    @Test
    void processEvent_ADD_shouldPersistLibraryEventAndBook() {
        // given
        var bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
        var dto = new LibraryEventDto(null, EventType.ADD, bookDto);
        var record = buildConsumerRecord(null, dto);

        // when
        libraryEventService.processEvent(record);

        // then
        assertEquals(1, libraryEventRepository.count());
        assertEquals(1, bookRepository.count());

        List<LibraryEvent> events = libraryEventRepository.findAll();
        LibraryEvent savedEvent = events.get(0);
        assertNotNull(savedEvent.getLibraryEventId());
        assertEquals(EventType.ADD, savedEvent.getEventType());
        assertNotNull(savedEvent.getCreatedAt());
        assertNotNull(savedEvent.getUpdatedAt());

        Book savedBook = savedEvent.getBook();
        assertNotNull(savedBook);
        assertEquals(1, savedBook.getBookId());
        assertEquals("Clean Code", savedBook.getBookName());
        assertEquals("Robert C. Martin", savedBook.getBookAuthor());
        assertEquals(savedEvent.getLibraryEventId(), savedBook.getLibraryEvent().getLibraryEventId());
        assertNotNull(savedBook.getCreatedAt());
        assertNotNull(savedBook.getUpdatedAt());
    }

    @Test
    void processEvent_UPDATE_shouldUpdateExistingLibraryEventAndBook() {
        // given — seed an existing ADD event
        var addDto = new LibraryEventDto(null, EventType.ADD, new BookDto(2, "Old Name", "Old Author"));
        libraryEventService.processEvent(buildConsumerRecord(null, addDto));

        Integer savedId = libraryEventRepository.findAll().get(0).getLibraryEventId();

        // when — process UPDATE for the same id
        var updateDto = new LibraryEventDto(savedId, EventType.UPDATE, new BookDto(2, "New Name", "New Author"));
        libraryEventService.processEvent(buildConsumerRecord(savedId, updateDto));

        // then
        assertEquals(1, libraryEventRepository.count());
        LibraryEvent updatedEvent = libraryEventRepository.findById(savedId).orElseThrow();
        assertEquals(EventType.UPDATE, updatedEvent.getEventType());
        assertEquals("New Name", updatedEvent.getBook().getBookName());
        assertEquals("New Author", updatedEvent.getBook().getBookAuthor());
    }

    @Test
    void processEvent_UPDATE_withNonExistentId_shouldThrowIllegalArgumentException() {
        // given
        var dto = new LibraryEventDto(999, EventType.UPDATE, new BookDto(1, "Kafka", "Dilip"));
        var record = buildConsumerRecord(999, dto);

        // when / then
        var exception = assertThrows(IllegalArgumentException.class,
                () -> libraryEventService.processEvent(record));
        assertEquals("LibraryEvent not found for libraryEventId=999", exception.getMessage());
    }

    @Test
    void processEvent_UPDATE_withNullLibraryEventId_shouldThrowIllegalArgumentException() {
        // given
        var dto = new LibraryEventDto(null, EventType.UPDATE, new BookDto(1, "Kafka", "Dilip"));
        var record = buildConsumerRecord(null, dto);

        // when / then
        var exception = assertThrows(IllegalArgumentException.class,
                () -> libraryEventService.processEvent(record));
        assertEquals("libraryEventId is required for UPDATE events", exception.getMessage());
    }

    private ConsumerRecord<Integer, LibraryEventDto> buildConsumerRecord(Integer key, LibraryEventDto value) {
        return new ConsumerRecord<>(
                "library-events",
                0,
                0L,
                key,
                value
        );
    }
}
