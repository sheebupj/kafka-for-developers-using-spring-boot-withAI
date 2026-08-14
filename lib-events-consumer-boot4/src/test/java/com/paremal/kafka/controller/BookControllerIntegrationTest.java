package com.paremal.kafka.controller;

import com.paremal.kafka.dto.BookDto;
import com.paremal.kafka.entity.Book;
import com.paremal.kafka.entity.LibraryEvent;
import com.paremal.kafka.domain.EventType;
import com.paremal.kafka.repository.BookRepository;
import com.paremal.kafka.repository.LibraryEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.PostgreSQLContainer;
import tools.jackson.databind.ObjectMapper;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@EmbeddedKafka(
        partitions = 1,
        topics = {"library-events"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@TestPropertySource(properties = {"spring.kafka.consumer.auto-offset-reset=earliest"})
@ImportTestcontainers
class BookControllerIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();
    }

    // ── GET all ──────────────────────────────────────────────

    @Test
    void getAllBooks_shouldReturnEmptyList() throws Exception {
        mockMvc.perform(get("/v1/books"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.length()").value(0));
    }

    @Test
    void getAllBooks_shouldReturnAllBooks() throws Exception {
        persistBookWithLibraryEvent(1, "Clean Code", "Robert C. Martin");
        persistBookWithLibraryEvent(2, "Effective Java", "Joshua Bloch");

        mockMvc.perform(get("/v1/books"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.length()").value(2));
    }

    // ── GET by ID ────────────────────────────────────────────

    @Test
    void getBookById_shouldReturnBook() throws Exception {
        persistBookWithLibraryEvent(1, "Clean Code", "Robert C. Martin");

        mockMvc.perform(get("/v1/books/1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bookId").value(1))
                .andExpect(jsonPath("$.bookName").value("Clean Code"))
                .andExpect(jsonPath("$.bookAuthor").value("Robert C. Martin"))
                .andExpect(jsonPath("$.libraryEventId").isNotEmpty())
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());
    }

    @Test
    void getBookById_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(get("/v1/books/999"))
                .andExpect(status().isNotFound());
    }

    // ── POST (create) ────────────────────────────────────────

    @Test
    void createBook_shouldPersistAndReturn201() throws Exception {
        BookDto bookDto = new BookDto(10, "Domain-Driven Design", "Eric Evans");

        mockMvc.perform(post("/v1/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(bookDto)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.bookId").value(10))
                .andExpect(jsonPath("$.bookName").value("Domain-Driven Design"))
                .andExpect(jsonPath("$.bookAuthor").value("Eric Evans"))
                .andExpect(jsonPath("$.libraryEventId").isEmpty())
                .andExpect(jsonPath("$.createdAt").isNotEmpty())
                .andExpect(jsonPath("$.updatedAt").isNotEmpty());
    }

    @Test
    void createBook_invalidPayload_shouldReturn400() throws Exception {
        BookDto invalidDto = new BookDto(null, "", "");

        mockMvc.perform(post("/v1/books")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(invalidDto)))
                .andExpect(status().isBadRequest());
    }

    // ── PUT (update) ─────────────────────────────────────────

    @Test
    void updateBook_shouldUpdateAndReturn200() throws Exception {
        persistBookWithLibraryEvent(5, "Old Title", "Old Author");
        BookDto updateDto = new BookDto(5, "New Title", "New Author");

        mockMvc.perform(put("/v1/books/5")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bookId").value(5))
                .andExpect(jsonPath("$.bookName").value("New Title"))
                .andExpect(jsonPath("$.bookAuthor").value("New Author"));
    }

    @Test
    void updateBook_notFound_shouldReturn404() throws Exception {
        BookDto updateDto = new BookDto(999, "Non-existent", "Nobody");

        mockMvc.perform(put("/v1/books/999")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(updateDto)))
                .andExpect(status().isNotFound());
    }

    // ── DELETE ────────────────────────────────────────────────

    @Test
    void deleteBook_shouldDeleteAndReturn204() throws Exception {
        persistBookWithLibraryEvent(7, "Kafka: The Definitive Guide", "Neha Narkhede");

        mockMvc.perform(delete("/v1/books/7"))
                .andExpect(status().isNoContent());
    }

    @Test
    void deleteBook_notFound_shouldReturn404() throws Exception {
        mockMvc.perform(delete("/v1/books/999"))
                .andExpect(status().isNotFound());
    }

    // ── Helper ────────────────────────────────────────────────

    private void persistBookWithLibraryEvent(Integer bookId, String bookName, String bookAuthor) {
        LibraryEvent libraryEvent = new LibraryEvent(null, EventType.ADD, null);
        LibraryEvent savedEvent = libraryEventRepository.save(libraryEvent);

        Book book = new Book(bookId, bookName, bookAuthor);
        book.setLibraryEvent(savedEvent);
        bookRepository.save(book);
    }
}
