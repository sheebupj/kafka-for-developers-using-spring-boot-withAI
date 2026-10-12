package com.paremal.kafka.consumer;

import com.paremal.kafka.domain.EventType;
import com.paremal.kafka.dto.BookDto;
import com.paremal.kafka.dto.LibraryEventDto;
import com.paremal.kafka.entity.Book;
import com.paremal.kafka.entity.LibraryEvent;
import com.paremal.kafka.repository.BookRepository;
import com.paremal.kafka.repository.LibraryEventFailureRepository;
import com.paremal.kafka.repository.LibraryEventRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.context.ImportTestcontainers;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"library-events", "library-events.DLT"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
@TestPropertySource(properties = {
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "app.kafka.recovery.mode=both"
})
@ImportTestcontainers
class LibraryEventsConsumerIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private LibraryEventRepository libraryEventRepository;

    @Autowired
    private BookRepository bookRepository;

    @Autowired
    private LibraryEventFailureRepository failureRepository;

    @Value("${app.kafka.recovery.mode}")
    private String recoveryMode;

    private KafkaTemplate<Integer, LibraryEventDto> kafkaTemplate;

    @BeforeEach
    void setUp() {
        bookRepository.deleteAll();
        libraryEventRepository.deleteAll();
        failureRepository.deleteAll();
        kafkaTemplate = buildKafkaTemplate();
    }

    @Test
    void consumeLibraryEvent_ADD_shouldPersistLibraryEventAndBook() throws Exception {
        // given
        var bookDto = new BookDto(1, "Clean Code", "Robert C. Martin");
        var dto = new LibraryEventDto(null, EventType.ADD, bookDto);

        // when — produce to embedded Kafka
        kafkaTemplate.send("library-events", dto).get(10, TimeUnit.SECONDS);

        // then — wait for async consumer to persist
        waitForRecordCount(1, 10);

        List<LibraryEvent> events = libraryEventRepository.findAll();
        assertEquals(1, events.size());

        LibraryEvent savedEvent = events.get(0);
        assertNotNull(savedEvent.getLibraryEventId());
        assertEquals(EventType.ADD, savedEvent.getEventType());
        assertNotNull(savedEvent.getCreatedAt());
        assertNotNull(savedEvent.getUpdatedAt());

        List<Book> books = bookRepository.findAll();
        assertEquals(1, books.size());

        Book savedBook = books.get(0);
        assertEquals(1, savedBook.getBookId());
        assertEquals("Clean Code", savedBook.getBookName());
        assertEquals("Robert C. Martin", savedBook.getBookAuthor());
        assertEquals(savedEvent.getLibraryEventId(), savedBook.getLibraryEvent().getLibraryEventId());
        assertNotNull(savedBook.getCreatedAt());
        assertNotNull(savedBook.getUpdatedAt());
    }

    @Test
    void consumeLibraryEvent_UPDATE_shouldUpdateExistingLibraryEvent() throws Exception {
        // given — persist an initial ADD event
        var addDto = new LibraryEventDto(null, EventType.ADD, new BookDto(2, "Old Book Name", "Old Author"));
        kafkaTemplate.send("library-events", addDto).get(10, TimeUnit.SECONDS);
        waitForRecordCount(1, 10);

        Integer savedId = libraryEventRepository.findAll().get(0).getLibraryEventId();

        // when — send UPDATE for the same libraryEventId
        var updateDto = new LibraryEventDto(savedId, EventType.UPDATE, new BookDto(2, "New Book Name", "New Author"));
        kafkaTemplate.send("library-events", updateDto).get(10, TimeUnit.SECONDS);

        // wait for the update to be processed (still 1 record)
        Thread.sleep(3_000L);

        // then
        LibraryEvent updatedEvent = libraryEventRepository.findById(savedId).orElseThrow();
        assertEquals(EventType.UPDATE, updatedEvent.getEventType());
        assertEquals("New Book Name", updatedEvent.getBook().getBookName());
        assertEquals("New Author", updatedEvent.getBook().getBookAuthor());
        assertNotNull(updatedEvent.getUpdatedAt());
    }

    @Test
    void consumeLibraryEvent_UPDATE_withoutLibraryEventId_shouldPublishToDltAndPersistFailure() throws Exception {
        assertEquals("both", recoveryMode);
        String uniqueBookName = "failed-update-" + UUID.randomUUID();
        var updateDto = new LibraryEventDto(
                null, EventType.UPDATE, new BookDto(3, uniqueBookName, "Test Author"));

        try (var dltConsumer = buildDltConsumer()) {
            dltConsumer.subscribe(List.of("library-events.DLT"));
            long assignmentDeadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);
            while (dltConsumer.assignment().isEmpty() && System.currentTimeMillis() < assignmentDeadline) {
                dltConsumer.poll(Duration.ofMillis(100));
            }
            assertFalse(dltConsumer.assignment().isEmpty(), "Timed out waiting for DLT consumer assignment");

            kafkaTemplate.send("library-events", updateDto).get(10, TimeUnit.SECONDS);

            ConsumerRecord<Integer, String> deadLetterRecord = null;
            long dltDeadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(15);
            while (deadLetterRecord == null && System.currentTimeMillis() < dltDeadline) {
                var records = dltConsumer.poll(Duration.ofMillis(500));
                for (var record : records) {
                    if (record.value() != null && record.value().contains(uniqueBookName)) {
                        deadLetterRecord = record;
                        break;
                    }
                }
            }

            assertNotNull(deadLetterRecord, "Expected failed update to be published to the DLT");
            assertEquals("library-events.DLT", deadLetterRecord.topic());
            assertTrue(deadLetterRecord.value().contains("\"eventType\":\"UPDATE\""));
            assertTrue(deadLetterRecord.value().contains(uniqueBookName));
        }

        var failures = failureRepository.findAll();
        assertEquals(1, failures.size());
        var failure = failures.getFirst();
        assertEquals("library-events", failure.getTopic());
        assertEquals(IllegalArgumentException.class.getName(), failure.getRootCauseClass());
        assertEquals("libraryEventId is required for UPDATE events", failure.getRootCauseMessage());
        assertTrue(failure.getPayload().contains(uniqueBookName));
    }

    private KafkaTemplate<Integer, LibraryEventDto> buildKafkaTemplate() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(configs));
    }

    private KafkaConsumer<Integer, String> buildDltConsumer() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "dlt-test-" + UUID.randomUUID());
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(configs);
    }

    private void waitForRecordCount(long expectedCount, int timeoutSeconds) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1_000L;
        while (System.currentTimeMillis() < deadline) {
            if (libraryEventRepository.count() >= expectedCount) {
                return;
            }
            Thread.sleep(500);
        }
        assertEquals(expectedCount, libraryEventRepository.count(),
                "Timed out waiting for " + expectedCount + " record(s) to be persisted");
    }
}
