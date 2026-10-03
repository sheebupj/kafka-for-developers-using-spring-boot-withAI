package com.paremal.kafka.config;

import com.paremal.kafka.repository.LibraryEventFailureRepository;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        topics = {"library-events"}
)
@TestPropertySource(properties = {"app.kafka.recovery.mode=failure-table"})
@ImportTestcontainers
class FailureTableRecovererIntegrationTest {

    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

    @Autowired
    private FailureTableRecoverer failureTableRecoverer;

    @Autowired
    private LibraryEventFailureRepository failureRepository;

    @BeforeEach
    void setUp() {
        failureRepository.deleteAll();
    }

    @Test
    void accept_shouldPersistCompleteFailureDetails() {
        var record = new ConsumerRecord<>("library-events", 1, 42L, 7, "payload-json");
        record.headers().add("trace", "abc".getBytes());
        var exception = new IllegalStateException("outer", new IllegalArgumentException("root cause"));

        failureTableRecoverer.accept(record, exception);

        var failures = failureRepository.findAll();
        assertEquals(1, failures.size());
        var failure = failures.getFirst();
        assertNotNull(failure.getFailureId());
        assertEquals("library-events", failure.getTopic());
        assertEquals(1, failure.getPartitionId());
        assertEquals(42L, failure.getOffsetValue());
        assertEquals("7", failure.getRecordKey());
        assertEquals("payload-json", failure.getPayload());
        assertEquals(IllegalStateException.class.getName(), failure.getExceptionClass());
        assertEquals("outer", failure.getExceptionMessage());
        assertEquals(IllegalArgumentException.class.getName(), failure.getRootCauseClass());
        assertEquals("root cause", failure.getRootCauseMessage());
        assertTrue(failure.getStackTrace().contains("IllegalStateException"));
        assertEquals("trace=abc", failure.getRecordHeaders());
        assertNotNull(failure.getFailedAt());
    }
}
