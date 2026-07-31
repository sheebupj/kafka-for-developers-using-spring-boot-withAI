package com.paremal.kafka.controller;

import com.paremal.kafka.model.EventType;
import com.paremal.kafka.model.LibraryEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1)
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
class LibraryEventsControllerIntegrationTest {

    @LocalServerPort
    private int port;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    private RestTemplate restTemplate;
    private String baseUrl;

    private KafkaConsumer<String, String> kafkaConsumer;

    @BeforeEach
    void setUp() {
        restTemplate = new RestTemplate();
        baseUrl = "http://localhost:" + port;

        String bootstrapServers = embeddedKafkaBroker.getBrokersAsString();

        Map<String, Object> consumerProps = new HashMap<>();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        consumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);

        kafkaConsumer = new KafkaConsumer<>(consumerProps);
        kafkaConsumer.subscribe(Collections.singletonList("library-events"));
    }

    @AfterEach
    void tearDown() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
    }

    @Test
    void postLibraryEvent_success_returns201AndPublishesToKafka() {
        // Given
        String payload = """
                {
                  "libraryEventId": 1,
                  "eventType": "ADD",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(payload, headers);

        // When
        ResponseEntity<LibraryEvent> response = restTemplate.postForEntity(
                baseUrl + "/api/v1/library-events",
                request,
                LibraryEvent.class
        );

        // Then - HTTP response
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getLibraryEventId()).isEqualTo(1L);
        assertThat(response.getBody().getEventType()).isEqualTo(EventType.ADD);

        // Then - Kafka message
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
        assertThat(records).isNotEmpty();

        var record = records.iterator().next();
        assertThat(record.key()).isEqualTo("1");
        // Verify message was published with expected content
        assertThat(record.value())
                .contains("\"libraryEventId\":1")
                .contains("\"eventType\":\"ADD\"")
                .contains("\"bookId\":123");
    }

    @Test
    void postLibraryEvent_invalidEventType_returns400() {
        // Given
        String payload = """
                {
                  "libraryEventId": 2,
                  "eventType": "UPDATE",
                  "book": {
                    "bookId": 456,
                    "bookName": "Spring in Action",
                    "bookAuthor": "Craig Walls"
                  }
                }
                """;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(payload, headers);

        // When & Then
        try {
            restTemplate.postForEntity(
                    baseUrl + "/api/v1/library-events",
                    request,
                    String.class
            );
            // If we get here, test failed - should have thrown exception
            assertThat(false).isTrue();
        } catch (HttpClientErrorException e) {
            // Then
            assertThat(e.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(e.getResponseBodyAsString()).contains("eventType must be ADD for POST endpoint");
        }
    }

    @Test
    void postLibraryEvent_invalidBook_returns400() {
        // Given
        String payload = """
                {
                  "libraryEventId": 3,
                  "eventType": "ADD",
                  "book": {
                    "bookId": 789,
                    "bookName": "",
                    "bookAuthor": "Unknown"
                  }
                }
                """;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(payload, headers);

        // When & Then
        try {
            restTemplate.postForEntity(
                    baseUrl + "/api/v1/library-events",
                    request,
                    String.class
            );
            // If we get here, test failed - should have thrown exception
            assertThat(false).isTrue();
        } catch (HttpClientErrorException e) {
            // Then
            assertThat(e.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
            assertThat(e.getResponseBodyAsString()).contains("bookName");
        }
    }
}













