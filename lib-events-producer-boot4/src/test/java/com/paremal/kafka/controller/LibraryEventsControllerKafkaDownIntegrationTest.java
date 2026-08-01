package com.paremal.kafka.controller;

import com.paremal.kafka.exception.ApiErrorResponse;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.RestTemplate;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=localhost:65530",
        "spring.kafka.producer.acks=all",
        "spring.kafka.producer.retries=0",
        "spring.kafka.producer.properties.enable.idempotence=false",
        "spring.kafka.producer.properties.max.block.ms=1000",
        "spring.kafka.producer.properties.request.timeout.ms=1000",
        "spring.kafka.producer.properties.delivery.timeout.ms=1500",
        "spring.kafka.producer.properties.retry.backoff.ms=100"
})
class LibraryEventsControllerKafkaDownIntegrationTest {

    @LocalServerPort
    private int port;

    @Test
    void postLibraryEvent_whenKafkaUnavailable_returns503() {
        String payload = """
                {
                  "libraryEventId": 11,
                  "eventType": "ADD",
                  "book": {
                    "bookId": 201,
                    "bookName": "Kafka Failure Test",
                    "bookAuthor": "Test Author"
                  }
                }
                """;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(payload, headers);
        RestTemplate restTemplate = new RestTemplate();

        try {
            restTemplate.postForEntity(
                    "http://localhost:" + port + "/api/v1/library-events",
                    request,
                    ApiErrorResponse.class
            );
            assertThat(false).isTrue();
        } catch (HttpServerErrorException e) {
            assertThat(e.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
            assertThat(e.getResponseBodyAsString()).contains("Kafka publish failed");
            assertThat(e.getResponseBodyAsString()).contains("\"field\":\"kafka\"");
        }
    }

    @Test
    void putLibraryEvent_whenKafkaUnavailable_returns503() {
        String payload = """
                {
                  "libraryEventId": 21,
                  "eventType": "UPDATE",
                  "book": {
                    "bookId": 301,
                    "bookName": "Kafka Failure Update Test",
                    "bookAuthor": "Test Author"
                  }
                }
                """;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(payload, headers);
        RestTemplate restTemplate = new RestTemplate();

        try {
            restTemplate.exchange(
                    "http://localhost:" + port + "/api/v1/library-events/21",
                    HttpMethod.PUT,
                    request,
                    ApiErrorResponse.class
            );
            assertThat(false).isTrue();
        } catch (HttpServerErrorException e) {
            assertThat(e.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
            assertThat(e.getResponseBodyAsString()).contains("Kafka publish failed");
            assertThat(e.getResponseBodyAsString()).contains("\"field\":\"kafka\"");
        }
    }
}
