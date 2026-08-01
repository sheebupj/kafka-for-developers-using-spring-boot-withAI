Implementation Plan - Layered Tasks (using application.yml)

Project: Library Events Producer (REST → Kafka)
Platform: Spring Boot 4.1.0, Java 25

Overview
- Layer-by-layer detailed tasks mapped to PRD. Configuration uses application.yml as requested.

Tasks

1) Project bootstrap
- Create Maven/Gradle project (Java 25, Spring Boot 4.1.0)
- Add dependencies: spring-boot-starter-web, spring-boot-starter-validation, spring-kafka, micrometer-registry-prometheus, spring-boot-starter-actuator, jackson-databind, spring-kafka-test, junit-jupiter
- Files: pom.xml or build.gradle

2) Configuration (YAML)
- Add src/main/resources/application.yml with env-overridable keys:
  library:
    events:
      topic: library-events
  spring:
    kafka:
      bootstrap-servers: ${SPRING_KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
      producer:
        acks: all
        retries: 5
        properties:
          enable.idempotence: true
        key-serializer: org.apache.kafka.common.serialization.StringSerializer
        value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
- Add configurable retry/backoff properties and timeouts
- Add Spring profile files for environment-specific values:
  - src/main/resources/application-dev.yml
  - src/main/resources/application-stage.yml
  - src/main/resources/application-prod.yml
- Keep only common defaults in application.yml and move differing values (bootstrap servers, DB URLs, log levels, feature flags) into profile files
- Activate profile per runtime:
  - Local dev: SPRING_PROFILES_ACTIVE=dev
  - Stage: SPRING_PROFILES_ACTIVE=stage
  - Production: SPRING_PROFILES_ACTIVE=prod
- For local runs, allow override with CLI: --spring.profiles.active=dev
- Keep secrets out of repo; reference env vars in YAML (example: ${DB_PASSWORD})

3) Domain & DTOs (validation)
- model/Book.java
  - bookId: @NotNull @Positive
  - bookName: @NotBlank @Size(max=255)
  - bookAuthor: @NotBlank @Size(max=255)
- model/LibraryEvent.java
  - libraryEventId: @NotNull @Positive
  - eventType: EventType enum (ADD, UPDATE)
  - book: @Valid @NotNull
  - timestamp: Optional; service may populate if missing
- EventType enum with ADD and UPDATE

4) Controller layer
- controller/LibraryEventsController.java
  - POST /api/v1/library-events -> expects eventType=ADD
  - PUT /api/v1/library-events/{libraryEventId} -> expects eventType=UPDATE and path/body id match
  - Use @Valid and BindingResult to return 400 on validation errors
  - Set response codes: 201 for POST, 200 for PUT

5) Service layer
- service/LibraryEventService.java
  - Semantic validation (id match on PUT), fill timestamp when absent
  - Prepare key (libraryEventId as string) and value (LibraryEvent DTO)
  - Call LibraryEventsProducer to publish

6) Kafka producer & config beans
- kafka/KafkaProducerConfig.java
  - Configure ProducerFactory and KafkaTemplate using properties from application.yml
  - Configure JsonSerializer for value, StringSerializer for key
- kafka/LibraryEventsProducer.java
  - send(String topic, String key, LibraryEvent value) -> CompletableFuture or use KafkaTemplate.send and wait/get with timeout
  - On success: log partition/offset, increment metrics
  - On failure: handle and bubble up a custom exception for ControllerAdvice
  - Include correlation id in logs (MDC)

7) Error handling & retries
- Use producer retries/backoff and enable.idempotence in producer config
- controller/advice/GlobalExceptionHandler.java
  - Handle MethodArgumentNotValidException -> 400 with details
  - Handle ProducerFailureException -> 503 with structured error (correlation id, libraryEventId, message)

8) Observability & logging
- Add Micrometer meters and timers:
  - requests_total, requests_success, requests_failure
  - kafka_publish_latency_seconds, kafka_publish_failures_total
- Use structured JSON logs (include request-id, libraryEventId, eventType)
- Optionally add OpenTelemetry spans around request -> publish

9) API docs: Swagger/OpenAPI
- Add dependency for OpenAPI generation/UI (springdoc starter for WebMVC)
- Add OpenAPI metadata (title, version, description, contact) via @OpenAPIDefinition or bean config
- Annotate endpoints with @Operation and include request/response examples for POST/PUT
- Group/tag Library Events APIs for cleaner Swagger UI navigation
- Enable and configure Swagger UI landing page (title, default expansion, operations sorter)
- Expose and verify:
  - Swagger UI: /swagger-ui/index.html
  - OpenAPI JSON: /v3/api-docs
  - OpenAPI YAML: /v3/api-docs.yaml
- Add quick validation step: open Swagger UI and execute POST/PUT from the browser
- Add build step/command to export spec artifact:
  - curl http://localhost:8080/v3/api-docs > openapi.json
  - curl http://localhost:8080/v3/api-docs.yaml > openapi.yaml
- Store generated spec under docs/api/ (or publish as CI artifact) and keep it in sync with endpoint changes

10) Testing: unit
- Controller tests with MockMvc and mocked LibraryEventService
- Service/Producer unit tests mocking KafkaTemplate to assert send behavior and exception handling
- Validation tests for DTO constraints

11) Testing: integration
- EmbeddedKafka tests verifying message key and value on "library-events" topic
- Tests for success (POST/PUT) and failure (simulate broker down) leading to 503
- Use @SpringBootTest with test-specific application.yml overrides

12) CI/Docker
- Dockerfile for running the app (runtime image: Temurin 25 or Eclipse Temurin JRE)
- GitHub Actions: build -> unit tests -> integration (if feasible) -> build/publish image

13) Acceptance & docs
- small README.md / OpenAPI snippet showing endpoints and example payloads
- Map PRD acceptance criteria to automated test cases

Estimates
- Implementation + unit tests: 2–3 days
- Integration tests + CI: +1 day

Next steps
- Confirm build tool (Maven or Gradle)
- Start implementing chosen tasks (recommend starting with project bootstrap and configuration)

Saved: 2026-07-22
