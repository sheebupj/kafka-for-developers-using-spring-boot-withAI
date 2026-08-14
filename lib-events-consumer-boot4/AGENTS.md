# AGENTS.md

## Project Overview

**Spring Boot 4.1 / Java 25** Kafka consumer service.

Listens to the `library-events` Kafka topic, deserializes JSON payloads into typed DTOs,
applies `ADD` / `UPDATE` business logic, persists to **PostgreSQL** via Spring Data JPA,
and routes failed records to a Dead Letter Topic (`library-events.DLT`).
Schema is managed exclusively by **Flyway** — Hibernate DDL auto-management is disabled.

---

## Technology Stack

| Layer | Technology |
|---|---|
| Framework | Spring Boot 4.1.0 |
| Language | Java 25 |
| Messaging | Spring Kafka (Apache Kafka client 4.x) |
| Persistence | Spring Data JPA + Hibernate + PostgreSQL |
| Schema mgmt | Flyway (`ddl-auto: none`) |
| Validation | Jakarta Bean Validation (`spring-boot-starter-validation`) |
| Web | Spring MVC (`spring-boot-starter-webmvc`) |
| JSON | Jackson 3 (`tools.jackson.databind.ObjectMapper`) in app code; Jackson 2 (`com.fasterxml`) in Kafka serializers |
| Test DB | Testcontainers 2.x (`PostgreSQLContainer`) |
| Test Kafka | `@EmbeddedKafka` (in-process broker, no Docker) |

---

## Package Structure

```
com.paremal.kafka
├── config/          LibraryEventsConsumerConfig  — KafkaListenerContainerFactory, DefaultErrorHandler, DLT recoverer
├── consumer/        LibraryEventsConsumer        — @KafkaListener entry point
├── domain/          EventType (enum: ADD, UPDATE)
├── dto/             LibraryEventDto, BookDto     — Java records + bean validation, no JPA
├── entity/          LibraryEvent, Book           — JPA entities with audit callbacks, no Lombok
├── mapper/          LibraryEventMapper           — manual DTO→Entity mapping, no MapStruct
├── repository/      LibraryEventRepository, BookRepository — JpaRepository interfaces
└── service/         LibraryEventService          — @Transactional business logic
```

---

## Architecture & Data Flow

```
Kafka topic "library-events"
  → LibraryEventsConsumer        (@KafkaListener, @Component)
    → LibraryEventService.processEvent(ConsumerRecord<Integer, LibraryEventDto>)
        ├── ADD  → LibraryEventMapper.toEntity(dto) → libraryEventRepository.save()
        └── UPDATE → findById() → LibraryEventMapper.updateEntity(dto, existing) → save()

On failure:
  DefaultErrorHandler (FixedBackOff 1s × 2 retries)
    → DeadLetterPublishingRecoverer → "library-events.DLT"

Non-retryable exceptions (no retry, straight to DLT):
  IllegalArgumentException, DeserializationException, SerializationException
```

---

## Key Design Decisions

### DTO / Entity Separation
- `dto/` — Java `record` types (`LibraryEventDto`, `BookDto`) with `@NotNull`/`@NotBlank` bean validation. **No JPA annotations.**
- `entity/` — JPA `@Entity` classes (`LibraryEvent`, `Book`). **No validation annotations from DTO layer.**
- `LibraryEventMapper` bridges them manually (no MapStruct).

### Primary Keys
| Entity | PK strategy | Reason |
|---|---|---|
| `LibraryEvent.libraryEventId` | `@GeneratedValue(IDENTITY)` — DB-generated | Consumer should not dictate event IDs |
| `Book.bookId` | `@Id` only — producer-assigned | Book ID is meaningful to the producer |

### Bidirectional `@OneToOne` Relationship
- `LibraryEvent.book` is the **inverse** side (`mappedBy = "libraryEvent"`), with `cascade = ALL, orphanRemoval = true`.
- `Book.libraryEvent` is the **owning** side (`@JoinColumn(name = "library_event_id")`).
- On persist: save `LibraryEvent` first to get the IDENTITY-generated PK, then set the FK on `Book` and save.

### Kafka Deserialization
`application.yml` uses `ErrorHandlingDeserializer` wrapping `JsonDeserializer`:
```yaml
spring.kafka.consumer:
  key-deserializer: ErrorHandlingDeserializer   # delegates to IntegerDeserializer
  value-deserializer: ErrorHandlingDeserializer  # delegates to JsonDeserializer
  properties:
    spring.json.use.type.headers: false
    spring.json.value.default.type: com.paremal.kafka.dto.LibraryEventDto
```
Type headers from the producer are ignored; messages always deserialize to `LibraryEventDto`.

### Error Handling
Configured in `LibraryEventsConsumerConfig`:
- **Retryable** (default): any transient exception → 2 retries with 1 s fixed back-off.
- **Non-retryable**: `IllegalArgumentException`, `DeserializationException`, `SerializationException` → skips retries, publishes to `library-events.DLT`.
- `setCommitRecovered(true)` ensures the offset is committed even for non-retryable failures.

### Audit Columns
`createdAt` and `updatedAt` on both `LibraryEvent` and `Book` are populated via JPA lifecycle callbacks (`@PrePersist`, `@PreUpdate`). Never set them in constructors or service code.

---

## Database Schema (Flyway Migrations)

| Migration | Table(s) | Description |
|---|---|---|
| `V1__init_schema.sql` | `library_event`, `book` | Core tables with FK |
| `V2__add_audit_columns.sql` | both | `created_at`, `updated_at` |
| `V3__create_library_event_failure_table.sql` | `library_event_failure` | Dead-letter record store |

**Rule:** Never modify existing migrations. New schema changes → new `V{N}__description.sql`.
Never set `spring.jpa.hibernate.ddl-auto` to anything other than `none`.

---

## Build & Run Commands

```bash
# Compile + all tests (Docker must be running for Testcontainers)
./gradlew build

# Tests only
./gradlew test

# Run a specific test class
./gradlew test --tests "com.paremal.kafka.service.LibraryEventsServiceIntegrationTest"

# Run the application locally (needs Kafka + Postgres via compose.yaml)
./gradlew bootRun

# Start PostgreSQL for local dev
docker compose up -d postgres
```

---

## Testing Strategy

### Test Infrastructure
| Component | Approach |
|---|---|
| PostgreSQL | Testcontainers 2.x — `@ImportTestcontainers` + `static @ServiceConnection PostgreSQLContainer<?>` |
| Kafka (consumer tests) | `@EmbeddedKafka` — in-process, no Docker |
| Kafka (service tests) | `@EmbeddedKafka` — embedded broker lets the listener start without interfering with direct `processEvent()` calls |
| Mocks | **None** — all tests run against real infrastructure |

### Test Categories

**Unit tests** (`service/`, `consumer/`) — no Spring context, use Mockito only where unavoidable:
- `LibraryEventServiceTest` — validates ADD/UPDATE branching, null payload, missing book, missing ID.
- `LibraryEventsConsumerTest` — verifies `onMessage()` delegates to `libraryEventService`.

**Integration tests** (`consumer/`, `service/`):
- `LibraryEventsConsumerIntegrationTest` — produces to `@EmbeddedKafka`, waits for async persist, asserts DB rows with FK and audit fields.
- `LibraryEventsServiceIntegrationTest` — calls `libraryEventService.processEvent()` directly (no Kafka produce), asserts full DB state.

### Key Test Patterns
```java
// Container per test class (static field, Spring Boot 4 pattern)
@ServiceConnection
static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:latest");

// Cleanup — always @BeforeEach, child entity first (FK constraint)
bookRepository.deleteAll();
libraryEventRepository.deleteAll();

// Build ConsumerRecord without a broker (service integration tests)
private ConsumerRecord<Integer, LibraryEventDto> buildConsumerRecord(Integer key, LibraryEventDto value) {
    return new ConsumerRecord<>("library-events", 0, 0L, key, value);
}

// Poll DB until async consumer persists (consumer integration tests)
private void waitForRecordCount(long expected, int timeoutSeconds) throws InterruptedException { ... }
```

### Test Configuration (`src/test/resources/application.yml`)
> **Important:** In Spring Boot 4, `src/test/resources/application.yml` **replaces** (does not merge with) `src/main/resources/application.yml`. The test yml must therefore repeat the full Kafka consumer config.

Key overrides:
- `server.port: 0` — random port
- `spring.flyway.clean-disabled: false` — allow Flyway clean in tests
- `spring.jpa.hibernate.ddl-auto: none` — Flyway owns schema
- Full `spring.kafka.consumer.*` block — required because test yml replaces main yml
- No `spring.datasource.*` — `@ServiceConnection` injects the Testcontainer JDBC URL

### JVM Timezone
```groovy
// build.gradle — required: PostgreSQL 17 rejects deprecated "Asia/Calcutta" timezone
tasks.named('test') {
    useJUnitPlatform()
    jvmArgs = ['-Duser.timezone=UTC']
}
```

---

## Key Conventions

| Convention | Rule |
|---|---|
| **No Lombok** | Entities use explicit getters/setters; DTOs are Java `record` |
| **No MapStruct** | `LibraryEventMapper` does manual field mapping |
| **No `@Autowired` on fields** | Constructor injection only |
| **No mocks in integration tests** | Real Testcontainers DB + embedded Kafka |
| **Logging** | `private static final Logger log = LoggerFactory.getLogger(...)` |
| **Jackson** | App code uses Jackson 3 (`tools.jackson.databind`); Kafka serializers use Jackson 2 (`com.fasterxml.jackson`) |
| **Enum** | `EventType` (not `LibraryEventType`) — `ADD`, `UPDATE` |
| **Test naming** | `{method}_{scenario}_{expectedBehavior}()` e.g. `processEvent_ADD_shouldPersistLibraryEventAndBook` |
| **Assertions** | JUnit Jupiter only (`assertEquals`, `assertNotNull`) — no AssertJ or Hamcrest |

---

## Reference Docs (`docs/`)

| File | Topic |
|---|---|
| `1_PRD.md` | Product requirements |
| `2_IMPLEMENTATION_PLAN.md` | Step-by-step build roadmap |
| `3_Kafka_Consumer_Under_the_hood.md` | Kafka poll loop, listener dispatch internals |
| `4_STRING_VS_JSON_DESERIALIZER.md` | Deserializer strategy decision |
| `5_CONSUMER_CONCEPTS_HANDS_ON.md` | Consumer groups, offset management |
| `6_ONETOONE_MAPPING_EXPLAINED.md` | JPA bidirectional OneToOne details |
| `7_INTEGRATION_TESTING_WITH_TESTCONTAINERS.md` | Testcontainers 2.x setup |
| `8_FLYWAY_SCHEMA_MANAGEMENT.md` | Flyway conventions and migration rules |
| `9_KAFKA_ERROR_HANDLING.md` | Retry, DLT, and recovery strategy |
| `REST_API.md` | REST endpoint contracts |

