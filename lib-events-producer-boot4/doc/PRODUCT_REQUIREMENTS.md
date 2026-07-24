# Product Requirements Document (PRD)

Project: Library Events Producer (REST → Kafka)
Date: 2026-07-21
Platform: Spring Boot 4.1.0, Java 25

1. Purpose
- Expose REST endpoints to accept library events (ADD, UPDATE) and publish them as JSON messages to Kafka topic "library-events" for downstream processing.

2. Key decisions
- libraryEventId is supplied by upstream clients (POST must include it).
- Service does not persist events; it only publishes to Kafka.
- No SLA or retention requirements declared.
- No dead-letter behavior required.

3. Goals & Success Metrics
- Correctness: events published with expected schema and key.
- Latency: API response time acceptable for stakeholders (no strict SLA).
- Observability: logs and metrics for publish success/failure.
- Test coverage: unit + integration (EmbeddedKafka) tests passing.

4. Stakeholders
- Backend engineers, Kafka operators, downstream consumers, QA.

5. Scope
In-scope:
- POST /api/v1/library-events — create ADD event (client-provided libraryEventId).
- PUT /api/v1/library-events/{libraryEventId} — publish UPDATE event.
- Publish JSON events to Kafka topic "library-events".
- Validation, retries, logging, metrics, tests.

Out-of-scope:
- Persistent event store, DLQ/dead-letter handling, formal SLA/retention tuning, built-in auth.

6. Data model / Event schema
- Book
  - bookId: integer (required)
  - bookName: string (required)
  - bookAuthor: string (required)
- LibraryEvent
  - libraryEventId: long (required; provided by client)
  - eventType: string enum {"ADD","UPDATE"} (required)
  - book: Book (required)
  - timestamp: ISO-8601 string (optional; producer may add if absent)

Example message value:
{
  "libraryEventId": 1001,
  "eventType": "ADD",
  "book": { "bookId": 10, "bookName": "X", "bookAuthor": "Y" },
  "timestamp": "2026-07-21T22:00:00Z"
}

Kafka key: libraryEventId (string/long) — used for partitioning and consumer idempotency.

7. REST API contract
Base path: /api/v1/library-events

- POST /api/v1/library-events
  - Purpose: publish ADD event
  - Request body: LibraryEvent (libraryEventId required, eventType must be "ADD")
  - Responses:
    - 201 Created — event published
    - 400 Bad Request — validation error
    - 503 Service Unavailable — Kafka publish failure after retries

- PUT /api/v1/library-events/{libraryEventId}
  - Purpose: publish UPDATE event
  - Request body: LibraryEvent (eventType should be "UPDATE"; path id and body id must match)
  - Responses:
    - 200 OK — event published
    - 400 Bad Request — validation error or id mismatch
    - 503 Service Unavailable — Kafka publish failure after retries

Validation rules:
- libraryEventId: required, positive integer
- bookId: required, positive integer
- bookName/bookAuthor: non-empty, max length 255
- eventType must be "ADD" or "UPDATE" and consistent with endpoint

8. Kafka integration & delivery
- Topic: library-events (configurable via properties)
- Key: libraryEventId
- Value: JSON (Jackson)
- Recommended producer settings:
  - acks=all
  - retries with backoff (configurable)
  - enable.idempotence=true (recommended to avoid duplicates)
- Delivery semantics:
  - Attempt retries on transient errors.
  - If publish ultimately fails (after configured retries), return 503 to caller. No DLQ required per decision.

9. Error handling
- Validation errors → 400 with details.
- Kafka transient errors → retry; on exhaustion → 503.
- Log structured error with correlation id and libraryEventId.

10. Observability & telemetry
- Structured logs include request-id (correlation), libraryEventId, eventType, Kafka partition/offset on success.
- Metrics:
  - requests_total, requests_success, requests_failure
  - kafka_publish_latency_seconds, kafka_publish_failures_total
- Tracing: optional OpenTelemetry spans for request → publish.

11. Security
- Input validation and sanitization enforced (bookName/bookAuthor length, types).
- Enforce request size limits and payload schema.
- Secure Kafka connection configurable (SSL/SASL) if infra requires.
- No built-in API authentication/authorization in this service.

12. Testing
- Unit tests: controller/service/validation with mocked KafkaTemplate.
- Integration tests: EmbeddedKafka verifying messages, key/value and headers.
- Failure tests: simulate Kafka down to confirm retries and API 503.

13. Deployment & configuration
- Containerized; environment properties:
  - spring.kafka.bootstrap-servers
  - library.events.topic (default "library-events")
  - producer retries, timeouts, idempotence flag
- Stateless; scale horizontally.

14. Acceptance criteria
- POST valid payload → 201 and message appears on "library-events" with key=libraryEventId and matching value (verified by integration test).
- PUT valid payload (matching id) → 200 and UPDATE message published.
- Invalid payloads → 400.
- Simulated Kafka failure → API returns 503 after retry exhaustion; failures are logged and metrics increase.

15. Open questions / Next steps
- Resolved per inputs: client supplies libraryEventId; no persistence; no DLQ; no SLA.
- Recommended next deliverables: OpenAPI specification, implementation skeleton (controller/service), and integration tests.
