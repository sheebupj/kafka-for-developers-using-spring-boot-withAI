# Quick Reference - Validation Error Testing

## Error Response Format

```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "fieldName",
      "message": "Error message describing what's wrong"
    }
  ]
}
```

---

## Common Test Scenarios

### 1️⃣ Missing Required Field
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1, "eventType": "ADD", "book": {"bookId": 101, "bookName": "Title"}}'
```
**Error:** `"bookAuthor is required"`

---

### 2️⃣ Invalid Value (Not Positive)
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 0, "eventType": "ADD", "book": {"bookId": 101, "bookName": "Title", "bookAuthor": "Author"}}'
```
**Error:** `"libraryEventId must be a positive number"`

---

### 3️⃣ Blank Value
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1, "eventType": "ADD", "book": {"bookId": 101, "bookName": "", "bookAuthor": "Author"}}'
```
**Error:** `"bookName is required"`

---

### 4️⃣ Exceeds Max Length
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1, "eventType": "ADD", "book": {"bookId": 101, "bookName": "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat duis aute irure dolor in reprehenderit in voluptate velit esse", "bookAuthor": "Author"}}'
```
**Error:** `"bookName cannot exceed 255 characters"`

---

### 5️⃣ Multiple Validation Errors
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1, "book": {}}'
```
**Errors:** 
- `"eventType is required"`
- `"book.bookId is required"`
- `"book.bookName is required"`
- `"book.bookAuthor is required"`

---

### 6️⃣ Wrong EventType for Endpoint
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1, "eventType": "UPDATE", "book": {"bookId": 101, "bookName": "Title", "bookAuthor": "Author"}}'
```
**Error:** `"eventType must be ADD for POST endpoint"`

---

### 7️⃣ Path ID Mismatch (PUT Request)
```bash
curl -X PUT http://localhost:8080/api/v1/library-events/50 \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1, "eventType": "UPDATE", "book": {"bookId": 101, "bookName": "Title", "bookAuthor": "Author"}}'
```
**Error:** `"Path libraryEventId must match body.libraryEventId"`

---

### 8️⃣ Wrong EventType for PUT Endpoint
```bash
curl -X PUT http://localhost:8080/api/v1/library-events/1 \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1, "eventType": "ADD", "book": {"bookId": 101, "bookName": "Title", "bookAuthor": "Author"}}'
```
**Error:** `"eventType must be UPDATE for PUT endpoint"`

---

## ✅ Valid Request Examples

### POST - Add Event
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1,
    "eventType": "ADD",
    "book": {
      "bookId": 101,
      "bookName": "Spring in Action",
      "bookAuthor": "Craig Walls"
    }
  }'
```
**Status:** 201 Created

---

### PUT - Update Event
```bash
curl -X PUT http://localhost:8080/api/v1/library-events/1 \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1,
    "eventType": "UPDATE",
    "book": {
      "bookId": 101,
      "bookName": "Spring in Action Updated",
      "bookAuthor": "Craig Walls"
    }
  }'
```
**Status:** 200 OK

---

## All Validation Constraints

### LibraryEvent Fields
| Field | Required | Constraints | Error Messages |
|-------|----------|-------------|-----------------|
| `libraryEventId` | ✅ Yes | Must be positive number | "libraryEventId is required", "libraryEventId must be a positive number" |
| `eventType` | ✅ Yes | Must be ADD or UPDATE | "eventType is required" |
| `book` | ✅ Yes | Valid Book object | "book is required" |
| `timestamp` | ❌ No | ISO 8601 format | - |

### Book Fields
| Field | Required | Constraints | Error Messages |
|-------|----------|-------------|-----------------|
| `bookId` | ✅ Yes | Must be positive number | "bookId is required", "bookId must be a positive number" |
| `bookName` | ✅ Yes | Non-blank, max 255 chars | "bookName is required", "bookName cannot exceed 255 characters" |
| `bookAuthor` | ✅ Yes | Non-blank, max 255 chars | "bookAuthor is required", "bookAuthor cannot exceed 255 characters" |

---

## Testing Tips

### 1. Use pretty-printed JSON
```bash
curl ... | jq '.'
```

### 2. See response headers
```bash
curl -i ...
```

### 3. See full request/response
```bash
curl -v ...
```

### 4. Save request to file
```bash
cat > request.json << 'EOF'
{
  "libraryEventId": 1,
  "eventType": "ADD",
  "book": {...}
}
EOF

curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d @request.json
```

---

## Expected HTTP Status Codes

| Scenario | Status Code |
|----------|-------------|
| Validation error | **400 Bad Request** |
| Invalid JSON syntax | **400 Bad Request** |
| Missing required field | **400 Bad Request** |
| Invalid field value | **400 Bad Request** |
| Path/body mismatch | **400 Bad Request** |
| Valid POST (success) | **201 Created** |
| Valid PUT (success) | **200 OK** |

---

## Implementation Details

- **Framework:** Spring Boot with Spring Validation
- **Annotations:** Jakarta Validation (formerly javax.validation)
- **Error Handler:** Global `@RestControllerAdvice` 
- **Error Response:** Structured JSON with timestamp, status, message, and detailed field errors
- **Logging:** All validation errors logged for debugging

---

## Where to Find More Information

1. **VALIDATION_TEST_COMMANDS.md** - 25+ test cases with curl commands
2. **VALIDATION_RESPONSES.md** - Same tests with actual response examples
3. **ERROR_HANDLING_SUMMARY.md** - Implementation details and changes made
