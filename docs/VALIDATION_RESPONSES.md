# Payload Validation Test Commands with Error Responses

This document contains curl commands to test the payload validations for the Library Events API with detailed error response formats.

## Base URL
```
http://localhost:8080/api/v1/library-events
```

## Validation Rules Reference

### LibraryEvent
- `libraryEventId`: Required, must be positive number
- `eventType`: Required, must be either ADD or UPDATE
- `book`: Required, must be valid Book object
- `timestamp`: Optional, ISO 8601 format

### Book
- `bookId`: Required, must be positive number
- `bookName`: Required, cannot be blank, max 255 characters
- `bookAuthor`: Required, cannot be blank, max 255 characters

---

## Error Response Format

All validation errors are returned in the following JSON format:

```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "fieldName",
      "message": "Field error message"
    }
  ]
}
```

---

## ✅ VALID REQUEST EXAMPLES

### 1. Valid POST Request (ADD Event)
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

**Expected Response (201 Created):**
```json
{
  "libraryEventId": 1,
  "eventType": "ADD",
  "book": {
    "bookId": 101,
    "bookName": "Spring in Action",
    "bookAuthor": "Craig Walls"
  },
  "timestamp": "2026-07-28T22:12:15.409+05:30"
}
```

### 2. Valid POST Request with Timestamp
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 2,
    "eventType": "ADD",
    "book": {
      "bookId": 102,
      "bookName": "Kafka: The Definitive Guide",
      "bookAuthor": "Neha Narkhede"
    },
    "timestamp": "2026-07-28T21:49:02.061+05:30"
  }'
```

**Expected Response (201 Created):** Same as above

### 3. Valid PUT Request (UPDATE Event)
```bash
curl -X PUT http://localhost:8080/api/v1/library-events/3 \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 3,
    "eventType": "UPDATE",
    "book": {
      "bookId": 103,
      "bookName": "Clean Code",
      "bookAuthor": "Robert C. Martin"
    }
  }'
```

**Expected Response (200 OK):**
```json
{
  "libraryEventId": 3,
  "eventType": "UPDATE",
  "book": {
    "bookId": 103,
    "bookName": "Clean Code",
    "bookAuthor": "Robert C. Martin"
  },
  "timestamp": null
}
```

---

## ❌ INVALID REQUEST EXAMPLES

### 5. Missing libraryEventId
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "ADD",
    "book": {
      "bookId": 105,
      "bookName": "Effective Java",
      "bookAuthor": "Joshua Bloch"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "libraryEventId",
      "message": "libraryEventId is required"
    }
  ]
}
```

### 6. libraryEventId = 0 (Not Positive)
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 0,
    "eventType": "ADD",
    "book": {
      "bookId": 106,
      "bookName": "Design Patterns",
      "bookAuthor": "Gang of Four"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "libraryEventId",
      "message": "libraryEventId must be a positive number"
    }
  ]
}
```

### 7. libraryEventId < 0 (Negative)
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": -5,
    "eventType": "ADD",
    "book": {
      "bookId": 107,
      "bookName": "Microservices Patterns",
      "bookAuthor": "Chris Richardson"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "libraryEventId",
      "message": "libraryEventId must be a positive number"
    }
  ]
}
```

### 8. Missing eventType
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 8,
    "book": {
      "bookId": 108,
      "bookName": "The Pragmatic Programmer",
      "bookAuthor": "David Thomas"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "eventType",
      "message": "eventType is required"
    }
  ]
}
```

### 9. Wrong eventType for POST (UPDATE instead of ADD)
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 10,
    "eventType": "UPDATE",
    "book": {
      "bookId": 110,
      "bookName": "Code Complete",
      "bookAuthor": "Steve McConnell"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "eventType",
      "message": "eventType must be ADD for POST endpoint"
    }
  ]
}
```

### 10. Multiple Validation Errors - Missing Book and eventType
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 11
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "eventType",
      "message": "eventType is required"
    },
    {
      "field": "book",
      "message": "book is required"
    }
  ]
}
```

### 11. Missing Book Object
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 11,
    "eventType": "ADD"
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "book",
      "message": "book is required"
    }
  ]
}
```

### 12. Multiple Book Validation Errors - Missing bookId and bookAuthor
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 12,
    "eventType": "ADD",
    "book": {
      "bookName": "Accelerate"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "book.bookId",
      "message": "bookId is required"
    },
    {
      "field": "book.bookAuthor",
      "message": "bookAuthor is required"
    }
  ]
}
```

### 13. Book with Non-Positive bookId (0)
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 13,
    "eventType": "ADD",
    "book": {
      "bookId": 0,
      "bookName": "Dune",
      "bookAuthor": "Frank Herbert"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "book.bookId",
      "message": "bookId must be a positive number"
    }
  ]
}
```

### 14. Book with Negative bookId
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 14,
    "eventType": "ADD",
    "book": {
      "bookId": -99,
      "bookName": "Foundation",
      "bookAuthor": "Isaac Asimov"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "book.bookId",
      "message": "bookId must be a positive number"
    }
  ]
}
```

### 15. Book with Blank bookName
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 16,
    "eventType": "ADD",
    "book": {
      "bookId": 116,
      "bookName": "   ",
      "bookAuthor": "George Orwell"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "book.bookName",
      "message": "bookName is required"
    }
  ]
}
```

### 16. Book with Empty bookName
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 17,
    "eventType": "ADD",
    "book": {
      "bookId": 117,
      "bookName": "",
      "bookAuthor": "Ray Bradbury"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "book.bookName",
      "message": "bookName is required"
    }
  ]
}
```

### 17. Book with bookName Exceeding 255 Characters
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 21,
    "eventType": "ADD",
    "book": {
      "bookId": 121,
      "bookName": "This is a very long book title that exceeds the maximum allowed length of 255 characters. It continues to go on and on with more and more words being added to make it longer than the allowed limit. The validation should catch this and return an error message indicating that the field size must not exceed the maximum value specified in the constraint.",
      "bookAuthor": "Jane Austen"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "book.bookName",
      "message": "bookName cannot exceed 255 characters"
    }
  ]
}
```

### 18. Book with bookAuthor Exceeding 255 Characters
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 22,
    "eventType": "ADD",
    "book": {
      "bookId": 122,
      "bookName": "The Hobbit",
      "bookAuthor": "Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat duis aute irure dolor in reprehenderit in voluptate velit esse"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "book.bookAuthor",
      "message": "bookAuthor cannot exceed 255 characters"
    }
  ]
}
```

### 19. Book with Missing bookAuthor
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 18,
    "eventType": "ADD",
    "book": {
      "bookId": 118,
      "bookName": "1984"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "book.bookAuthor",
      "message": "bookAuthor is required"
    }
  ]
}
```

### 20. PUT with Wrong Path ID (Path ID != Body ID)
```bash
curl -X PUT http://localhost:8080/api/v1/library-events/50 \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 23,
    "eventType": "UPDATE",
    "book": {
      "bookId": 123,
      "bookName": "Sapiens",
      "bookAuthor": "Yuval Noah Harari"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "libraryEventId",
      "message": "Path libraryEventId must match body.libraryEventId"
    }
  ]
}
```

### 21. PUT with Wrong eventType (ADD instead of UPDATE)
```bash
curl -X PUT http://localhost:8080/api/v1/library-events/24 \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 24,
    "eventType": "ADD",
    "book": {
      "bookId": 124,
      "bookName": "Guns Germs and Steel",
      "bookAuthor": "Jared Diamond"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "eventType",
      "message": "eventType must be UPDATE for PUT endpoint"
    }
  ]
}
```

### 22. PUT with Both Path Mismatch and Wrong eventType
```bash
curl -X PUT http://localhost:8080/api/v1/library-events/50 \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 24,
    "eventType": "ADD",
    "book": {
      "bookId": 124,
      "bookName": "Guns Germs and Steel",
      "bookAuthor": "Jared Diamond"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "libraryEventId",
      "message": "Path libraryEventId must match body.libraryEventId"
    },
    {
      "field": "eventType",
      "message": "eventType must be UPDATE for PUT endpoint"
    }
  ]
}
```

### 23. Multiple Errors - Missing Multiple Fields at All Levels
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "eventType": "ADD",
    "book": {
      "bookId": 130
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "libraryEventId",
      "message": "libraryEventId is required"
    },
    {
      "field": "book.bookName",
      "message": "bookName is required"
    },
    {
      "field": "book.bookAuthor",
      "message": "bookAuthor is required"
    }
  ]
}
```

### 24. Invalid JSON Syntax
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 25,
    "eventType": "ADD"
    "book": {
      "bookId": 125,
      "bookName": "Brief History of Time",
      "bookAuthor": "Stephen Hawking"
    }
  }'
```

**Expected Response (400 Bad Request):**
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "An error occurred",
  "errors": [
    {
      "field": "error",
      "message": "JSON parse error: Unexpected character ('}' (code 125)): expected a value"
    }
  ]
}
```

---

## Running Tests with Pretty JSON Output

```bash
# Install jq if not already installed (for pretty-printing JSON)
# macOS: brew install jq
# Ubuntu: sudo apt-get install jq
# Windows: choco install jq

# Test with pretty JSON response
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1, "eventType": "ADD", "book": {"bookId": 101, "bookName": "Spring in Action", "bookAuthor": "Craig Walls"}}' \
  | jq '.'
```

### Test with Verbose Output
```bash
curl -v -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1, "eventType": "ADD", "book": {"bookId": 101, "bookName": "Spring in Action", "bookAuthor": "Craig Walls"}}'
```

### Test with Response Headers
```bash
curl -i -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1, "eventType": "ADD", "book": {"bookId": 101, "bookName": "Spring in Action", "bookAuthor": "Craig Walls"}}'
```

---

## Expected HTTP Status Codes

| Scenario | Status Code |
|----------|-------------|
| Valid POST request | 201 Created |
| Valid PUT request | 200 OK |
| Validation error | 400 Bad Request |
| Invalid JSON | 400 Bad Request |
| Path/Body mismatch | 400 Bad Request |
| Wrong eventType | 400 Bad Request |
