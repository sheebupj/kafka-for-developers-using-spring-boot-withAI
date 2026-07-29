# Validation Error Handling - Implementation Summary

## Overview
This document summarizes the implementation of detailed validation error responses for the Library Events API.

## Changes Made

### 1. New Error Response Class
**File:** `exception/ApiErrorResponse.java`

A standardized JSON error response structure that contains:
- `timestamp`: ISO 8601 formatted timestamp of when the error occurred
- `status`: HTTP status code (400 for validation errors)
- `message`: Overall error message ("Validation failed")
- `errors`: Array of field-level errors with:
  - `field`: The field name that failed validation
  - `message`: Descriptive error message for that field

### 2. Global Exception Handler
**File:** `exception/GlobalExceptionHandler.java`

Centralized exception handling using Spring's `@RestControllerAdvice`:
- Catches `MethodArgumentNotValidException` for validation errors
- Extracts all field-level and global errors
- Returns structured `ApiErrorResponse` with detailed error messages
- Handles generic exceptions with error details

### 3. Enhanced Model Validation Messages
**Files:** `model/Book.java`, `model/LibraryEvent.java`

Added custom validation messages to all constraints:

**Book Model:**
- `bookId`: "bookId is required" and "bookId must be a positive number"
- `bookName`: "bookName is required" and "bookName cannot exceed 255 characters"
- `bookAuthor`: "bookAuthor is required" and "bookAuthor cannot exceed 255 characters"

**LibraryEvent Model:**
- `libraryEventId`: "libraryEventId is required" and "libraryEventId must be a positive number"
- `eventType`: "eventType is required"
- `book`: "book is required"

### 4. Updated Controller
**File:** `controller/LibraryEventsController.java`

Simplified controller logic:
- Removed manual BindingResult processing (handled by global exception handler)
- Validation errors are automatically caught and formatted
- Business logic errors (eventType mismatch, path ID mismatch) still handled in controller
- Uses `ApiErrorResponse` for consistency

## Error Response Examples

### Validation Error Response Format
```json
{
  "timestamp": "2026-07-28T22:12:15.409+05:30",
  "status": 400,
  "message": "Validation failed",
  "errors": [
    {
      "field": "fieldName",
      "message": "Error message"
    }
  ]
}
```

### Example: Missing Required Fields
Request with missing `eventType` and `book`:
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1}'
```

Response:
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

### Example: Multiple Nested Errors
Request with missing book fields:
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

Response:
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

## Benefits

1. **Detailed Error Messages**: Users see exactly what fields failed and why
2. **All Errors at Once**: Multiple validation errors are returned together, reducing back-and-forth
3. **Standardized Format**: Consistent error response structure across the API
4. **Nested Error Support**: Validates nested objects (Book inside LibraryEvent) with proper field paths
5. **Maintainability**: Error handling centralized in one place (GlobalExceptionHandler)

## Testing the Implementation

See the following documentation files for comprehensive test cases:

1. **VALIDATION_TEST_COMMANDS.md** - curl commands for basic validation tests
2. **VALIDATION_RESPONSES.md** - curl commands with actual error response examples

### Quick Test Example
```bash
# Test missing required field
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 1,
    "eventType": "ADD",
    "book": {
      "bookId": 101,
      "bookName": "Spring in Action"
    }
  }'

# Expected Response with error about missing bookAuthor
```

## Error Handling Flow

1. Client sends request with invalid payload
2. Spring validation framework validates based on annotations
3. If validation fails, `MethodArgumentNotValidException` is thrown
4. `GlobalExceptionHandler` catches the exception
5. All errors are collected from the binding result
6. `ApiErrorResponse` is created with all field errors
7. Response is returned with HTTP 400 status

## All Validation Rules Summary

| Field | Constraints | Error Messages |
|-------|-------------|-----------------|
| libraryEventId | @NotNull, @Positive | "libraryEventId is required", "libraryEventId must be a positive number" |
| eventType | @NotNull | "eventType is required" |
| book | @NotNull, @Valid | "book is required" |
| book.bookId | @NotNull, @Positive | "bookId is required", "bookId must be a positive number" |
| book.bookName | @NotBlank, @Size(max=255) | "bookName is required", "bookName cannot exceed 255 characters" |
| book.bookAuthor | @NotBlank, @Size(max=255) | "bookAuthor is required", "bookAuthor cannot exceed 255 characters" |
| timestamp | - | (Optional field, no validation) |

## Business Logic Validations (in Controller)

| Endpoint | Validation | Error Message |
|----------|------------|-----------------|
| POST /api/v1/library-events | eventType must be ADD | "eventType must be ADD for POST endpoint" |
| PUT /api/v1/library-events/{id} | Path ID == Body ID | "Path libraryEventId must match body.libraryEventId" |
| PUT /api/v1/library-events/{id} | eventType must be UPDATE | "eventType must be UPDATE for PUT endpoint" |

## File Locations

```
lib-events-producer-boot4/src/main/java/com/paremal/kafka/
├── exception/
│   ├── ApiErrorResponse.java (NEW)
│   ├── GlobalExceptionHandler.java (NEW)
│   └── KafkaPublishException.java (existing)
├── model/
│   ├── Book.java (UPDATED - added validation messages)
│   └── LibraryEvent.java (UPDATED - added validation messages)
└── controller/
    └── LibraryEventsController.java (UPDATED - simplified with global handler)
```

## Documentation Files

- `docs/VALIDATION_TEST_COMMANDS.md` - Basic curl command examples
- `docs/VALIDATION_RESPONSES.md` - Detailed examples with error responses
