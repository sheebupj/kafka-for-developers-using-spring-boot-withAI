# Payload Validation Test Commands

This document contains curl commands to test the payload validations for the Library Events API.

## Base URL
```
http://localhost:8080/api/v1/library-events
```

## Test Data Reference

### Validation Rules:
- **LibraryEvent**
  - `libraryEventId`: Required, must be positive number
  - `eventType`: Required, must be either ADD or UPDATE
  - `book`: Required, must be valid Book object
  - `timestamp`: Optional, ISO 8601 format

- **Book**
  - `bookId`: Required, must be positive number
  - `bookName`: Required, cannot be blank, max 255 characters
  - `bookAuthor`: Required, cannot be blank, max 255 characters

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

### 4. Valid Book with Maximum Length Title and Author
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 4,
    "eventType": "ADD",
    "book": {
      "bookId": 104,
      "bookName": "The Quick Brown Fox Jumps Over The Lazy Dog And Then Continues Running Through The Forest Until It Reaches The Mountains Where It Finds A Beautiful Valley Full Of Flowers And Crystal Clear Streams And Decides To Stay There Forever Because It Was The Most",
      "bookAuthor": "Lorem Ipsum Dolor Sit Amet Consectetur Adipiscing Elit Sed Do Eiusmod Tempor Incididunt Ut Labore Et Dolore Magna Aliqua Ut Enim Ad Minim Veniam Quis Nostrud Exercitation Ullamco Laboris Nisi Ut"
    }
  }'
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
**Expected Error**: libraryEventId cannot be null

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
**Expected Error**: libraryEventId must be positive

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
**Expected Error**: libraryEventId must be positive

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
**Expected Error**: eventType cannot be null

### 9. Invalid eventType (GET instead of ADD/UPDATE)
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 9,
    "eventType": "GET",
    "book": {
      "bookId": 109,
      "bookName": "Refactoring",
      "bookAuthor": "Martin Fowler"
    }
  }'
```
**Expected Error**: Invalid enum value for eventType

### 10. Wrong eventType for POST (UPDATE instead of ADD)
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
**Expected Error**: eventType must be ADD for POST endpoint

### 11. Missing Book Object
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 11,
    "eventType": "ADD"
  }'
```
**Expected Error**: book cannot be null

### 12. Book with Missing bookId
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 12,
    "eventType": "ADD",
    "book": {
      "bookName": "Accelerate",
      "bookAuthor": "Nicole Forsgren"
    }
  }'
```
**Expected Error**: bookId cannot be null

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
**Expected Error**: bookId must be positive

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
**Expected Error**: bookId must be positive

### 15. Book with Missing bookName
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 15,
    "eventType": "ADD",
    "book": {
      "bookId": 115,
      "bookAuthor": "Stephen King"
    }
  }'
```
**Expected Error**: bookName cannot be null

### 16. Book with Blank bookName
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
**Expected Error**: bookName cannot be blank

### 17. Book with Empty bookName
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
**Expected Error**: bookName cannot be blank

### 18. Book with Missing bookAuthor
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
**Expected Error**: bookAuthor cannot be null

### 19. Book with Blank bookAuthor
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 19,
    "eventType": "ADD",
    "book": {
      "bookId": 119,
      "bookName": "The Great Gatsby",
      "bookAuthor": "   "
    }
  }'
```
**Expected Error**: bookAuthor cannot be blank

### 20. Book with Empty bookAuthor
```bash
curl -X POST http://localhost:8080/api/v1/library-events \
  -H "Content-Type: application/json" \
  -d '{
    "libraryEventId": 20,
    "eventType": "ADD",
    "book": {
      "bookId": 120,
      "bookName": "Pride and Prejudice",
      "bookAuthor": ""
    }
  }'
```
**Expected Error**: bookAuthor cannot be blank

### 21. Book with bookName Exceeding 255 Characters
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
**Expected Error**: bookName size must be between 0 and 255

### 22. Book with bookAuthor Exceeding 255 Characters
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
**Expected Error**: bookAuthor size must be between 0 and 255

### 23. PUT with Wrong Path ID (Path ID != Body ID)
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
**Expected Error**: Path libraryEventId must match body.libraryEventId

### 24. PUT with Wrong eventType (ADD instead of UPDATE)
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
**Expected Error**: eventType must be UPDATE for PUT endpoint

### 25. Invalid JSON Syntax
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
**Expected Error**: JSON parsing error / 400 Bad Request

---

## 📋 Quick Test Script

You can also save these commands to a bash script file and run them sequentially:

```bash
#!/bin/bash

# Set base URL
BASE_URL="http://localhost:8080/api/v1/library-events"

# Test 1: Valid ADD event
echo "Test 1: Valid ADD event"
curl -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d '{"libraryEventId": 1, "eventType": "ADD", "book": {"bookId": 101, "bookName": "Spring in Action", "bookAuthor": "Craig Walls"}}'
echo -e "\n\n"

# Test 2: Missing libraryEventId
echo "Test 2: Missing libraryEventId"
curl -X POST $BASE_URL \
  -H "Content-Type: application/json" \
  -d '{"eventType": "ADD", "book": {"bookId": 105, "bookName": "Effective Java", "bookAuthor": "Joshua Bloch"}}'
echo -e "\n\n"

# Add more tests as needed...
```

---

## Running Tests with Custom Headers

### Test with Pretty JSON Response
```bash
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
