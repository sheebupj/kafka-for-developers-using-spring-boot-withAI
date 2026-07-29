package com.paremal.kafka.exception;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Instant;
import java.util.List;

public class ApiErrorResponse {
    @JsonFormat(shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING)
    private Instant timestamp;
    private int status;
    private String message;
    private List<FieldError> errors;

    public ApiErrorResponse() {
        this.timestamp = Instant.now();
    }

    public ApiErrorResponse(int status, String message, List<FieldError> errors) {
        this();
        this.status = status;
        this.message = message;
        this.errors = errors;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public List<FieldError> getErrors() {
        return errors;
    }

    public void setErrors(List<FieldError> errors) {
        this.errors = errors;
    }

    public static class FieldError {
        private String field;
        private String message;

        public FieldError(String field, String message) {
            this.field = field;
            this.message = message;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }
}
