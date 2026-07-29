package com.paremal.kafka.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.ArrayList;
import java.util.List;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiErrorResponse> handleValidationException(MethodArgumentNotValidException ex) {
        List<ApiErrorResponse.FieldError> fieldErrors = new ArrayList<>();

        ex.getBindingResult().getFieldErrors().forEach(error -> {
            String fieldName = error.getField();
            String errorMessage = error.getDefaultMessage();
            fieldErrors.add(new ApiErrorResponse.FieldError(fieldName, errorMessage));
        });

        ex.getBindingResult().getGlobalErrors().forEach(error -> {
            String objectName = error.getObjectName();
            String errorMessage = error.getDefaultMessage();
            fieldErrors.add(new ApiErrorResponse.FieldError(objectName, errorMessage));
        });

        ApiErrorResponse errorResponse = new ApiErrorResponse(
                HttpStatus.BAD_REQUEST.value(),
                "Validation failed",
                fieldErrors
        );

        log.error("Validation error: {}", errorResponse);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(errorResponse);
    }

    @ExceptionHandler(KafkaPublishException.class)
    public ResponseEntity<ApiErrorResponse> handleKafkaPublishException(KafkaPublishException ex) {
        List<ApiErrorResponse.FieldError> errors = new ArrayList<>();
        errors.add(new ApiErrorResponse.FieldError("kafka", ex.getMessage()));

        ApiErrorResponse errorResponse = new ApiErrorResponse(
                HttpStatus.SERVICE_UNAVAILABLE.value(),
                "Kafka publish failed",
                errors
        );

        log.error("Kafka publish error", ex);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(errorResponse);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiErrorResponse> handleGenericException(Exception ex) {
        List<ApiErrorResponse.FieldError> errors = new ArrayList<>();
        errors.add(new ApiErrorResponse.FieldError("error", ex.getMessage()));

        ApiErrorResponse errorResponse = new ApiErrorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                "An error occurred",
                errors
        );

        log.error("Unexpected error: ", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorResponse);
    }
}
