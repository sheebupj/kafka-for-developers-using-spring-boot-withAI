package com.paremal.kafka.model;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class LibraryEventValidationTest {

    private static ValidatorFactory validatorFactory;
    private static Validator validator;

    @BeforeAll
    static void setUpValidator() {
        validatorFactory = Validation.buildDefaultValidatorFactory();
        validator = validatorFactory.getValidator();
    }

    @AfterAll
    static void tearDownValidator() {
        validatorFactory.close();
    }

    @Test
    void validLibraryEvent_hasNoViolations() {
        LibraryEvent event = new LibraryEvent(1L, EventType.ADD, new Book(1, "Kafka", "Dilip"), null);

        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);

        assertThat(violations).isEmpty();
    }

    @Test
    void invalidNestedBookAndMissingEventId_returnsViolations() {
        LibraryEvent event = new LibraryEvent(null, EventType.ADD, new Book(1, "", ""), null);

        Set<ConstraintViolation<LibraryEvent>> violations = validator.validate(event);

        assertThat(violations)
                .extracting(v -> v.getPropertyPath().toString(), ConstraintViolation::getMessage)
                .contains(
                        tuple("libraryEventId", "libraryEventId is required"),
                        tuple("book.bookName", "bookName is required"),
                        tuple("book.bookAuthor", "bookAuthor is required")
                );
    }
}
