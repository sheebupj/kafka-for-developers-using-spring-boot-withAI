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

class BookValidationTest {

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
    void validBook_hasNoViolations() {
        Book book = new Book(1, "Kafka", "Dilip");

        Set<ConstraintViolation<Book>> violations = validator.validate(book);

        assertThat(violations).isEmpty();
    }

    @Test
    void invalidBookNameAndAuthor_returnsViolations() {
        Book book = new Book(1, "", "   ");

        Set<ConstraintViolation<Book>> violations = validator.validate(book);

        assertThat(violations)
                .extracting(v -> v.getPropertyPath().toString(), ConstraintViolation::getMessage)
                .contains(
                        tuple("bookName", "bookName is required"),
                        tuple("bookAuthor", "bookAuthor is required")
                );
    }
}

