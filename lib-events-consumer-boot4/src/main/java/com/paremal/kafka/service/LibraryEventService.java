package com.paremal.kafka.service;

import com.paremal.kafka.dto.LibraryEventDto;
import com.paremal.kafka.dto.LibraryEventResponseDto;
import com.paremal.kafka.mapper.LibraryEventMapper;
import com.paremal.kafka.repository.LibraryEventRepository;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Service layer for processing Kafka library events and exposing read operations.
 */
@Service
public class LibraryEventService {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);
    private final LibraryEventRepository libraryEventRepository;
    private final LibraryEventMapper libraryEventMapper;
    private final Validator validator;

    /**
     * Creates the service with its required repository, mapper, and validator dependencies.
     */
    public LibraryEventService(
            LibraryEventRepository libraryEventRepository,
            LibraryEventMapper libraryEventMapper,
            Validator validator) {
        this.libraryEventRepository = libraryEventRepository;
        this.libraryEventMapper = libraryEventMapper;
        this.validator = validator;
    }

    /**
     * Validates and dispatches an incoming Kafka record to ADD or UPDATE processing.
     * Throws {@link IllegalArgumentException} when the payload is null or invalid.
     */
    public void processEvent(ConsumerRecord<Integer, LibraryEventDto> consumerRecord) {
        var libraryEventDto = consumerRecord.value();
        if (libraryEventDto == null) {
            throw new IllegalArgumentException("libraryEvent payload cannot be null");
        }
        validateEvent(libraryEventDto);

        log.info("Processing libraryEventDto. libraryEventDto={}", libraryEventDto);

        switch (libraryEventDto.eventType()) {
            case ADD -> processAddEvent(libraryEventDto);
            case UPDATE -> processUpdateEvent(libraryEventDto);
        }
    }

    /**
     * Persists a new {@link com.paremal.kafka.entity.LibraryEvent} from an ADD event payload.
     */
    private void processAddEvent(LibraryEventDto libraryEventDto) {
        var entity = libraryEventMapper.toEntity(libraryEventDto);
        libraryEventRepository.save(entity);
    }

    /**
     * Updates an existing {@link com.paremal.kafka.entity.LibraryEvent} from an UPDATE event payload.
     * Throws {@link IllegalArgumentException} when the ID is missing or the record does not exist.
     */
    private void processUpdateEvent(LibraryEventDto libraryEventDto) {
        if (libraryEventDto.libraryEventId() == null) {
            throw new IllegalArgumentException("libraryEventId is required for UPDATE events");
        }

        var existingEvent = libraryEventRepository.findById(libraryEventDto.libraryEventId())
                .orElseThrow(() -> new IllegalArgumentException(
                        "LibraryEvent not found for libraryEventId=%d".formatted(libraryEventDto.libraryEventId())));
        libraryEventMapper.updateEntity(libraryEventDto, existingEvent);
        libraryEventRepository.save(existingEvent);
    }
    /**
     * Returns all library events as response DTOs.
     */
    public List<LibraryEventResponseDto> findAll() {
        log.info("Fetching all library events");
        return libraryEventRepository.findAll()
                .stream()
                .map(libraryEventMapper::toLibraryEventResponseDto)
                .toList();
    }
    /**
     * Returns a single library event by ID, or empty when not found.
     */
    public Optional<LibraryEventResponseDto> findById(Integer libraryEventId) {
        log.info("Fetching library event with id: {}", libraryEventId);
        return libraryEventRepository.findById(libraryEventId)
                .map(libraryEventMapper::toLibraryEventResponseDto);
    }

    /**
     * Validates the event DTO against bean-validation constraints.
     * Throws {@link IllegalArgumentException} listing all violations when invalid.
     */
    private void validateEvent(LibraryEventDto libraryEventDto) {
        var violations = validator.validate(libraryEventDto);
        if (!violations.isEmpty()) {
            throw new IllegalArgumentException(violationMessage(violations));
        }
    }

    /**
     * Formats constraint violations into a single human-readable error message.
     */
    private String violationMessage(Iterable<ConstraintViolation<LibraryEventDto>> violations) {
        return "Validation failed: " + java.util.stream.StreamSupport.stream(violations.spliterator(), false)
                .map(violation -> "%s %s".formatted(violation.getPropertyPath(), violation.getMessage()))
                .collect(Collectors.joining(", "));
    }
}
