package com.paremal.kafka.service;

import com.paremal.kafka.dto.LibraryEventDto;
import com.paremal.kafka.mapper.LibraryEventMapper;
import com.paremal.kafka.repository.LibraryEventRepository;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.stream.Collectors;

@Service
public class LibraryEventService {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventService.class);
    private final LibraryEventRepository libraryEventRepository;
    private final LibraryEventMapper libraryEventMapper;
    private final Validator validator;

    public LibraryEventService(
            LibraryEventRepository libraryEventRepository,
            LibraryEventMapper libraryEventMapper,
            Validator validator) {
        this.libraryEventRepository = libraryEventRepository;
        this.libraryEventMapper = libraryEventMapper;
        this.validator = validator;
    }

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

    private void processAddEvent(LibraryEventDto libraryEventDto) {
        var entity = libraryEventMapper.toEntity(libraryEventDto);
        libraryEventRepository.save(entity);
    }

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

    private void validateEvent(LibraryEventDto libraryEventDto) {
        var violations = validator.validate(libraryEventDto);
        if (!violations.isEmpty()) {
            throw new IllegalArgumentException(violationMessage(violations));
        }
    }

    private String violationMessage(Iterable<ConstraintViolation<LibraryEventDto>> violations) {
        return "Validation failed: " + java.util.stream.StreamSupport.stream(violations.spliterator(), false)
                .map(violation -> "%s %s".formatted(violation.getPropertyPath(), violation.getMessage()))
                .collect(Collectors.joining(", "));
    }
}
