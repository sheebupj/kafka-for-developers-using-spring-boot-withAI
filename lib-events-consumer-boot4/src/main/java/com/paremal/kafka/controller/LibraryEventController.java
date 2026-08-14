package com.paremal.kafka.controller;

import com.paremal.kafka.dto.LibraryEventDto;
import com.paremal.kafka.dto.LibraryEventResponseDto;
import com.paremal.kafka.entity.LibraryEvent;
import com.paremal.kafka.service.LibraryEventService;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.slf4j.Logger;

import java.util.List;

@RestController
@RequestMapping("/vi/library-eventes")
public class LibraryEventController {

    private static final Logger log= LoggerFactory.getLogger(LibraryEventController.class);

    private final LibraryEventService libraryEventService;


    public LibraryEventController(LibraryEventService libraryEventService) {
        this.libraryEventService = libraryEventService;
    }

    @GetMapping
    public ResponseEntity<List<LibraryEventResponseDto>> getAllLibraryEvents(){
        log.info("GET /v1/library-events");
        return ResponseEntity.ok(libraryEventService.findAll());
    }

    @GetMapping("/{libraryEventId}")
    public ResponseEntity<LibraryEventResponseDto> getLibraryEventById(@PathVariable Integer libraryEventId) {
        log.info("GET /v1/library-events/{}", libraryEventId);
        return libraryEventService.findById(libraryEventId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
