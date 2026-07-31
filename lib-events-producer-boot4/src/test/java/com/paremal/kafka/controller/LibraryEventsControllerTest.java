package com.paremal.kafka.controller;

import com.paremal.kafka.model.LibraryEvent;
import com.paremal.kafka.service.LibraryEventService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(LibraryEventsController.class)
class LibraryEventsControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private LibraryEventService libraryEventService;

    @Test
    void postLibraryEvent_validInput_returnsCreated() throws Exception {
        String payload = """
                {
                  "libraryEventId": 1,
                  "eventType": "ADD",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        when(libraryEventService.publishAdd(any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        MvcResult mvcResult = mockMvc.perform(post("/api/v1/library-events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.libraryEventId").value(1))
                .andExpect(jsonPath("$.eventType").value("ADD"))
                .andExpect(jsonPath("$.book.bookId").value(123));

        verify(libraryEventService).publishAdd(any(LibraryEvent.class));
    }

    @Test
    void postLibraryEvent_invalidEventType_returnsBadRequest() throws Exception {
        String payload = """
                {
                  "libraryEventId": 1,
                  "eventType": "UPDATE",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        MvcResult mvcResult = mockMvc.perform(post("/api/v1/library-events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.message").value("Validation failed"))
                .andExpect(jsonPath("$.errors[0].field").value("eventType"))
                .andExpect(jsonPath("$.errors[0].message")
                        .value("eventType must be ADD for POST endpoint"));

        verify(libraryEventService, never()).publishAdd(any(LibraryEvent.class));
    }

    @Test
    void postLibraryEvent_invalidBookName_returnsBadRequest() throws Exception {
        String invalidPayload = """
                {
                  "libraryEventId": 1,
                  "eventType": "ADD",
                  "book": {
                    "bookId": 123,
                    "bookName": "",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        mockMvc.perform(post("/api/v1/library-events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(invalidPayload))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.message").value("Validation failed"))
                .andExpect(jsonPath("$.errors[*].field", hasItem("book.bookName")));

        verify(libraryEventService, never()).publishAdd(any(LibraryEvent.class));
    }

    @Test
    void putLibraryEvent_validInput_returnsOk() throws Exception {
        String payload = """
                {
                  "libraryEventId": 1,
                  "eventType": "UPDATE",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        when(libraryEventService.updateLibraryEvent(any(LibraryEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        MvcResult mvcResult = mockMvc.perform(put("/api/v1/library-events/{libraryEventId}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.libraryEventId").value(1))
                .andExpect(jsonPath("$.eventType").value("UPDATE"));

        verify(libraryEventService).updateLibraryEvent(any(LibraryEvent.class));
    }

    @Test
    void putLibraryEvent_pathBodyIdMismatch_returnsBadRequest() throws Exception {
        String payload = """
                {
                  "libraryEventId": 2,
                  "eventType": "UPDATE",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        MvcResult mvcResult = mockMvc.perform(put("/api/v1/library-events/{libraryEventId}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.message").value("Validation failed"))
                .andExpect(jsonPath("$.errors[0].field").value("libraryEventId"))
                .andExpect(jsonPath("$.errors[0].message")
                        .value("Path libraryEventId must match body.libraryEventId"));

        verify(libraryEventService, never()).updateLibraryEvent(any(LibraryEvent.class));
    }

    @Test
    void putLibraryEvent_invalidEventType_returnsBadRequest() throws Exception {
        String payload = """
                {
                  "libraryEventId": 1,
                  "eventType": "ADD",
                  "book": {
                    "bookId": 123,
                    "bookName": "Kafka Using Spring Boot",
                    "bookAuthor": "Dilip"
                  }
                }
                """;

        MvcResult mvcResult = mockMvc.perform(put("/api/v1/library-events/{libraryEventId}", 1)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(payload))
                .andExpect(request().asyncStarted())
                .andReturn();

        mockMvc.perform(asyncDispatch(mvcResult))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.status").value(400))
                .andExpect(jsonPath("$.message").value("Validation failed"))
                .andExpect(jsonPath("$.errors[0].field").value("eventType"))
                .andExpect(jsonPath("$.errors[0].message")
                        .value("eventType must be UPDATE for PUT endpoint"));

        verify(libraryEventService, never()).updateLibraryEvent(any(LibraryEvent.class));
    }
}
