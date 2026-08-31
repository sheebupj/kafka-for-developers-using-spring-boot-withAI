package com.paremal.kafka.entity;

import com.paremal.kafka.domain.EventType;
import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import java.time.LocalDateTime;

/**
 * JPA entity representing a library event and its associated book.
 */
@Entity
@Table(name = "library_event")
public class LibraryEvent {

    public LibraryEvent() {
    }

    public LibraryEvent(Integer libraryEventId, EventType eventType, Book book) {
        this.libraryEventId = libraryEventId;
        this.eventType = eventType;
        this.book = book;
        if (book != null) {
            book.setLibraryEvent(this);
        }
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "library_event_id")
    private Integer libraryEventId;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false)
    private EventType eventType;

    @OneToOne(mappedBy = "libraryEvent", cascade = CascadeType.ALL, orphanRemoval = true)
    private Book book;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        var now = LocalDateTime.now();
        createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    public Integer getLibraryEventId() {
        return libraryEventId;
    }

    public void setLibraryEventId(Integer libraryEventId) {
        this.libraryEventId = libraryEventId;
    }

    public EventType getEventType() {
        return eventType;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public Book getBook() {
        return book;
    }

    public void setBook(Book book) {
        this.book = book;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }
}
