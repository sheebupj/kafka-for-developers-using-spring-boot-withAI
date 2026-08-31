package com.paremal.kafka.repository;

import com.paremal.kafka.entity.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data repository for managing {@link LibraryEvent} entities.
 */
public interface LibraryEventRepository extends JpaRepository<LibraryEvent, Integer> {
}
