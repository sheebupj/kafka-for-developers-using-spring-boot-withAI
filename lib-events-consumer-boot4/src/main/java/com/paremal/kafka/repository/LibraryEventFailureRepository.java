package com.paremal.kafka.repository;

import com.paremal.kafka.entity.LibraryEventFailure;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Repository for failed Kafka record details.
 */
public interface LibraryEventFailureRepository extends JpaRepository<LibraryEventFailure, Long> {
}
