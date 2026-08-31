package com.paremal.kafka.repository;

import com.paremal.kafka.entity.Book;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data repository for managing {@link Book} entities.
 */
public interface BookRepository extends JpaRepository<Book, Integer> {
}
