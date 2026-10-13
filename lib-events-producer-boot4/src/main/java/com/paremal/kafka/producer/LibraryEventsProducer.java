package com.paremal.kafka.producer;

import com.paremal.kafka.exception.KafkaPublishException;
import com.paremal.kafka.model.LibraryEvent;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class LibraryEventsProducer {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsProducer.class);

    private final KafkaTemplate<Integer, Object> kafkaTemplate;
    private final String defaultTopic;
    private final MeterRegistry meterRegistry;
    private final Counter producedCounter;

    public LibraryEventsProducer(KafkaTemplate<Integer, Object> kafkaTemplate,
                                 @Value("${library.events.topic}") String defaultTopic,
                                 MeterRegistry meterRegistry) {
        this.kafkaTemplate = kafkaTemplate;
        this.defaultTopic = defaultTopic;
        this.meterRegistry = meterRegistry;
        this.producedCounter = this.meterRegistry.counter("library.events.produced");
    }

    /**
     * Send a library event asynchronously. Returns a CompletableFuture that completes with RecordMetadata on success
     * or completes exceptionally with KafkaPublishException on failure.
     */
    public CompletableFuture<RecordMetadata> send(String topic, Integer key, LibraryEvent value) {
        String existingCorrelation = MDC.get("correlationId");
        final String correlationId = (existingCorrelation != null) ? existingCorrelation : String.valueOf(key);
        final boolean correlationIdAdded = existingCorrelation == null;
        if (correlationIdAdded) {
            MDC.put("correlationId", correlationId);
        }

        CompletableFuture<RecordMetadata> resultFuture = new CompletableFuture<>();
        try {
            kafkaTemplate.send(topic, key, value).whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("correlationId={} Failed to send message to topic {}", correlationId, topic, ex);
                    resultFuture.completeExceptionally(new KafkaPublishException("Failed to publish message to Kafka", ex));
                    return;
                }
                RecordMetadata metadata = result.getRecordMetadata();
                log.info("correlationId={} Message sent to topic {} partition {} offset {}", correlationId, topic, metadata.partition(), metadata.offset());
                producedCounter.increment();
                resultFuture.complete(metadata);
            });
        } catch (RuntimeException ex) {
            log.error("correlationId={} Failed to send message to topic {}", correlationId, topic, ex);
            resultFuture.completeExceptionally(new KafkaPublishException("Failed to publish message to Kafka", ex));
        } finally {
            if (correlationIdAdded) {
                MDC.remove("correlationId");
            }
        }

        return resultFuture;
    }

    /** convenience overload that uses the configured default topic */
    public CompletableFuture<RecordMetadata> send(Integer key, LibraryEvent value) {
        return send(this.defaultTopic, key, value);
    }

    /**
     * Send a library event synchronously and return RecordMetadata on success.
     */
    public RecordMetadata sendSynchronously(String topic, Integer key, LibraryEvent value) {
        String existingCorrelation = MDC.get("correlationId");
        final String correlationId = (existingCorrelation != null) ? existingCorrelation : String.valueOf(key);
        final boolean correlationIdAdded = existingCorrelation == null;
        if (correlationIdAdded) {
            MDC.put("correlationId", correlationId);
        }

        try {
            SendResult<Integer, Object> result = kafkaTemplate.send(topic, key, value).get(10, TimeUnit.SECONDS);
            RecordMetadata metadata = result.getRecordMetadata();
            log.info("correlationId={} Message sent to topic {} partition {} offset {}", correlationId, topic, metadata.partition(), metadata.offset());
            producedCounter.increment();
            return metadata;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            log.error("correlationId={} Failed to send message to topic {}", correlationId, topic, ex);
            throw new KafkaPublishException("Failed to publish message to Kafka", ex);
        } catch (ExecutionException ex) {
            log.error("correlationId={} Failed to send message to topic {}", correlationId, topic, ex);
            throw new KafkaPublishException("Failed to publish message to Kafka", ex);
        } catch (TimeoutException ex) {
            log.error("correlationId={} Timed out while sending message to topic {}", correlationId, topic, ex);
            throw new KafkaPublishException("Timed out while publishing message to Kafka", ex);
        } finally {
            if (correlationIdAdded) {
                MDC.remove("correlationId");
            }
        }
    }

    /** convenience overload that uses the configured default topic */
    public RecordMetadata sendSynchronously(Integer key, LibraryEvent value) {
        return sendSynchronously(this.defaultTopic, key, value);
    }

    /**
     * Send a library event inside a Kafka transaction and block until the broker acknowledges it.
     * The transaction commits when the method returns and rolls back if it throws.
     */
    @Transactional
    public CompletableFuture<RecordMetadata> sendInTransaction(String topic, Integer key, LibraryEvent value) {
        return send(topic, key, value);
    }

    /** convenience overload that uses the configured default topic */
    @Transactional
    public CompletableFuture<RecordMetadata> sendInTransaction(Integer key, LibraryEvent value) {
        return send(this.defaultTopic, key, value);
    }


    /**
     * Send several library events atomically: either all are committed or none are visible to consumers.
     */
    @Transactional
    public List<RecordMetadata> sendAllInTransaction(String topic, Map<Integer, LibraryEvent> events) {
        List<RecordMetadata> results = new ArrayList<>();
        events.forEach((key, value) -> results.add(sendSynchronously(topic, key, value)));
        return results;
    }
}
