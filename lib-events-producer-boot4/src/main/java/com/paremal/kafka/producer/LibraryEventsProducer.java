package com.paremal.kafka.producer;

import com.paremal.kafka.exception.KafkaPublishException;
import com.paremal.kafka.model.LibraryEvent;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;

import java.util.concurrent.CompletableFuture;

@Component
public class LibraryEventsProducer {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String defaultTopic;
    private final MeterRegistry meterRegistry;
    private final Counter producedCounter;

    public LibraryEventsProducer(KafkaTemplate<String, Object> kafkaTemplate,
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
    public CompletableFuture<RecordMetadata> send(String topic, String key, LibraryEvent value) {
        String existingCorrelation = MDC.get("correlationId");
                final String correlationId = (existingCorrelation != null) ? existingCorrelation : key;
                if (existingCorrelation == null) {
                    MDC.put("correlationId", correlationId);
                }

                CompletableFuture<RecordMetadata> resultFuture = new CompletableFuture<>();

        kafkaTemplate.send(topic, key, value).whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("correlationId={} Failed to send message to topic {}", correlationId, topic, ex);
                        resultFuture.completeExceptionally(new KafkaPublishException("Failed to publish message to Kafka", ex));
                        MDC.remove("correlationId");
                        return;
                    }
                    RecordMetadata metadata = result.getRecordMetadata();
                    log.info("correlationId={} Message sent to topic {} partition {} offset {}", correlationId, topic, metadata.partition(), metadata.offset());
                    producedCounter.increment();
                    resultFuture.complete(metadata);
                    MDC.remove("correlationId");
                });

        return resultFuture;
    }

    /** convenience overload that uses the configured default topic */
    public CompletableFuture<RecordMetadata> send(String key, LibraryEvent value) {
        return send(this.defaultTopic, key, value);
    }
}
