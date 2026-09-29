package com.paremal.kafka.config;

import com.paremal.kafka.dto.LibraryEventDto;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RetryListener;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;

/**
 * Kafka consumer configuration for listener containers and error recovery behavior.
 */
@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumerConfig.class);

    /**
     * Creates the Kafka listener container factory with consumer settings,
     * concurrency, and shared error handling.
     */
    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, LibraryEventDto>> kafkaListenerContainerFactory(
           ConsumerFactory<Integer, LibraryEventDto> consumerFactory,
           DefaultErrorHandler defaultErrorHandler) {
       var factory = new ConcurrentKafkaListenerContainerFactory<Integer, LibraryEventDto>();
       factory.setConsumerFactory(consumerFactory);
       // factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL);
       factory.setConcurrency(3);
       factory.setCommonErrorHandler(defaultErrorHandler);
       return factory;
    }

    /**
     * Routes failed records to the dead-letter topic using the same partition
     * as the original record.
     */
    @Bean
    DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaOperations<Object, Object> kafkaTemplate) {
       return new DeadLetterPublishingRecoverer(
               kafkaTemplate,
               (consumerRecord, exception) -> {
                   log.warn(
                           "Routing record to DLT. topic={}, partition={}, offset={}, error={}",
                           consumerRecord.topic(),
                           consumerRecord.partition(),
                           consumerRecord.offset(),
                           exception.getMessage(),
                           exception);
                   return new TopicPartition(consumerRecord.topic() + ".DLT", consumerRecord.partition());
               });
    }

    /**
     * Configures retry and recovery behavior for listener failures.
     */
    @Bean
    DefaultErrorHandler defaultErrorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
       var exponentialBackOff = new ExponentialBackOffWithMaxRetries(2);
       exponentialBackOff.setInitialInterval(1_000L);
       exponentialBackOff.setMultiplier(2.0);
       var errorHandler = new DefaultErrorHandler(
               deadLetterPublishingRecoverer,
               exponentialBackOff);
       // errorHandler.setRetryListeners((consumerRecord, exception, deliveryAttempt) -> log.warn(
       //         "Retrying record. topic={}, partition={}, offset={}, deliveryAttempt={}",
       //                 consumerRecord.topic(),
       //                 consumerRecord.partition(),
       //                 consumerRecord.offset(),
       //         deliveryAttempt,
       //         exception));
       errorHandler.setRetryListeners(new RetryListener() {
           @Override
           public void failedDelivery(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?> consumerRecord,
                                     Exception exception,
                                     int deliveryAttempt) {
               log.warn(
                       "Retrying record. topic={}, partition={}, offset={}, deliveryAttempt={}",
                       consumerRecord.topic(),
                       consumerRecord.partition(),
                       consumerRecord.offset(),
                       deliveryAttempt,
                       exception);
           }
       });
       errorHandler.addNotRetryableExceptions(
               DeserializationException.class,
               NullPointerException.class,
               IllegalArgumentException.class
               );

       errorHandler.setCommitRecovered(true);
       return errorHandler;
    }
}
