package com.paremal.kafka.config;

import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@EnableKafka
public class LibraryEventsConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(LibraryEventsConsumerConfig.class);

    @Bean
    KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>> kafkaListenerContainerFactory(
            ConsumerFactory<Integer, String> consumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<Integer, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(defaultErrorHandler());
        return factory;
    }

    @Bean
    DefaultErrorHandler defaultErrorHandler() {
        var errorHandler = new DefaultErrorHandler(
                (consumerRecord, exception) -> log.error(
                        "Skipping unreadable record. topic={}, partition={}, offset={}",
                        consumerRecord.topic(),
                        consumerRecord.partition(),
                        consumerRecord.offset(),
                        exception),
                new FixedBackOff(0L, 0L));
        errorHandler.addNotRetryableExceptions(DeserializationException.class, SerializationException.class);
        errorHandler.setCommitRecovered(true);
        return errorHandler;
    }
}
