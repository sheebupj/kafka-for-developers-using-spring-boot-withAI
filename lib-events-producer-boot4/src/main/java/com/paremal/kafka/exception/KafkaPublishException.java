package com.paremal.kafka.exception;

public class KafkaPublishException extends RuntimeException {
    public KafkaPublishException(String message, Throwable cause) {
        super(message, cause);
    }

    public KafkaPublishException(String message) {
        super(message);
    }
}
