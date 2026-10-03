package com.paremal.kafka.config;

import com.paremal.kafka.entity.LibraryEventFailure;
import com.paremal.kafka.repository.LibraryEventFailureRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.stereotype.Component;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Recovers failed records by persisting the complete failure details to the
 * {@code library_event_failure} table.
 */
@Component
public class FailureTableRecoverer implements ConsumerRecordRecoverer {

    private static final Logger log = LoggerFactory.getLogger(FailureTableRecoverer.class);

    private final LibraryEventFailureRepository failureRepository;

    public FailureTableRecoverer(LibraryEventFailureRepository failureRepository) {
        this.failureRepository = failureRepository;
    }

    @Override
    public void accept(ConsumerRecord<?, ?> consumerRecord, Exception exception) {
        var rootCause = NestedExceptionUtils.getMostSpecificCause(exception);

        var failure = new LibraryEventFailure();
        failure.setTopic(consumerRecord.topic());
        failure.setPartitionId(consumerRecord.partition());
        failure.setOffsetValue(consumerRecord.offset());
        failure.setRecordKey(consumerRecord.key() == null ? null : String.valueOf(consumerRecord.key()));
        failure.setPayload(payloadOf(consumerRecord, rootCause));
        failure.setExceptionClass(exception.getClass().getName());
        failure.setExceptionMessage(exception.getMessage());
        failure.setStackTrace(stackTraceOf(exception));
        failure.setRootCauseClass(rootCause.getClass().getName());
        failure.setRootCauseMessage(rootCause.getMessage());
        failure.setRecordTimestamp(consumerRecord.timestamp());
        failure.setRecordHeaders(headersOf(consumerRecord));

        failureRepository.save(failure);
        log.warn("Persisted failed record. topic={}, partition={}, offset={}, error={}",
                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                exception.getMessage());
    }

    private String payloadOf(ConsumerRecord<?, ?> consumerRecord, Throwable rootCause) {
        if (consumerRecord.value() != null) {
            return String.valueOf(consumerRecord.value());
        }
        if (rootCause instanceof DeserializationException deserializationException
                && deserializationException.getData() != null) {
            return new String(deserializationException.getData(), StandardCharsets.UTF_8);
        }
        return "null";
    }

    private String stackTraceOf(Throwable throwable) {
        var writer = new StringWriter();
        throwable.printStackTrace(new PrintWriter(writer));
        return writer.toString();
    }

    private String headersOf(ConsumerRecord<?, ?> consumerRecord) {
        return StreamSupport.stream(consumerRecord.headers().spliterator(), false)
                .map(this::headerToString)
                .collect(Collectors.joining(", "));
    }

    private String headerToString(Header header) {
        var value = header.value() == null ? "" : new String(header.value(), StandardCharsets.UTF_8);
        return header.key() + "=" + value;
    }
}
