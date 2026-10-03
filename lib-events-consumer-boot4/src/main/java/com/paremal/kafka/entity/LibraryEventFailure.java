package com.paremal.kafka.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import java.time.LocalDateTime;

/**
 * JPA entity holding the full details of a Kafka record that failed processing.
 */
@Entity
@Table(name = "library_event_failure")
public class LibraryEventFailure {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "failure_id")
    private Long failureId;

    @Column(name = "topic", nullable = false)
    private String topic;

    @Column(name = "partition_id", nullable = false)
    private Integer partitionId;

    @Column(name = "offset_value", nullable = false)
    private Long offsetValue;

    @Column(name = "record_key")
    private String recordKey;

    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "exception_class", nullable = false)
    private String exceptionClass;

    @Column(name = "exception_message")
    private String exceptionMessage;

    @Column(name = "stack_trace", nullable = false)
    private String stackTrace;

    @Column(name = "root_cause_class")
    private String rootCauseClass;

    @Column(name = "root_cause_message")
    private String rootCauseMessage;

    @Column(name = "record_timestamp")
    private Long recordTimestamp;

    @Column(name = "record_headers")
    private String recordHeaders;

    @Column(name = "failed_at", nullable = false)
    private LocalDateTime failedAt;

    @PrePersist
    protected void onCreate() {
        if (failedAt == null) {
            failedAt = LocalDateTime.now();
        }
    }

    public Long getFailureId() {
        return failureId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Integer getPartitionId() {
        return partitionId;
    }

    public void setPartitionId(Integer partitionId) {
        this.partitionId = partitionId;
    }

    public Long getOffsetValue() {
        return offsetValue;
    }

    public void setOffsetValue(Long offsetValue) {
        this.offsetValue = offsetValue;
    }

    public String getRecordKey() {
        return recordKey;
    }

    public void setRecordKey(String recordKey) {
        this.recordKey = recordKey;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getExceptionClass() {
        return exceptionClass;
    }

    public void setExceptionClass(String exceptionClass) {
        this.exceptionClass = exceptionClass;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public void setExceptionMessage(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    public String getRootCauseClass() {
        return rootCauseClass;
    }

    public void setRootCauseClass(String rootCauseClass) {
        this.rootCauseClass = rootCauseClass;
    }

    public String getRootCauseMessage() {
        return rootCauseMessage;
    }

    public void setRootCauseMessage(String rootCauseMessage) {
        this.rootCauseMessage = rootCauseMessage;
    }

    public Long getRecordTimestamp() {
        return recordTimestamp;
    }

    public void setRecordTimestamp(Long recordTimestamp) {
        this.recordTimestamp = recordTimestamp;
    }

    public String getRecordHeaders() {
        return recordHeaders;
    }

    public void setRecordHeaders(String recordHeaders) {
        this.recordHeaders = recordHeaders;
    }

    public LocalDateTime getFailedAt() {
        return failedAt;
    }
}
