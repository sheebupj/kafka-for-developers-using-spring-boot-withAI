ALTER TABLE library_event_failure
    ADD COLUMN root_cause_class    VARCHAR(500),
    ADD COLUMN root_cause_message  TEXT,
    ADD COLUMN record_timestamp    BIGINT,
    ADD COLUMN record_headers      TEXT;
