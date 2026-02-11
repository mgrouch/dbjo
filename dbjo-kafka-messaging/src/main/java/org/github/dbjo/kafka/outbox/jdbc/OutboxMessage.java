package org.github.dbjo.kafka.outbox.jdbc;

import java.util.Objects;
import org.apache.avro.specific.SpecificRecord;

public record OutboxMessage<T extends SpecificRecord>(
    String outboxId,
    long sequenceNo,
    String partitionKey,
    T event
) {
    public OutboxMessage {
        if (outboxId == null || outboxId.isBlank()) {
            throw new IllegalArgumentException("outboxId must not be null or blank");
        }
        if (sequenceNo <= 0) {
            throw new IllegalArgumentException("sequenceNo must be > 0");
        }
        if (partitionKey == null || partitionKey.isBlank()) {
            throw new IllegalArgumentException("partitionKey must not be null or blank");
        }
        Objects.requireNonNull(event, "event must not be null");
    }
}
