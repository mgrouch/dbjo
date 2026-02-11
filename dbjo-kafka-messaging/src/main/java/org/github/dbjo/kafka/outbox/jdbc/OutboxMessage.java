package org.github.dbjo.kafka.outbox.jdbc;

import java.util.Objects;

public record OutboxMessage<T>(
    String outboxId,
    long sequenceNo,
    String partitionKey,
    T event,
    long occurredAtEpochMs
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
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        if (occurredAtEpochMs <= 0) {
            throw new IllegalArgumentException("occurredAtEpochMs must be > 0");
        }
    }

    public static <T> OutboxMessage<T> fromEvent(
        String outboxId,
        long sequenceNo,
        T event,
        String partitionKey,
        long occurredAtEpochMs
    ) {
        Objects.requireNonNull(event, "event must not be null");
        return new OutboxMessage<>(
            outboxId,
            sequenceNo,
            partitionKey,
            event,
            occurredAtEpochMs
        );
    }
}
