package org.github.dbjo.kafka.outbox.jdbc;

import java.util.Arrays;
import java.util.Objects;

public record OutboxMessage(
    String outboxId,
    long sequenceNo,
    String payloadType,
    String partitionKey,
    byte[] payload,
    long occurredAtEpochMs
) {
    public OutboxMessage {
        if (outboxId == null || outboxId.isBlank()) {
            throw new IllegalArgumentException("outboxId must not be null or blank");
        }
        if (sequenceNo <= 0) {
            throw new IllegalArgumentException("sequenceNo must be > 0");
        }
        if (payloadType == null || payloadType.isBlank()) {
            throw new IllegalArgumentException("payloadType must not be null or blank");
        }
        if (partitionKey == null || partitionKey.isBlank()) {
            throw new IllegalArgumentException("partitionKey must not be null or blank");
        }
        if (payload == null || payload.length == 0) {
            throw new IllegalArgumentException("payload must not be null or empty");
        }
        if (occurredAtEpochMs <= 0) {
            throw new IllegalArgumentException("occurredAtEpochMs must be > 0");
        }
        payload = Arrays.copyOf(payload, payload.length);
    }

    @Override
    public byte[] payload() {
        return Arrays.copyOf(payload, payload.length);
    }

    public static <T> OutboxMessage fromEvent(
        String outboxId,
        long sequenceNo,
        T event,
        String payloadType,
        String partitionKey,
        long occurredAtEpochMs,
        OutboxEventCodec<T> codec
    ) {
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(codec, "codec must not be null");
        return new OutboxMessage(
            outboxId,
            sequenceNo,
            payloadType,
            partitionKey,
            codec.encode(event),
            occurredAtEpochMs
        );
    }
}
