package org.github.dbjo.kafka.outbox.jdbc;

import java.util.Objects;
import org.github.dbjo.kafka.avro.OrderEvent;

public record OutboxMessage(
    String outboxId,
    long sequenceNo,
    String eventId,
    String productId,
    String eventType,
    long occurredAtEpochMs
) {
    public OutboxMessage {
        if (outboxId == null || outboxId.isBlank()) {
            throw new IllegalArgumentException("outboxId must not be null or blank");
        }
        if (sequenceNo <= 0) {
            throw new IllegalArgumentException("sequenceNo must be > 0");
        }
        if (eventId == null || eventId.isBlank()) {
            throw new IllegalArgumentException("eventId must not be null or blank");
        }
        if (productId == null || productId.isBlank()) {
            throw new IllegalArgumentException("productId must not be null or blank");
        }
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType must not be null or blank");
        }
        if (occurredAtEpochMs <= 0) {
            throw new IllegalArgumentException("occurredAtEpochMs must be > 0");
        }
    }

    public OutboxMessage(String outboxId, long sequenceNo, OrderEvent event) {
        this(
            outboxId,
            sequenceNo,
            Objects.requireNonNull(event, "event must not be null").getEventId().toString(),
            event.getProductId().toString(),
            event.getEventType().toString(),
            event.getOccurredAtEpochMs()
        );
    }

    public OrderEvent toOrderEvent() {
        return new OrderEvent(eventId, productId, eventType, occurredAtEpochMs);
    }
}
