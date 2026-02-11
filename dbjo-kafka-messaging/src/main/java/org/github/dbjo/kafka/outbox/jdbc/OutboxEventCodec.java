package org.github.dbjo.kafka.outbox.jdbc;

/**
 * Encodes and decodes outbox event payloads stored in DB.
 */
public interface OutboxEventCodec<T> {
    byte[] encode(T event);

    T decode(byte[] payload);
}
