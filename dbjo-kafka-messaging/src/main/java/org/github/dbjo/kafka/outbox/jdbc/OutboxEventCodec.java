package org.github.dbjo.kafka.outbox.jdbc;

import org.apache.avro.specific.SpecificRecord;

/**
 * Encodes and decodes outbox event payloads stored in DB.
 */
public interface OutboxEventCodec<T extends SpecificRecord> {
    byte[] encode(T event);

    T decode(byte[] payload);
}
