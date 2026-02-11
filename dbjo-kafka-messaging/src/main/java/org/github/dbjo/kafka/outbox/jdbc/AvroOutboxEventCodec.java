package org.github.dbjo.kafka.outbox.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

public final class AvroOutboxEventCodec<T extends SpecificRecord> implements OutboxEventCodec<T> {
    private final Schema schema;
    private final Supplier<T> emptyRecordSupplier;

    public AvroOutboxEventCodec(Schema schema, Supplier<T> emptyRecordSupplier) {
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        this.emptyRecordSupplier = Objects.requireNonNull(emptyRecordSupplier, "emptyRecordSupplier must not be null");
    }

    @Override
    public byte[] encode(T event) {
        Objects.requireNonNull(event, "event must not be null");
        SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(schema);
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(output, null);
            writer.write(event, encoder);
            encoder.flush();
            return output.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to encode outbox event", ex);
        }
    }

    @Override
    public T decode(byte[] payload) {
        if (payload == null || payload.length == 0) {
            throw new IllegalArgumentException("payload must not be null or empty");
        }
        try {
            T target = emptyRecordSupplier.get();
            SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
            return reader.read(target, decoder);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to decode outbox event", ex);
        }
    }
}
