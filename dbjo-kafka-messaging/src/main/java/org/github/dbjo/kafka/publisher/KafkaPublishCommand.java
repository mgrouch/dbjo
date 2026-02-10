package org.github.dbjo.kafka.publisher;

import java.util.Objects;
import org.apache.avro.specific.SpecificRecord;
import org.github.dbjo.meta.features.Partitioned;

public record KafkaPublishCommand<T extends SpecificRecord>(String outboxId, T event, Partitioned partitioned) {
    public KafkaPublishCommand {
        if (outboxId == null || outboxId.isBlank()) {
            throw new IllegalArgumentException("outboxId must not be null or blank");
        }
        Objects.requireNonNull(event, "event must not be null");
        Objects.requireNonNull(partitioned, "partitioned must not be null");
    }
}
