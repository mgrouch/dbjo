package org.github.dbjo.kafka.listener;

import java.util.List;
import java.util.Map;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.github.dbjo.kafka.publisher.KafkaPublishCommand;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;

/**
 * Persists consume-side offsets and publish commands in an outbox store.
 *
 * <p>Implementations are expected to persist consumed offsets together with business changes and
 * outbox rows in one DB transaction, then expose pending rows for publishing and finally mark
 * published rows with Kafka metadata in a separate DB transaction.
 */
public interface ConsumeOutboxStore<T extends SpecificRecord> {
    void saveConsumedOffsetsAndCommands(
        Map<TopicPartition, OffsetAndMetadata> consumedOffsets,
        List<KafkaPublishCommand<T>> commands
    );

    List<KafkaPublishCommand<T>> loadPendingPublishCommands(int batchSize);

    void markPublished(List<KafkaPublishReceipt> receipts);
}
