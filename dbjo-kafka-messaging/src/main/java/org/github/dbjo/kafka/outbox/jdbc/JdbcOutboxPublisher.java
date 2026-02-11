package org.github.dbjo.kafka.outbox.jdbc;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.avro.specific.SpecificRecord;
import org.github.dbjo.kafka.MutablePartitionKey;
import org.github.dbjo.kafka.publisher.KafkaEventPublisher;
import org.github.dbjo.kafka.publisher.KafkaPublishCommand;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Polls outbox rows in order, publishes to Kafka, then marks rows as published.
 */
public class JdbcOutboxPublisher<T extends SpecificRecord> {
    private final JdbcOutboxStore store;
    private final KafkaEventPublisher<T> publisher;
    private final OutboxEventCodec<T> codec;
    private final TransactionTemplate transactionTemplate;
    private final String lockOwner;

    public JdbcOutboxPublisher(
        JdbcOutboxStore store,
        KafkaEventPublisher<T> publisher,
        OutboxEventCodec<T> codec,
        TransactionTemplate transactionTemplate
    ) {
        this(store, publisher, codec, transactionTemplate, UUID.randomUUID().toString());
    }

    public JdbcOutboxPublisher(
        JdbcOutboxStore store,
        KafkaEventPublisher<T> publisher,
        OutboxEventCodec<T> codec,
        TransactionTemplate transactionTemplate,
        String lockOwner
    ) {
        this.store = Objects.requireNonNull(store, "store must not be null");
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.codec = Objects.requireNonNull(codec, "codec must not be null");
        this.transactionTemplate = Objects.requireNonNull(transactionTemplate, "transactionTemplate must not be null");
        if (lockOwner == null || lockOwner.isBlank()) {
            throw new IllegalArgumentException("lockOwner must not be null or blank");
        }
        this.lockOwner = lockOwner;
    }

    public List<KafkaPublishReceipt> pollAndPublish(int batchSize) {
        List<OutboxMessage> claimed = transactionTemplate.execute(status -> store.claimNextBatch(batchSize, lockOwner));
        if (claimed == null || claimed.isEmpty()) {
            return List.of();
        }

        List<KafkaPublishCommand<T>> commands = claimed.stream()
            .map(message -> new KafkaPublishCommand<>(
                message.outboxId(),
                codec.decode(message.payload()),
                new MutablePartitionKey(message.partitionKey())
            ))
            .toList();

        List<KafkaPublishReceipt> receipts = publisher.publishBatchInTransaction(commands);
        transactionTemplate.executeWithoutResult(status -> store.markPublished(receipts));
        return receipts;
    }
}
