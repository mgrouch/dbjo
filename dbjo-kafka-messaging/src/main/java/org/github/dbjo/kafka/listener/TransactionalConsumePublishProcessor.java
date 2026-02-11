package org.github.dbjo.kafka.listener;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.apache.avro.specific.SpecificRecord;
import org.github.dbjo.kafka.outbox.OutboxTransactionExecutor;
import org.github.dbjo.kafka.publisher.KafkaEventPublisher;
import org.github.dbjo.kafka.publisher.KafkaPublishCommand;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;

/**
 * Processes consumed records via explicit outbox flow with separated consume and publish
 * transactions.
 *
 * <p>Flow per poll:
 * <ol>
 *   <li>Poll listener without committing offsets.</li>
 *   <li>Run DB work in local transaction and persist consumed offsets + outbox commands.</li>
 *   <li>In a separate DB transaction, load pending outbox rows for publishing.</li>
 *   <li>Publish rows in Kafka transaction.</li>
 *   <li>In a separate DB transaction, persist published Kafka metadata.</li>
 * </ol>
 *
 * <p>Important: DB and Kafka are still separate systems (no distributed XA transaction),
 * so this class follows the outbox pattern with durable retries on restart.
 */
public class TransactionalConsumePublishProcessor<TIn extends SpecificRecord, TOut extends SpecificRecord> {
    private final KafkaEventListener<TIn> listener;
    private final KafkaEventPublisher<TOut> publisher;
    private final ConsumeOutboxStore<TOut> outboxStore;
    private final OutboxTransactionExecutor dbTransaction;
    private final Duration pollTimeout;
    private final int publishBatchSize;

    public TransactionalConsumePublishProcessor(
        KafkaEventListener<TIn> listener,
        KafkaEventPublisher<TOut> publisher,
        ConsumeOutboxStore<TOut> outboxStore,
        OutboxTransactionExecutor dbTransaction,
        Duration pollTimeout,
        int publishBatchSize
    ) {
        this.listener = Objects.requireNonNull(listener, "listener must not be null");
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.outboxStore = Objects.requireNonNull(outboxStore, "outboxStore must not be null");
        this.dbTransaction = Objects.requireNonNull(dbTransaction, "dbTransaction must not be null");
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout must not be null");
        if (publishBatchSize <= 0) {
            throw new IllegalArgumentException("publishBatchSize must be greater than 0");
        }
        this.publishBatchSize = publishBatchSize;
    }

    public List<KafkaPublishReceipt> pollAndProcess(
        Function<List<PartitionedKafkaEvent<TIn>>, List<KafkaPublishCommand<TOut>>> work
    ) {
        Objects.requireNonNull(work, "work must not be null");
        ConsumedKafkaBatch<TIn> batch = listener.pollBatch(pollTimeout);
        if (batch.events().isEmpty()) {
            return List.of();
        }

        dbTransaction.inTransaction(() -> {
            List<KafkaPublishCommand<TOut>> commands = work.apply(batch.events());
            List<KafkaPublishCommand<TOut>> normalized = commands == null ? List.of() : List.copyOf(commands);
            outboxStore.saveConsumedOffsetsAndCommands(batch.offsetsToCommit(), normalized);
            return null;
        });

        return publishPendingFromOutbox();
    }

    public List<KafkaPublishReceipt> publishPendingFromOutbox() {
        List<KafkaPublishCommand<TOut>> publishCommands = dbTransaction.inTransaction(
            () -> List.copyOf(outboxStore.loadPendingPublishCommands(publishBatchSize))
        );
        if (publishCommands.isEmpty()) {
            return List.of();
        }

        List<KafkaPublishReceipt> receipts = publisher.publishBatchInTransaction(publishCommands);
        dbTransaction.inTransaction(() -> {
            outboxStore.markPublished(receipts);
            return null;
        });
        return receipts;
    }
}
