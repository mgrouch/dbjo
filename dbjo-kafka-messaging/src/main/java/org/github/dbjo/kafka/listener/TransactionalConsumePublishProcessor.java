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
 * Processes a consumed batch with local DB transaction and Kafka transaction,
 * including atomic offset commit with produced messages on Kafka side.
 *
 * <p>Flow per poll:
 * <ol>
 *   <li>Poll listener without committing offsets.</li>
 *   <li>Run DB work in local transaction and produce output commands.</li>
 *   <li>Publish outputs + commit consumed offsets in one Kafka transaction.</li>
 * </ol>
 *
 * <p>Important: DB and Kafka are still separate systems (no distributed XA transaction).
 */
public class TransactionalConsumePublishProcessor<TIn extends SpecificRecord, TOut extends SpecificRecord> {
    private final KafkaEventListener<TIn> listener;
    private final KafkaEventPublisher<TOut> publisher;
    private final OutboxTransactionExecutor dbTransaction;
    private final Duration pollTimeout;

    public TransactionalConsumePublishProcessor(
        KafkaEventListener<TIn> listener,
        KafkaEventPublisher<TOut> publisher,
        OutboxTransactionExecutor dbTransaction,
        Duration pollTimeout
    ) {
        this.listener = Objects.requireNonNull(listener, "listener must not be null");
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
        this.dbTransaction = Objects.requireNonNull(dbTransaction, "dbTransaction must not be null");
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout must not be null");
    }

    public List<KafkaPublishReceipt> pollAndProcess(
        Function<List<PartitionedKafkaEvent<TIn>>, List<KafkaPublishCommand<TOut>>> work
    ) {
        Objects.requireNonNull(work, "work must not be null");
        ConsumedKafkaBatch<TIn> batch = listener.pollBatch(pollTimeout);
        if (batch.events().isEmpty()) {
            return List.of();
        }

        List<KafkaPublishCommand<TOut>> publishCommands = dbTransaction.inTransaction(() -> {
            List<KafkaPublishCommand<TOut>> commands = work.apply(batch.events());
            if (commands == null) {
                return List.of();
            }
            return List.copyOf(commands);
        });

        return publisher.publishBatchAndCommitOffsetsInTransaction(
            publishCommands,
            batch.offsetsToCommit(),
            batch.consumerGroupMetadata()
        );
    }
}
