package org.github.dbjo.kafka.outbox;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.avro.specific.SpecificRecord;
import org.github.dbjo.kafka.publisher.KafkaEventPublisher;
import org.github.dbjo.kafka.publisher.KafkaPublishCommand;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;

/**
 * Coordinates the standard outbox flow:
 * <ol>
 *     <li>Persist business changes + outbox rows in one DB transaction.</li>
 *     <li>Publish outbox rows to Kafka (single or batch).</li>
 *     <li>Persist Kafka partition/offset in DB in a separate transaction.</li>
 * </ol>
 *
 * <p>Kafka and relational DB do not share a single ACID transaction in this module.
 * The API reflects that by keeping transaction boundaries explicit and separate.
 */
public class KafkaOutboxCoordinator<T extends SpecificRecord> {
    private final KafkaEventPublisher<T> publisher;

    public KafkaOutboxCoordinator(KafkaEventPublisher<T> publisher) {
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
    }

    public KafkaPublishReceipt publishOneAndMark(
        OutboxTransactionExecutor tx,
        OutboxStateStore stateStore,
        KafkaPublishCommand<T> command
    ) {
        Objects.requireNonNull(tx, "tx must not be null");
        Objects.requireNonNull(stateStore, "stateStore must not be null");
        Objects.requireNonNull(command, "command must not be null");
        KafkaPublishReceipt receipt = publisher.publishBatchInTransaction(List.of(command)).getFirst();
        tx.inTransaction(() -> {
            stateStore.markPublished(List.of(receipt));
            return null;
        });
        return receipt;
    }

    public List<KafkaPublishReceipt> publishBatchAndMark(
        OutboxTransactionExecutor tx,
        OutboxStateStore stateStore,
        Collection<KafkaPublishCommand<T>> commands
    ) {
        Objects.requireNonNull(tx, "tx must not be null");
        Objects.requireNonNull(stateStore, "stateStore must not be null");
        Objects.requireNonNull(commands, "commands must not be null");
        List<KafkaPublishCommand<T>> commandList = List.copyOf(commands);
        List<KafkaPublishReceipt> receipts = publisher.publishBatchInTransaction(commandList);
        tx.inTransaction(() -> {
            stateStore.markPublished(receipts);
            return null;
        });
        return receipts;
    }
}
