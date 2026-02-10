package org.github.dbjo.kafka.outbox.spring;

import java.util.Objects;
import java.util.function.Supplier;
import org.github.dbjo.kafka.outbox.OutboxTransactionExecutor;
import org.springframework.transaction.support.TransactionOperations;

/**
 * Spring adapter for {@link OutboxTransactionExecutor}.
 *
 * <p>Typical usage:
 * <pre>{@code
 * TransactionTemplate template = new TransactionTemplate(txManager);
 * OutboxTransactionExecutor tx = SpringOutboxTransactionExecutor.of(template);
 * }</pre>
 */
public final class SpringOutboxTransactionExecutor implements OutboxTransactionExecutor {
    private final TransactionOperations tx;

    private SpringOutboxTransactionExecutor(TransactionOperations tx) {
        this.tx = Objects.requireNonNull(tx, "tx must not be null");
    }

    public static SpringOutboxTransactionExecutor of(TransactionOperations tx) {
        return new SpringOutboxTransactionExecutor(tx);
    }

    @Override
    public <T> T inTransaction(Supplier<T> work) {
        Objects.requireNonNull(work, "work must not be null");
        return tx.execute(status -> work.get());
    }
}
