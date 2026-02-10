package org.github.dbjo.kafka.outbox;

import java.util.function.Supplier;

/**
 * Abstraction over DB transaction execution for outbox state changes.
 *
 * <p>Can be implemented with Spring's {@code TransactionTemplate}, JTA, or custom
 * transaction managers.
 */
@FunctionalInterface
public interface OutboxTransactionExecutor {
    <T> T inTransaction(Supplier<T> work);
}
