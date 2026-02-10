package org.github.dbjo.kafka.outbox;

import java.util.function.Supplier;

@FunctionalInterface
public interface OutboxTransactionExecutor {
    <T> T inTransaction(Supplier<T> work);
}
