package org.github.dbjo.kafka.listener.app;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.github.dbjo.kafka.listener.KafkaOrderEventListener;
import org.github.dbjo.kafka.listener.PartitionedKafkaEvent;
import org.github.dbjo.kafka.avro.OrderEvent;

public abstract class PartitionedKafkaListenerWorker {
    private final KafkaOrderEventListener listener;
    private final Duration pollTimeout;

    protected PartitionedKafkaListenerWorker(KafkaOrderEventListener listener, Duration pollTimeout) {
        this.listener = Objects.requireNonNull(listener, "listener must not be null");
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout must not be null");
        if (pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be greater than 0");
        }
    }

    public final void pollOnce() {
        List<PartitionedKafkaEvent<OrderEvent>> events = listener.listenPartitioned(pollTimeout);
        if (events.isEmpty()) {
            onEmptyBatch();
            return;
        }
        processBatch(events);
        for (PartitionedKafkaEvent<OrderEvent> event : events) {
            processMessage(event);
        }
    }

    public final void run(long maxPollIterations) {
        if (maxPollIterations == 0) {
            throw new IllegalArgumentException("maxPollIterations must not be 0");
        }

        long iteration = 0;
        while (!Thread.currentThread().isInterrupted()) {
            pollOnce();
            iteration++;
            if (maxPollIterations > 0 && iteration >= maxPollIterations) {
                return;
            }
        }
    }

    protected void onEmptyBatch() {
        // Default no-op.
    }

    protected abstract void processBatch(List<PartitionedKafkaEvent<OrderEvent>> events);

    protected abstract void processMessage(PartitionedKafkaEvent<OrderEvent> event);
}
