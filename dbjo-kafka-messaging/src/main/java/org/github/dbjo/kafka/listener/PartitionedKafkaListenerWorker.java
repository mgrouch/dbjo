package org.github.dbjo.kafka.listener;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.avro.specific.SpecificRecord;

public abstract class PartitionedKafkaListenerWorker<T extends SpecificRecord> {
    private final KafkaEventListener<T> listener;
    private final Duration pollTimeout;

    protected PartitionedKafkaListenerWorker(KafkaEventListener<T> listener, Duration pollTimeout) {
        this.listener = Objects.requireNonNull(listener, "listener must not be null");
        this.pollTimeout = Objects.requireNonNull(pollTimeout, "pollTimeout must not be null");
        if (pollTimeout.isNegative() || pollTimeout.isZero()) {
            throw new IllegalArgumentException("pollTimeout must be greater than 0");
        }
    }

    public final void pollOnce() {
        List<PartitionedKafkaEvent<T>> events = listener.listenPartitioned(pollTimeout);
        if (events.isEmpty()) {
            onEmptyBatch();
            return;
        }
        processBatch(events);
        for (PartitionedKafkaEvent<T> event : events) {
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

    protected abstract void processBatch(List<PartitionedKafkaEvent<T>> events);

    protected abstract void processMessage(PartitionedKafkaEvent<T> event);
}
