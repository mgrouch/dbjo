package org.github.dbjo.kafka.listener.app;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.github.dbjo.kafka.MutablePartitionKey;
import org.github.dbjo.kafka.avro.OrderEvent;
import org.github.dbjo.kafka.listener.TransactionalConsumePublishProcessor;
import org.github.dbjo.kafka.outbox.OutboxTransactionExecutor;
import org.github.dbjo.kafka.publisher.KafkaEventPublisher;
import org.github.dbjo.kafka.publisher.KafkaPublishCommand;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class PartitionedOrderEventListenerRunner implements CommandLineRunner {
    private final OrderEventListenerProperties properties;

    public PartitionedOrderEventListenerRunner(OrderEventListenerProperties properties) {
        this.properties = properties;
    }

    @Override
    public void run(String... args) {
        Properties publisherProps = new Properties();
        publisherProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        publisherProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            properties.getProducerTransactionalIdPrefix() + "-p" + properties.getPartition());
        publisherProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        try (
            KafkaOrderEventListener listener = new KafkaOrderEventListener(
                properties.getBootstrapServers(),
                properties.getTopic(),
                properties.getGroupId(),
                properties.getPartition(),
                properties.getPartitionCount());
            KafkaEventPublisher<OrderEvent> publisher = new KafkaEventPublisher<>(
                publisherProps,
                properties.getOutputTopic(),
                properties.getPartitionCount(),
                OrderEvent.getClassSchema())
        ) {
            OutboxTransactionExecutor dbTx = new OutboxTransactionExecutor() {
                @Override
                public <T> T inTransaction(java.util.function.Supplier<T> work) {
                    return work.get();
                }
            };
            TransactionalConsumePublishProcessor<OrderEvent, OrderEvent> processor =
                new TransactionalConsumePublishProcessor<>(listener, publisher, dbTx, properties.getPollTimeout());

            AtomicLong iterations = new AtomicLong();
            while (!Thread.currentThread().isInterrupted()) {
                processor.pollAndProcess(events -> toOutputCommands(events, iterations.incrementAndGet()));
                if (properties.getMaxPollIterations() > 0 && iterations.get() >= properties.getMaxPollIterations()) {
                    return;
                }
            }
        }
    }

    private List<KafkaPublishCommand<OrderEvent>> toOutputCommands(
        List<org.github.dbjo.kafka.listener.PartitionedKafkaEvent<OrderEvent>> events,
        long iteration
    ) {
        return events.stream()
            .map(event -> new KafkaPublishCommand<>(
                "listener-" + iteration + "-" + event.offset(),
                event.event(),
                new MutablePartitionKey(event.getPartitionKey())
            ))
            .toList();
    }
}
