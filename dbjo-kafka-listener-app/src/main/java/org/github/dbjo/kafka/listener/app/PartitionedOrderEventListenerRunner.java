package org.github.dbjo.kafka.listener.app;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.github.dbjo.kafka.MutablePartitionKey;
import org.github.dbjo.kafka.avro.OrderEvent;
import org.github.dbjo.kafka.listener.ConsumeOutboxStore;
import org.github.dbjo.kafka.listener.TransactionalConsumePublishProcessor;
import org.github.dbjo.kafka.outbox.OutboxTransactionExecutor;
import org.github.dbjo.kafka.publisher.KafkaEventPublisher;
import org.github.dbjo.kafka.publisher.KafkaPublishCommand;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class PartitionedOrderEventListenerRunner implements CommandLineRunner {
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";

    private final OrderEventListenerProperties properties;

    public PartitionedOrderEventListenerRunner(OrderEventListenerProperties properties) {
        this.properties = properties;
    }

    @Override
    public void run(String... args) {
        Properties listenerProps = new Properties();
        listenerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        listenerProps.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getGroupId());
        listenerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, properties.getConsumerKeyDeserializerClass());
        listenerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, properties.getConsumerValueDeserializerClass());
        listenerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        listenerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        listenerProps.put(SPECIFIC_AVRO_READER_CONFIG, String.valueOf(properties.isConsumerSpecificAvroReader()));
        listenerProps.put(SCHEMA_REGISTRY_URL_CONFIG, properties.getSchemaRegistryUrl());

        Properties publisherProps = new Properties();
        publisherProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        publisherProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            properties.getProducerTransactionalIdPrefix() + "-p" + properties.getPartition());
        publisherProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
            String.valueOf(properties.isProducerEnableIdempotence()));
        publisherProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties.getProducerKeySerializerClass());
        publisherProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getProducerValueSerializerClass());
        publisherProps.put(SCHEMA_REGISTRY_URL_CONFIG, properties.getSchemaRegistryUrl());

        try (
            KafkaOrderEventListener listener = new KafkaOrderEventListener(
                listenerProps,
                properties.getTopic(),
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
            ConsumeOutboxStore<OrderEvent> outboxStore = new InMemoryConsumeOutboxStore();
            TransactionalConsumePublishProcessor<OrderEvent, OrderEvent> processor =
                new TransactionalConsumePublishProcessor<>(
                    listener,
                    publisher,
                    outboxStore,
                    dbTx,
                    properties.getPollTimeout(),
                    500
                );

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

    private static final class InMemoryConsumeOutboxStore implements ConsumeOutboxStore<OrderEvent> {
        private final ConcurrentLinkedQueue<KafkaPublishCommand<OrderEvent>> outbox = new ConcurrentLinkedQueue<>();
        private final Map<TopicPartition, OffsetAndMetadata> consumedOffsets = new ConcurrentHashMap<>();

        @Override
        public void saveConsumedOffsetsAndCommands(
            Map<TopicPartition, OffsetAndMetadata> offsets,
            List<KafkaPublishCommand<OrderEvent>> commands
        ) {
            consumedOffsets.putAll(offsets);
            outbox.addAll(commands);
        }

        @Override
        public List<KafkaPublishCommand<OrderEvent>> loadPendingPublishCommands(int batchSize) {
            java.util.ArrayList<KafkaPublishCommand<OrderEvent>> batch = new java.util.ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                KafkaPublishCommand<OrderEvent> command = outbox.poll();
                if (command == null) {
                    break;
                }
                batch.add(command);
            }
            return batch;
        }

        @Override
        public void markPublished(List<KafkaPublishReceipt> receipts) {
            // no-op for demo app; real implementation persists partition/offset metadata in DB.
        }
    }
}
