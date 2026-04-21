package org.github.dbjo.kafka.listener.app;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.github.dbjo.kafka.MutablePartitionKey;
import org.github.dbjo.kafka.avro.OrderEvent;
import org.github.dbjo.kafka.listener.ConsumeOutboxStore;
import org.github.dbjo.kafka.listener.TransactionalConsumePublishProcessor;
import org.github.dbjo.kafka.outbox.OutboxTransactionExecutor;
import org.github.dbjo.kafka.publisher.KafkaEventPublisher;
import org.github.dbjo.kafka.publisher.KafkaPublishCommand;
import org.springframework.boot.CommandLineRunner;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

@Component
public class PartitionedOrderEventListenerRunner implements CommandLineRunner {
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final String SPECIFIC_AVRO_READER_CONFIG = "specific.avro.reader";

    private final OrderEventListenerProperties properties;
    private final DataSource dataSource;
    private final TransactionTemplate transactionTemplate;

    public PartitionedOrderEventListenerRunner(
        OrderEventListenerProperties properties,
        DataSource dataSource,
        TransactionTemplate transactionTemplate
    ) {
        this.properties = properties;
        this.dataSource = dataSource;
        this.transactionTemplate = transactionTemplate;
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
        addSslConfig(listenerProps);

        Properties publisherProps = new Properties();
        publisherProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        publisherProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
            properties.getProducerTransactionalIdPrefix() + "-p" + properties.getPartition());
        publisherProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
            String.valueOf(properties.isProducerEnableIdempotence()));
        publisherProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties.getProducerKeySerializerClass());
        publisherProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getProducerValueSerializerClass());
        publisherProps.put(SCHEMA_REGISTRY_URL_CONFIG, properties.getSchemaRegistryUrl());
        addSslConfig(publisherProps);

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
                    return transactionTemplate.execute(status -> work.get());
                }
            };
            ConsumeOutboxStore<OrderEvent> outboxStore = new JdbcConsumeOutboxStore(new JdbcDatasource(dataSource));
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

    private void addSslConfig(Properties target) {
        putIfPresent(target, CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, properties.getSecurityProtocol());
        putIfPresent(target, SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, properties.getSslTruststoreLocation());
        putIfPresent(target, SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, properties.getSslTruststorePassword());
        putIfPresent(target, SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, properties.getSslTruststoreType());
        putIfPresent(target, SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, properties.getSslKeystoreLocation());
        putIfPresent(target, SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, properties.getSslKeystorePassword());
        putIfPresent(target, SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, properties.getSslKeystoreType());
        putIfPresent(target, SslConfigs.SSL_KEY_PASSWORD_CONFIG, properties.getSslKeyPassword());
        putIfPresent(
            target,
            SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
            properties.getSslEndpointIdentificationAlgorithm()
        );
    }

    private static void putIfPresent(Properties target, String key, String value) {
        if (value != null && !value.isBlank()) {
            target.put(key, value);
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
