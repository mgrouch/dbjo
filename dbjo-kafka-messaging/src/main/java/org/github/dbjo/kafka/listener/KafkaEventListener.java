package org.github.dbjo.kafka.listener;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.github.dbjo.meta.features.PartitionId;

public class KafkaEventListener<T extends SpecificRecord> implements AutoCloseable {
    static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    static final String SECURITY_PROTOCOL_CONFIG = "security.protocol";
    static final String SSL_TRUSTSTORE_LOCATION_CONFIG = "ssl.truststore.location";
    static final String SSL_TRUSTSTORE_PASSWORD_CONFIG = "ssl.truststore.password";
    static final String SSL_TRUSTSTORE_TYPE_CONFIG = "ssl.truststore.type";
    static final String SSL_KEYSTORE_LOCATION_CONFIG = "ssl.keystore.location";
    static final String SSL_KEYSTORE_PASSWORD_CONFIG = "ssl.keystore.password";
    static final String SSL_KEYSTORE_TYPE_CONFIG = "ssl.keystore.type";
    static final String SSL_KEY_PASSWORD_CONFIG = "ssl.key.password";
    static final String SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG = "ssl.endpoint.identification.algorithm";
    private static final String KAFKA_AVRO_DESERIALIZER = "KafkaAvroDeserializer";
    private static final String KAFKA_SCHEMA_REGISTRY_URL_ENV = "KAFKA_SCHEMA_REGISTRY_URL";
    private static final String KAFKA_AVRO_DESERIALIZER_CLASS = "io.confluent.kafka.serializers.KafkaAvroDeserializer";
    private static final String KAFKA_AVRO_SPECIFIC_READER_CONFIG = "specific.avro.reader";
    private static final String KAFKA_SECURITY_PROTOCOL_ENV = "KAFKA_SECURITY_PROTOCOL";
    private static final String KAFKA_SSL_TRUSTSTORE_LOCATION_ENV = "KAFKA_SSL_TRUSTSTORE_LOCATION";
    private static final String KAFKA_SSL_TRUSTSTORE_PASSWORD_ENV = "KAFKA_SSL_TRUSTSTORE_PASSWORD";
    private static final String KAFKA_SSL_TRUSTSTORE_TYPE_ENV = "KAFKA_SSL_TRUSTSTORE_TYPE";
    private static final String KAFKA_SSL_KEYSTORE_LOCATION_ENV = "KAFKA_SSL_KEYSTORE_LOCATION";
    private static final String KAFKA_SSL_KEYSTORE_PASSWORD_ENV = "KAFKA_SSL_KEYSTORE_PASSWORD";
    private static final String KAFKA_SSL_KEYSTORE_TYPE_ENV = "KAFKA_SSL_KEYSTORE_TYPE";
    private static final String KAFKA_SSL_KEY_PASSWORD_ENV = "KAFKA_SSL_KEY_PASSWORD";
    private static final String KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_ENV = "KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM";

    private final KafkaConsumer<String, T> consumer;
    private final TopicPartition topicPartition;
    private final int partitionCount;

    public KafkaEventListener(String bootstrapServers, String topic, String groupId, int partition, int partitionCount, Schema schema) {
        this(defaultProperties(bootstrapServers, groupId), topic, partition, partitionCount, schema);
    }

    public KafkaEventListener(Properties properties, String topic, int partition, int partitionCount, Schema schema) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be null or blank");
        }
        if (partition < 0) {
            throw new IllegalArgumentException("partition must be greater than or equal to 0");
        }
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be greater than 0");
        }
        if (partition >= partitionCount) {
            throw new IllegalArgumentException("partition must be less than partitionCount");
        }
        Objects.requireNonNull(schema, "schema must not be null");
        applySchemaRegistryUrlIfRequired(properties, System.getenv());
        applySslConfigIfPresent(properties, System.getenv());
        this.consumer = new KafkaConsumer<>(properties);
        this.topicPartition = new TopicPartition(topic, partition);
        this.partitionCount = partitionCount;
        this.consumer.assign(List.of(topicPartition));
    }

    public List<T> listen(Duration timeout) {
        return listen(timeout, this::onMessage);
    }

    public List<T> listen(Duration timeout, Consumer<T> handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }
        List<PartitionedKafkaEvent<T>> records = listenPartitioned(timeout, this::onPartitionedMessage);
        List<T> events = new ArrayList<>(records.size());
        for (PartitionedKafkaEvent<T> partitionedRecord : records) {
            T event = partitionedRecord.event();
            handler.accept(event);
            events.add(event);
        }
        return events;
    }

    public List<PartitionedKafkaEvent<T>> listenPartitioned(Duration timeout) {
        return listenPartitioned(timeout, this::onPartitionedMessage);
    }

    public List<PartitionedKafkaEvent<T>> listenPartitioned(Duration timeout, Consumer<PartitionedKafkaEvent<T>> handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }

        ConsumedKafkaBatch<T> batch = pollBatch(timeout);
        for (PartitionedKafkaEvent<T> event : batch.events()) {
            handler.accept(event);
        }
        if (!batch.offsetsToCommit().isEmpty()) {
            consumer.commitSync(batch.offsetsToCommit());
        }
        return batch.events();
    }

    public ConsumedKafkaBatch<T> pollBatch(Duration timeout) {
        return pollBatch(timeout, this::onPartitionedMessage);
    }

    public ConsumedKafkaBatch<T> pollBatch(Duration timeout, Consumer<PartitionedKafkaEvent<T>> handler) {
        if (timeout == null) {
            throw new IllegalArgumentException("timeout must not be null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }

        ConsumerRecords<String, T> records = consumer.poll(timeout);
        List<PartitionedKafkaEvent<T>> events = new ArrayList<>();
        long lastOffset = -1L;
        for (ConsumerRecord<String, T> record : records.records(topicPartition)) {
            assertExpectedPartition(record.partition(), record.key());
            PartitionedKafkaEvent<T> partitionedEvent =
                    new PartitionedKafkaEvent<>(record.partition(), record.offset(), record.key(), record.timestamp(), record.value());
            handler.accept(partitionedEvent);
            events.add(partitionedEvent);
            lastOffset = record.offset();
        }

        Map<TopicPartition, OffsetAndMetadata> offsets =
            lastOffset >= 0
                ? Map.of(topicPartition, new OffsetAndMetadata(lastOffset + 1))
                : Map.of();
        ConsumerGroupMetadata groupMetadata = consumer.groupMetadata();
        return new ConsumedKafkaBatch<>(List.copyOf(events), offsets, groupMetadata);
    }

    public void onMessage(T event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
    }

    public void onPartitionedMessage(PartitionedKafkaEvent<T> event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
    }

    @Override
    public void close() {
        consumer.close();
    }

    private static Properties defaultProperties(String bootstrapServers, String groupId) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be null or blank");
        }
        if (groupId == null || groupId.isBlank()) {
            throw new IllegalArgumentException("groupId must not be null or blank");
        }
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_AVRO_DESERIALIZER_CLASS);
        properties.put(KAFKA_AVRO_SPECIFIC_READER_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }

    static void applySchemaRegistryUrlIfRequired(Properties properties, Map<String, String> env) {
        Objects.requireNonNull(properties, "properties must not be null");
        String deserializer = Objects.toString(properties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG), "");
        if (!deserializer.contains(KAFKA_AVRO_DESERIALIZER)) {
            return;
        }

        Object configuredSchemaRegistryUrl = properties.get(SCHEMA_REGISTRY_URL_CONFIG);
        if (configuredSchemaRegistryUrl instanceof String schemaRegistryUrl && !schemaRegistryUrl.isBlank()) {
            return;
        }

        String schemaRegistryUrlFromEnv = env.get(KAFKA_SCHEMA_REGISTRY_URL_ENV);
        if (schemaRegistryUrlFromEnv != null && !schemaRegistryUrlFromEnv.isBlank()) {
            properties.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrlFromEnv);
            return;
        }

        throw new IllegalArgumentException("kafka schema registry URL is not set");
    }

    static void applySslConfigIfPresent(Properties properties, Map<String, String> env) {
        Objects.requireNonNull(properties, "properties must not be null");
        putIfMissing(properties, env, SECURITY_PROTOCOL_CONFIG, KAFKA_SECURITY_PROTOCOL_ENV);
        putIfMissing(properties, env, SSL_TRUSTSTORE_LOCATION_CONFIG, KAFKA_SSL_TRUSTSTORE_LOCATION_ENV);
        putIfMissing(properties, env, SSL_TRUSTSTORE_PASSWORD_CONFIG, KAFKA_SSL_TRUSTSTORE_PASSWORD_ENV);
        putIfMissing(properties, env, SSL_TRUSTSTORE_TYPE_CONFIG, KAFKA_SSL_TRUSTSTORE_TYPE_ENV);
        putIfMissing(properties, env, SSL_KEYSTORE_LOCATION_CONFIG, KAFKA_SSL_KEYSTORE_LOCATION_ENV);
        putIfMissing(properties, env, SSL_KEYSTORE_PASSWORD_CONFIG, KAFKA_SSL_KEYSTORE_PASSWORD_ENV);
        putIfMissing(properties, env, SSL_KEYSTORE_TYPE_CONFIG, KAFKA_SSL_KEYSTORE_TYPE_ENV);
        putIfMissing(properties, env, SSL_KEY_PASSWORD_CONFIG, KAFKA_SSL_KEY_PASSWORD_ENV);
        putIfMissing(
            properties,
            env,
            SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG,
            KAFKA_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_ENV
        );
    }

    private static void putIfMissing(Properties properties, Map<String, String> env, String propertyKey, String envKey) {
        Object configured = properties.get(propertyKey);
        if (configured instanceof String configuredValue && !configuredValue.isBlank()) {
            return;
        }

        String envValue = env.get(envKey);
        if (envValue != null && !envValue.isBlank()) {
            properties.put(propertyKey, envValue);
        }
    }

    private void assertExpectedPartition(int actualPartition, String partitionKey) {
        Integer expectedPartition = PartitionId.partition(partitionKey, partitionCount);
        if (expectedPartition == null) {
            throw new IllegalArgumentException("record.key must not be null");
        }
        if (expectedPartition != actualPartition) {
            throw new IllegalStateException(
                    "Received event in unexpected partition. expected=" + expectedPartition + ", actual=" + actualPartition);
        }
    }
}
