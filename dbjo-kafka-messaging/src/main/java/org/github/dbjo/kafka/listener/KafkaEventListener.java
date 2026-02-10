package org.github.dbjo.kafka.listener;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.github.dbjo.meta.features.PartitionId;

public class KafkaEventListener<T extends SpecificRecord> implements AutoCloseable {
    private final KafkaConsumer<String, byte[]> consumer;
    private final TopicPartition topicPartition;
    private final int partitionCount;
    private final Schema schema;

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
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
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
        if (timeout == null) {
            throw new IllegalArgumentException("timeout must not be null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }

        ConsumerRecords<String, byte[]> records = consumer.poll(timeout);
        List<PartitionedKafkaEvent<T>> events = new ArrayList<>();
        long lastOffset = -1L;
        for (ConsumerRecord<String, byte[]> record : records.records(topicPartition)) {
            T event = deserialize(record.value(), schema);
            assertExpectedPartition(record.partition(), record.key());
            PartitionedKafkaEvent<T> partitionedEvent =
                    new PartitionedKafkaEvent<>(record.partition(), record.offset(), record.key(), record.timestamp(), event);
            handler.accept(partitionedEvent);
            events.add(partitionedEvent);
            lastOffset = record.offset();
        }

        if (lastOffset >= 0) {
            consumer.commitSync();
        }

        return events;
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
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
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

    private static <T extends SpecificRecord> T deserialize(byte[] payload, Schema schema) {
        Objects.requireNonNull(payload, "payload must not be null");
        SpecificDatumReader<T> reader = new SpecificDatumReader<>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
        try {
            return reader.read(null, decoder);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to deserialize event", ex);
        }
    }
}
