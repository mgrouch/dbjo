package org.github.dbjo.kafka.listener;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.github.dbjo.kafka.avro.OrderEvent;

public class KafkaOrderEventListener implements AutoCloseable {
    private final KafkaConsumer<String, byte[]> consumer;
    private final String topic;

    public KafkaOrderEventListener(String bootstrapServers, String topic, String groupId) {
        this(defaultProperties(bootstrapServers, groupId), topic);
    }

    public KafkaOrderEventListener(Properties properties, String topic) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be null or blank");
        }
        this.consumer = new KafkaConsumer<>(properties);
        this.topic = topic;
        this.consumer.subscribe(List.of(topic));
    }

    public List<OrderEvent> listen(Duration timeout) {
        return listen(timeout, this::onMessage);
    }

    public List<OrderEvent> listen(Duration timeout, Consumer<OrderEvent> handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }
        List<PartitionedOrderEvent> records = listenPartitioned(timeout, this::onPartitionedMessage);
        List<OrderEvent> events = new ArrayList<>(records.size());
        for (PartitionedOrderEvent partitionedRecord : records) {
            OrderEvent event = partitionedRecord.event();
            handler.accept(event);
            events.add(event);
        }
        return events;
    }

    public List<PartitionedOrderEvent> listenPartitioned(Duration timeout) {
        return listenPartitioned(timeout, this::onPartitionedMessage);
    }

    public List<PartitionedOrderEvent> listenPartitioned(
            Duration timeout, Consumer<PartitionedOrderEvent> handler) {
        if (timeout == null) {
            throw new IllegalArgumentException("timeout must not be null");
        }
        if (handler == null) {
            throw new IllegalArgumentException("handler must not be null");
        }
        ConsumerRecords<String, byte[]> records = consumer.poll(timeout);
        List<PartitionedOrderEvent> events = new ArrayList<>();
        for (ConsumerRecord<String, byte[]> record : records) {
            OrderEvent event = deserialize(record.value());
            PartitionedOrderEvent partitionedEvent = new PartitionedOrderEvent(
                    record.partition(), record.offset(), record.key(), record.timestamp(), event);
            handler.accept(partitionedEvent);
            events.add(partitionedEvent);
        }
        return events;
    }

    public void onMessage(OrderEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
    }

    public void onPartitionedMessage(PartitionedOrderEvent event) {
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
        return properties;
    }

    private static OrderEvent deserialize(byte[] payload) {
        Objects.requireNonNull(payload, "payload must not be null");
        SpecificDatumReader<OrderEvent> reader = new SpecificDatumReader<>(OrderEvent.getClassSchema());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
        try {
            return reader.read(null, decoder);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to deserialize OrderEvent", ex);
        }
    }
}
