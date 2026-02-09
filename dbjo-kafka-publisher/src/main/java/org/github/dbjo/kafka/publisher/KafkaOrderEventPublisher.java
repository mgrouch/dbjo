package org.github.dbjo.kafka.publisher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.github.dbjo.kafka.avro.OrderEvent;

public class KafkaOrderEventPublisher implements AutoCloseable {
    private final KafkaProducer<String, byte[]> producer;
    private final String topic;

    public KafkaOrderEventPublisher(String bootstrapServers, String topic) {
        this(defaultProperties(bootstrapServers), topic);
    }

    public KafkaOrderEventPublisher(Properties properties, String topic) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be null or blank");
        }
        this.producer = new KafkaProducer<>(properties);
        this.topic = topic;
    }

    public void publish(OrderEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        byte[] payload = serialize(event);
        String key = Objects.toString(event.getEventId(), null);
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, payload);
        producer.send(record);
    }

    @Override
    public void close() {
        producer.close();
    }

    private static Properties defaultProperties(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be null or blank");
        }
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    private static byte[] serialize(OrderEvent event) {
        SpecificDatumWriter<OrderEvent> writer = new SpecificDatumWriter<>(OrderEvent.getClassSchema());
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            writer.write(event, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to serialize OrderEvent", ex);
        }
    }
}
