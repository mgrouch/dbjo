package org.github.dbjo.kafka.publisher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.github.dbjo.meta.features.PartitionId;
import org.github.dbjo.meta.features.Partitioned;

public class KafkaEventPublisher<T extends SpecificRecord> implements AutoCloseable {
    private final KafkaProducer<String, byte[]> producer;
    private final String topic;
    private final int partitionCount;
    private final Schema schema;
    private final boolean transactional;

    public KafkaEventPublisher(String bootstrapServers, String topic, int partitionCount, Schema schema) {
        this(defaultProperties(bootstrapServers), topic, partitionCount, schema);
    }

    public KafkaEventPublisher(Properties properties, String topic, int partitionCount, Schema schema) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("topic must not be null or blank");
        }
        if (partitionCount <= 0) {
            throw new IllegalArgumentException("partitionCount must be greater than 0");
        }
        this.schema = Objects.requireNonNull(schema, "schema must not be null");
        this.producer = new KafkaProducer<>(properties);
        this.topic = topic;
        this.partitionCount = partitionCount;
        this.transactional = properties.containsKey(ProducerConfig.TRANSACTIONAL_ID_CONFIG);
        if (transactional) {
            producer.initTransactions();
        }
    }

    public void publish(T event, Partitioned partitioned) {
        ProducerRecord<String, byte[]> record = createRecord(event, partitioned);
        producer.send(record);
    }

    public KafkaPublishReceipt publishSync(T event, Partitioned partitioned) {
        ProducerRecord<String, byte[]> record = createRecord(event, partitioned);
        RecordMetadata metadata = await(producer.send(record));
        return new KafkaPublishReceipt(null, metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
    }

    /**
     * Publishes a batch atomically in Kafka transaction and returns metadata for each message.
     *
     * <p>This API is intentionally separated from any DB transaction. For the outbox pattern,
     * store outbox rows in DB transaction first, then call this method, then persist returned
     * offsets in a new DB transaction. Kafka and DB cannot share one physical ACID transaction
     * without 2PC/XA.
     */
    public List<KafkaPublishReceipt> publishBatchInTransaction(List<KafkaPublishCommand<T>> commands) {
        if (!transactional) {
            throw new IllegalStateException(
                "Producer is not transactional. Configure " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " first"
            );
        }
        Objects.requireNonNull(commands, "commands must not be null");
        if (commands.isEmpty()) {
            return List.of();
        }

        List<KafkaPublishReceipt> receipts = new ArrayList<>(commands.size());
        producer.beginTransaction();
        try {
            for (KafkaPublishCommand<T> command : commands) {
                ProducerRecord<String, byte[]> record = createRecord(command.event(), command.partitioned());
                RecordMetadata metadata = await(producer.send(record));
                receipts.add(new KafkaPublishReceipt(
                    command.outboxId(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp()
                ));
            }
            producer.commitTransaction();
            return List.copyOf(receipts);
        } catch (RuntimeException ex) {
            producer.abortTransaction();
            throw ex;
        }
    }

    public List<KafkaPublishReceipt> publishBatchInTransaction(
        List<T> events,
        Function<T, Partitioned> partitionedResolver
    ) {
        Objects.requireNonNull(events, "events must not be null");
        Objects.requireNonNull(partitionedResolver, "partitionedResolver must not be null");
        if (events.isEmpty()) {
            return List.of();
        }

        List<KafkaPublishCommand<T>> commands = events.stream()
            .map(event -> new KafkaPublishCommand<T>(null, event, partitionedResolver.apply(event)))
            .toList();
        return publishBatchInTransaction(commands);
    }


    public List<KafkaPublishReceipt> publishBatchAndCommitOffsetsInTransaction(
        List<KafkaPublishCommand<T>> commands,
        java.util.Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
        ConsumerGroupMetadata consumerGroupMetadata
    ) {
        if (!transactional) {
            throw new IllegalStateException(
                "Producer is not transactional. Configure " + ProducerConfig.TRANSACTIONAL_ID_CONFIG + " first"
            );
        }
        Objects.requireNonNull(commands, "commands must not be null");
        Objects.requireNonNull(offsetsToCommit, "offsetsToCommit must not be null");
        Objects.requireNonNull(consumerGroupMetadata, "consumerGroupMetadata must not be null");

        List<KafkaPublishReceipt> receipts = new ArrayList<>(commands.size());
        producer.beginTransaction();
        try {
            for (KafkaPublishCommand<T> command : commands) {
                ProducerRecord<String, byte[]> record = createRecord(command.event(), command.partitioned());
                RecordMetadata metadata = await(producer.send(record));
                receipts.add(new KafkaPublishReceipt(
                    command.outboxId(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    metadata.timestamp()
                ));
            }
            producer.sendOffsetsToTransaction(offsetsToCommit, consumerGroupMetadata);
            producer.commitTransaction();
            return List.copyOf(receipts);
        } catch (RuntimeException ex) {
            producer.abortTransaction();
            throw ex;
        }
    }

    private ProducerRecord<String, byte[]> createRecord(T event, Partitioned partitioned) {
        if (event == null) {
            throw new IllegalArgumentException("event must not be null");
        }
        if (partitioned == null) {
            throw new IllegalArgumentException("partitioned must not be null");
        }

        String partitionKey = partitioned.getPartitionKey();
        Integer partition = PartitionId.partition(partitionKey, partitionCount);
        if (partition == null) {
            throw new IllegalArgumentException("partitionKey must not be null and partitionCount must be greater than 0");
        }

        byte[] payload = serialize(event, schema);
        return new ProducerRecord<>(topic, partition, partitionKey, payload);
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

    private static <T extends SpecificRecord> byte[] serialize(T event, Schema schema) {
        SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(schema);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            writer.write(event, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to serialize event", ex);
        }
    }

    private static RecordMetadata await(java.util.concurrent.Future<RecordMetadata> future) {
        try {
            return future.get(Duration.ofSeconds(30).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to publish to Kafka", ex);
        }
    }
}
