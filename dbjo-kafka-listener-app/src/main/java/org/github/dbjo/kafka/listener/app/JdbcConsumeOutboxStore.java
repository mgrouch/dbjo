package org.github.dbjo.kafka.listener.app;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.github.dbjo.generated.model.dao.jdbc.ListenerConsumedOffsetsJdbcDao;
import org.github.dbjo.generated.model.dao.jdbc.ListenerOutboxJdbcDao;
import org.github.dbjo.generated.model.entity.ListenerConsumedOffsets;
import org.github.dbjo.generated.model.entity.ListenerOutbox;
import org.github.dbjo.kafka.MutablePartitionKey;
import org.github.dbjo.kafka.avro.OrderEvent;
import org.github.dbjo.kafka.listener.ConsumeOutboxStore;
import org.github.dbjo.kafka.publisher.KafkaPublishCommand;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;

public class JdbcConsumeOutboxStore implements ConsumeOutboxStore<OrderEvent> {
    private static final SpecificDatumWriter<OrderEvent> EVENT_WRITER = new SpecificDatumWriter<>(OrderEvent.class);
    private static final SpecificDatumReader<OrderEvent> EVENT_READER = new SpecificDatumReader<>(OrderEvent.class);

    private final JdbcDatasource jdbcDatasource;
    private final ListenerOutboxJdbcDao outboxDao;
    private final ListenerConsumedOffsetsJdbcDao consumedOffsetsDao;

    public JdbcConsumeOutboxStore(JdbcDatasource jdbcDatasource) {
        this.jdbcDatasource = jdbcDatasource;
        this.outboxDao = jdbcDatasource.listenerOutboxDao();
        this.consumedOffsetsDao = jdbcDatasource.listenerConsumedOffsetsDao();
    }

    @Override
    public void saveConsumedOffsetsAndCommands(
        Map<TopicPartition, OffsetAndMetadata> consumedOffsets,
        List<KafkaPublishCommand<OrderEvent>> commands
    ) {
        try (Connection connection = jdbcDatasource.dataSource().getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : consumedOffsets.entrySet()) {
                    TopicPartition topicPartition = entry.getKey();
                    OffsetAndMetadata offsetAndMetadata = entry.getValue();

                    ListenerConsumedOffsets row = new ListenerConsumedOffsets();
                    row.setTopic(topicPartition.topic());
                    row.setPartitionNo(topicPartition.partition());
                    row.setOffsetValue(offsetAndMetadata.offset());
                    row.setOffsetMetadata(offsetAndMetadata.metadata());

                    if (consumedOffsetsDao.updateById(connection, row) == 0) {
                        consumedOffsetsDao.insert(connection, row);
                    }
                }

                for (KafkaPublishCommand<OrderEvent> command : commands) {
                    ListenerOutbox outbox = new ListenerOutbox();
                    outbox.setOutboxId(command.outboxId());
                    outbox.setPartitionKey(command.partitioned().getPartitionKey());
                    outbox.setEventPayload(serialize(command.event()));
                    outboxDao.insert(connection, outbox);
                }

                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to persist consumed offsets and outbox commands", e);
        }
    }

    @Override
    public List<KafkaPublishCommand<OrderEvent>> loadPendingPublishCommands(int batchSize) {
        try {
            return outboxDao.selectAll().stream()
                .filter(row -> row.getPublishedAt() == null)
                .sorted(
                    Comparator
                        .comparing(ListenerOutbox::getCreatedAt, Comparator.nullsLast(Comparator.naturalOrder()))
                        .thenComparing(ListenerOutbox::getOutboxId)
                )
                .limit(batchSize)
                .map(row -> new KafkaPublishCommand<>(
                    row.getOutboxId(),
                    deserialize(row.getEventPayload()),
                    new MutablePartitionKey(row.getPartitionKey())
                ))
                .toList();
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to load pending outbox commands", e);
        }
    }

    @Override
    public void markPublished(List<KafkaPublishReceipt> receipts) {
        Timestamp publishedAt = new Timestamp(System.currentTimeMillis());
        try (Connection connection = jdbcDatasource.dataSource().getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                for (KafkaPublishReceipt receipt : receipts) {
                    ListenerOutbox row = new ListenerOutbox();
                    row.setOutboxId(receipt.outboxId());
                    row.setPublishedAt(publishedAt);
                    row.setPublishedTopic(receipt.topic());
                    row.setPublishedPartition(receipt.partition());
                    row.setPublishedOffset(receipt.offset());
                    row.setPublishedTimestamp(receipt.timestamp());
                    outboxDao.updateById(connection, row);
                }
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Failed to mark outbox records as published", e);
        }
    }

    private static byte[] serialize(OrderEvent event) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
            EVENT_WRITER.write(event, encoder);
            encoder.flush();
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize outbox event", e);
        }
    }

    private static OrderEvent deserialize(byte[] bytes) {
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            return EVENT_READER.read(null, decoder);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to deserialize outbox event", e);
        }
    }
}
