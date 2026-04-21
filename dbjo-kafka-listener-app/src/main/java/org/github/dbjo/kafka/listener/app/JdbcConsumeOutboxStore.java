package org.github.dbjo.kafka.listener.app;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
import org.github.dbjo.kafka.MutablePartitionKey;
import org.github.dbjo.kafka.avro.OrderEvent;
import org.github.dbjo.kafka.listener.ConsumeOutboxStore;
import org.github.dbjo.kafka.publisher.KafkaPublishCommand;
import org.github.dbjo.kafka.publisher.KafkaPublishReceipt;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class JdbcConsumeOutboxStore implements ConsumeOutboxStore<OrderEvent> {
    private static final SpecificDatumWriter<OrderEvent> EVENT_WRITER = new SpecificDatumWriter<>(OrderEvent.class);
    private static final SpecificDatumReader<OrderEvent> EVENT_READER = new SpecificDatumReader<>(OrderEvent.class);

    private final JdbcTemplate jdbcTemplate;

    public JdbcConsumeOutboxStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS listener_outbox (
                outbox_id VARCHAR(255) PRIMARY KEY,
                partition_key VARCHAR(255) NOT NULL,
                event_payload BLOB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
                published_at TIMESTAMP NULL,
                published_topic VARCHAR(255),
                published_partition INT,
                published_offset BIGINT,
                published_timestamp BIGINT
            )
            """);
        this.jdbcTemplate.execute("""
            CREATE TABLE IF NOT EXISTS listener_consumed_offsets (
                topic VARCHAR(255) NOT NULL,
                partition_no INT NOT NULL,
                offset_value BIGINT NOT NULL,
                offset_metadata VARCHAR(1024),
                PRIMARY KEY (topic, partition_no)
            )
            """);
    }

    @Override
    public void saveConsumedOffsetsAndCommands(
        Map<TopicPartition, OffsetAndMetadata> consumedOffsets,
        List<KafkaPublishCommand<OrderEvent>> commands
    ) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : consumedOffsets.entrySet()) {
            TopicPartition topicPartition = entry.getKey();
            OffsetAndMetadata offsetAndMetadata = entry.getValue();
            jdbcTemplate.update(
                """
                MERGE INTO listener_consumed_offsets (topic, partition_no, offset_value, offset_metadata)
                KEY (topic, partition_no)
                VALUES (?, ?, ?, ?)
                """,
                topicPartition.topic(),
                topicPartition.partition(),
                offsetAndMetadata.offset(),
                offsetAndMetadata.metadata()
            );
        }

        for (KafkaPublishCommand<OrderEvent> command : commands) {
            jdbcTemplate.update(
                """
                INSERT INTO listener_outbox (outbox_id, partition_key, event_payload)
                VALUES (?, ?, ?)
                """,
                command.outboxId(),
                command.partitioned().getPartitionKey(),
                serialize(command.event())
            );
        }
    }

    @Override
    public List<KafkaPublishCommand<OrderEvent>> loadPendingPublishCommands(int batchSize) {
        return jdbcTemplate.query(
            """
            SELECT outbox_id, partition_key, event_payload
            FROM listener_outbox
            WHERE published_at IS NULL
            ORDER BY created_at, outbox_id
            LIMIT ?
            """,
            outboxMapper(),
            batchSize
        );
    }

    @Override
    public void markPublished(List<KafkaPublishReceipt> receipts) {
        for (KafkaPublishReceipt receipt : receipts) {
            jdbcTemplate.update(
                """
                UPDATE listener_outbox
                SET published_at = CURRENT_TIMESTAMP,
                    published_topic = ?,
                    published_partition = ?,
                    published_offset = ?,
                    published_timestamp = ?
                WHERE outbox_id = ?
                """,
                receipt.topic(),
                receipt.partition(),
                receipt.offset(),
                receipt.timestamp(),
                receipt.outboxId()
            );
        }
    }

    private static RowMapper<KafkaPublishCommand<OrderEvent>> outboxMapper() {
        return (rs, rowNum) -> new KafkaPublishCommand<>(
            rs.getString("outbox_id"),
            deserialize(rs.getBytes("event_payload")),
            new MutablePartitionKey(rs.getString("partition_key"))
        );
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
