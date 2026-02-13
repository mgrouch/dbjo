package org.github.dbjo.kafka.publisher;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaEventPublisherSchemaRegistryTest {
    @Test
    void shouldNotRequireSchemaRegistryUrlForNonAvroSerializer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

        KafkaEventPublisher.applySchemaRegistryUrlIfRequired(properties, Map.of());

        assertEquals(null, properties.get(KafkaEventPublisher.SCHEMA_REGISTRY_URL_CONFIG));
    }

    @Test
    void shouldSetSchemaRegistryUrlFromEnvironmentWhenAvroSerializerIsConfigured() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");

        KafkaEventPublisher.applySchemaRegistryUrlIfRequired(
            properties,
            Map.of("KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081")
        );

        assertEquals("http://localhost:8081", properties.get(KafkaEventPublisher.SCHEMA_REGISTRY_URL_CONFIG));
    }

    @Test
    void shouldFailWhenAvroSerializerIsConfiguredWithoutSchemaRegistryUrl() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> KafkaEventPublisher.applySchemaRegistryUrlIfRequired(properties, Map.of())
        );

        assertEquals("kafka schema registry URL is not set", ex.getMessage());
    }
}
