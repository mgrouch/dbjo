package org.github.dbjo.kafka.listener;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaEventListenerSchemaRegistryTest {
    @Test
    void shouldNotRequireSchemaRegistryUrlForNonAvroDeserializer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaEventListener.applySchemaRegistryUrlIfRequired(properties, Map.of());

        assertEquals(null, properties.get(KafkaEventListener.SCHEMA_REGISTRY_URL_CONFIG));
    }

    @Test
    void shouldSetSchemaRegistryUrlFromEnvironmentWhenAvroDeserializerIsConfigured() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        KafkaEventListener.applySchemaRegistryUrlIfRequired(
            properties,
            Map.of("KAFKA_SCHEMA_REGISTRY_URL", "http://localhost:8081")
        );

        assertEquals("http://localhost:8081", properties.get(KafkaEventListener.SCHEMA_REGISTRY_URL_CONFIG));
    }

    @Test
    void shouldFailWhenAvroDeserializerIsConfiguredWithoutSchemaRegistryUrl() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> KafkaEventListener.applySchemaRegistryUrlIfRequired(properties, Map.of())
        );

        assertEquals("kafka schema registry URL is not set", ex.getMessage());
    }
}
