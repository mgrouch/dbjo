package org.github.dbjo.kafka.listener.app;

import org.github.dbjo.kafka.listener.KafkaEventListenerProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "dbjo.kafka.listener")
public class OrderEventListenerProperties extends KafkaEventListenerProperties {
    private String outputTopic;
    private String schemaRegistryUrl;
    private String securityProtocol;
    private String sslTruststoreLocation;
    private String sslTruststorePassword;
    private String sslTruststoreType;
    private String sslKeystoreLocation;
    private String sslKeystorePassword;
    private String sslKeystoreType;
    private String sslKeyPassword;
    private String sslEndpointIdentificationAlgorithm;
    private String consumerKeyDeserializerClass;
    private String consumerValueDeserializerClass;
    private boolean consumerSpecificAvroReader;
    private String producerTransactionalIdPrefix;
    private boolean producerEnableIdempotence;
    private String producerKeySerializerClass;
    private String producerValueSerializerClass;

    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public String getSslTruststoreLocation() {
        return sslTruststoreLocation;
    }

    public void setSslTruststoreLocation(String sslTruststoreLocation) {
        this.sslTruststoreLocation = sslTruststoreLocation;
    }

    public String getSslTruststorePassword() {
        return sslTruststorePassword;
    }

    public void setSslTruststorePassword(String sslTruststorePassword) {
        this.sslTruststorePassword = sslTruststorePassword;
    }

    public String getSslTruststoreType() {
        return sslTruststoreType;
    }

    public void setSslTruststoreType(String sslTruststoreType) {
        this.sslTruststoreType = sslTruststoreType;
    }

    public String getSslKeystoreLocation() {
        return sslKeystoreLocation;
    }

    public void setSslKeystoreLocation(String sslKeystoreLocation) {
        this.sslKeystoreLocation = sslKeystoreLocation;
    }

    public String getSslKeystorePassword() {
        return sslKeystorePassword;
    }

    public void setSslKeystorePassword(String sslKeystorePassword) {
        this.sslKeystorePassword = sslKeystorePassword;
    }

    public String getSslKeystoreType() {
        return sslKeystoreType;
    }

    public void setSslKeystoreType(String sslKeystoreType) {
        this.sslKeystoreType = sslKeystoreType;
    }

    public String getSslKeyPassword() {
        return sslKeyPassword;
    }

    public void setSslKeyPassword(String sslKeyPassword) {
        this.sslKeyPassword = sslKeyPassword;
    }

    public String getSslEndpointIdentificationAlgorithm() {
        return sslEndpointIdentificationAlgorithm;
    }

    public void setSslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm) {
        this.sslEndpointIdentificationAlgorithm = sslEndpointIdentificationAlgorithm;
    }

    public String getConsumerKeyDeserializerClass() {
        return consumerKeyDeserializerClass;
    }

    public void setConsumerKeyDeserializerClass(String consumerKeyDeserializerClass) {
        this.consumerKeyDeserializerClass = consumerKeyDeserializerClass;
    }

    public String getConsumerValueDeserializerClass() {
        return consumerValueDeserializerClass;
    }

    public void setConsumerValueDeserializerClass(String consumerValueDeserializerClass) {
        this.consumerValueDeserializerClass = consumerValueDeserializerClass;
    }

    public boolean isConsumerSpecificAvroReader() {
        return consumerSpecificAvroReader;
    }

    public void setConsumerSpecificAvroReader(boolean consumerSpecificAvroReader) {
        this.consumerSpecificAvroReader = consumerSpecificAvroReader;
    }

    public String getProducerTransactionalIdPrefix() {
        return producerTransactionalIdPrefix;
    }

    public void setProducerTransactionalIdPrefix(String producerTransactionalIdPrefix) {
        this.producerTransactionalIdPrefix = producerTransactionalIdPrefix;
    }

    public boolean isProducerEnableIdempotence() {
        return producerEnableIdempotence;
    }

    public void setProducerEnableIdempotence(boolean producerEnableIdempotence) {
        this.producerEnableIdempotence = producerEnableIdempotence;
    }

    public String getProducerKeySerializerClass() {
        return producerKeySerializerClass;
    }

    public void setProducerKeySerializerClass(String producerKeySerializerClass) {
        this.producerKeySerializerClass = producerKeySerializerClass;
    }

    public String getProducerValueSerializerClass() {
        return producerValueSerializerClass;
    }

    public void setProducerValueSerializerClass(String producerValueSerializerClass) {
        this.producerValueSerializerClass = producerValueSerializerClass;
    }
}
