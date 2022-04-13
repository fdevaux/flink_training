package org.apache.flink.training.exercises.longrides.params;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.Objects;
import java.util.Optional;

public class KafkaParameters {
    private final String topic;
    private final String schemaRegistryUrl;
    private final String bootstrapServers;
    private final String groupId;

    public KafkaParameters(String topic, String schemaRegistryUrl, String bootstrapServers, String groupId) {
        this.topic = topic;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KafkaParameters that = (KafkaParameters) o;
        return Objects.equals(topic, that.topic) && Objects.equals(schemaRegistryUrl, that.schemaRegistryUrl) && Objects.equals(bootstrapServers, that.bootstrapServers) && Objects.equals(groupId, that.groupId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, schemaRegistryUrl, bootstrapServers, groupId);
    }

    @Override
    public String toString() {
        return "KafkaParameters{" +
                "topic='" + topic + '\'' +
                ", schemaRegistryUrl='" + schemaRegistryUrl + '\'' +
                ", bootstrapServers='" + bootstrapServers + '\'' +
                ", groupId='" + groupId + '\'' +
                '}';
    }

    public static KafkaParameters fromParamTool(ParameterTool params) {
        final String topic =  Optional.ofNullable(params.get("topic")).orElse("taxi-event");
        final String schemaRegistryUrl = Optional.ofNullable(params.get("schema-registry-url")).orElse("http://localhost:8081");
        final String bootstrapServers = Optional.ofNullable(params.get("bootstrap-servers")).orElse("localhost:9092");
        final String groupId = Optional.ofNullable(params.get("group-id")).orElse("taxi-events-processor-group");

        return new KafkaParameters(topic, schemaRegistryUrl, bootstrapServers, groupId);
    }
}
