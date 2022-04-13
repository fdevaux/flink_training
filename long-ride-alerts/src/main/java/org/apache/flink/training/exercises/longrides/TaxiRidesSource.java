package org.apache.flink.training.exercises.longrides;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.longrides.params.KafkaParameters;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.training.exercises.longrides.processes.TaxiEventDeserializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;

public class TaxiRidesSource {
    public static DataStream<TaxiRide> getSource(StreamExecutionEnvironment env, KafkaParameters params) {
        KafkaSource<TaxiRide> kafkaSource = KafkaSource.<TaxiRide>builder()
                .setBootstrapServers(params.getBootstrapServers())
                .setTopics(params.getTopic())
                .setGroupId(params.getGroupId())
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(
                        TaxiEventDeserializer.class))
                .build();

        WatermarkStrategy<TaxiRide> watermarkStrategy = WatermarkStrategy
                .<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<TaxiRide>() {
                    @Override
                    public long extractTimestamp(TaxiRide taxiRide, long previouslyAssignedTimestampToThisEvent) {
                        return taxiRide.getEventTimeMillis();
                    }
                })
                .withIdleness(Duration.ofSeconds(2));

        return env.fromSource(
                kafkaSource,
                watermarkStrategy,
                "taxi-rides");
    }
}
