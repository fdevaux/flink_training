/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.training.exercises.longrides;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator;
import org.apache.flink.training.exercises.longrides.params.KafkaParameters;
import org.apache.flink.training.exercises.longrides.params.OutputParams;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Optional;

/**
 * The "Long Ride Alerts" exercise.
 *
 * <p>The goal for this exercise is to emit the rideIds for taxi rides with a duration of more than
 * two hours. You should assume that TaxiRide events can be lost, but there are no duplicates.
 *
 * <p>You should eventually clear any state you create.
 */
public class LongRidesExercise {


    /**
     * Creates and executes the long rides pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        createInitialTable(params);

        KafkaParameters kafkaParameters = KafkaParameters.fromParamTool(params);
        OutputParams outputParams = OutputParams.fromParamTool(params);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamSetup.setupRestartsAndCheckpoints(env, outputParams.getCheckpointsPath());

        DataStream<TaxiRide> taxiRideEventsDataStream = TaxiRidesSource.getSource(env, kafkaParameters);

        // the WatermarkStrategy specifies how to extract timestamps and generate watermarks
        WatermarkStrategy<TaxiRide> watermarkStrategy =
                WatermarkStrategy.<TaxiRide>forBoundedOutOfOrderness(Duration.ofSeconds(60))
                        .withTimestampAssigner(
                                (ride, streamRecordTimestamp) -> ride.getEventTimeMillis());

        // create the pipeline
        taxiRideEventsDataStream.assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(ride -> ride.rideId)
                .process(new AlertFunction())
                .addSink(JdbcSink.sink(
                        "insert into taxi_alarm (rideId) values (?)",
                        (statement, id) -> {
                            statement.setString(1, String.valueOf(id));
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(5)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(Optional.ofNullable(params.get("db-url"))
                                        .orElse("jdbc:postgresql://localhost:5432/postgres"))
                                .withDriverName("org.postgresql.Driver")
                                .withUsername("postgres")
                                .withPassword("postgres")
                                .build()));

        // execute the pipeline
        env.execute("Long Taxi Rides");
    }


    @VisibleForTesting
    public static class AlertFunction extends KeyedProcessFunction<Long, TaxiRide, Long> {

        private transient ValueState<TaxiRide> rideValueState;

        @Override
        public void open(Configuration config) {
            this.rideValueState =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("aTaxiRide", TaxiRide.class));
        }

        @Override
        public void processElement(TaxiRide ride, Context context, Collector<Long> out)
                throws Exception {
            TaxiRide currentRideEvent = this.rideValueState.value();

            if (currentRideEvent == null) {
                // If current state for event is null, then event received becomes current state
                rideValueState.update(ride);
                // If event is START, start a timer to detect maximum time
                if (ride.isStart) {
                    context.timerService().registerEventTimeTimer(getTimerTime(ride));
                }
                // If the event is STOP, means STOP arrived before START
                // We don't start a timer, hoping that the start event will arrive
                // if not, the STOP event will never be cleared

            } else {
                // -------------- This is the second event
                // Calculate the duration between the 2 evnts
                // collect if it is over the limit
                // Then stop the timer if it is still running

                // --------   Stop the timer if it is running
                // If the event is START, we have not started the timer yet
                // so no need to stop it
                // If event is STOP, stop the timer if still running

                if (ride.isStart) {
                    if (rideTooLong(ride, currentRideEvent)) {
                        out.collect(ride.rideId);
                    }
                    rideValueState.clear();
                } else {
                    // Got a STOP event, and timer did not fire. Stop the timer,
                    // Check it time exceeded
                    // then clear state
                    context.timerService().deleteEventTimeTimer(getTimerTime(currentRideEvent));
                    if (rideTooLong(currentRideEvent, ride)) {
                        out.collect(ride.rideId);
                    }
                    rideValueState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<Long> out)
                throws Exception {
            // Timer fired so maximum time waiting for a STOP event has been reached
            out.collect(rideValueState.value().rideId);

            // Clear up event, even if it means that the STOP event will never be cleared
            // if it arrives
            rideValueState.clear();
        }

        private boolean rideTooLong(TaxiRide startEvent, TaxiRide endEvent) {
            return Duration.between(startEvent.eventTime, endEvent.eventTime)
                            .compareTo(Duration.ofHours(2))
                    > 0;
        }

        private long getTimerTime(TaxiRide ride) throws RuntimeException {
            if (ride.isStart) {
                return ride.eventTime.plusSeconds(7200).toEpochMilli();
            } else {
                throw new RuntimeException("Can not get start time from END event.");
            }
        }
    }


    private static void createInitialTable(ParameterTool params) throws ClassNotFoundException, SQLException {
        Class.forName("org.postgresql.Driver");
        Connection c =
                DriverManager.getConnection(
                        Optional.ofNullable(params.get("db-url"))
                                .orElse("jdbc:postgresql://localhost:5432/postgres"), "postgres", "postgres");
        Statement stmt = c.createStatement();
        stmt.execute("create table if not exists taxi_alarm(rideId varchar(255));");
        stmt.close();
        c.close();
    }

}
