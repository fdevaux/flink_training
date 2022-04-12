<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Lab: Windowed Analytics (Hourly Tips)

The task of the "Hourly Tips" exercise is to identify, for each hour, the driver earning the most tips. It's easiest to approach this in two steps: first use hour-long windows that compute the total tips for each driver during the hour, and then from that stream of window results, find the driver with the maximum tip total for each hour.

Please note that the program should operate in event time.

### Input Data

The input data of this exercise is a stream of `TaxiFare` events generated by the [Taxi Fare Stream Generator](../README.md#using-the-taxi-data-streams).

The `TaxiFareGenerator` annotates the generated `DataStream<TaxiFare>` with timestamps and watermarks. Hence, there is no need to provide a custom timestamp and watermark assigner in order to correctly use event time.

### Expected Output

The result of this exercise is a data stream of `Tuple3<Long, Long, Float>` records, one for each hour. Each hourly record should contain the timestamp at the end of the hour, the driverId of the driver earning the most in tips during that hour, and the actual total of their tips.

The resulting stream should be printed to standard out.

## Getting Started

> :information_source: Rather than following these links to the sources, you might prefer to open these classes in your IDE.

### Exercise Classes

- Java:  [`org.apache.flink.training.exercises.hourlytips.HourlyTipsExercise`](src/main/java/org/apache/flink/training/exercises/hourlytips/HourlyTipsExercise.java)
- Scala: [`org.apache.flink.training.exercises.hourlytips.scala.HourlyTipsExercise`](src/main/scala/org/apache/flink/training/exercises/hourlytips/scala/HourlyTipsExercise.scala)

### Tests

- Java:  [`org.apache.flink.training.exercises.hourlytips.HourlyTipsTest`](src/test/java/org/apache/flink/training/exercises/hourlytips/HourlyTipsTest.java)
- Scala: [`org.apache.flink.training.exercises.hourlytips.scala.HourlyTipsTest`](src/test/scala/org/apache/flink/training/exercises/hourlytips/scala/HourlyTipsTest.scala)

## Implementation Hints

<details>
<summary><strong>Program Structure</strong></summary>

Note that it is possible to cascade one set of time windows after another, so long as the timeframes are compatible (the second set of windows needs to have a duration that is a multiple of the first set). So you can have a initial set of hour-long windows that is keyed by the `driverId` and use this to create a stream of `(endOfHourTimestamp, driverId, totalTips)`, and then follow this with another hour-long window (this window is not keyed) that finds the record from the first window with the maximum `totalTips`.
</details>

## Documentation

- [Windows](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/windows)
- [See the section on aggregations on windows](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/operators/overview/#datastream-transformations)

## Reference Solutions

Reference solutions are available at GitHub:

- Java:  [`org.apache.flink.training.solutions.hourlytips.HourlyTipsSolution`](src/solution/java/org/apache/flink/training/solutions/hourlytips/HourlyTipsSolution.java)
- Scala: [`org.apache.flink.training.solutions.hourlytips.scala.HourlyTipsSolution`](src/solution/scala/org/apache/flink/training/solutions/hourlytips/scala/HourlyTipsSolution.scala)

-----

[**Lab Discussion: Windowed Analytics (Hourly Tips)**](DISCUSSION.md)

[**Back to Labs Overview**](../README.md#lab-exercises)