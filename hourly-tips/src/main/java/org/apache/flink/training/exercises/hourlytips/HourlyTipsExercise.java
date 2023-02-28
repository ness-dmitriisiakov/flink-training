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

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.util.Collector;

/**
 * The Hourly Tips exercise from the Flink training.
 *
 * <p>The task of the exercise is to first calculate the total tips collected by each driver, hour
 * by hour, and then from that stream, find the highest tip total in each hour.
 */
public class HourlyTipsExercise {

    private final SourceFunction<TaxiFare> source;
    private final SinkFunction<Tuple3<Long, Long, Float>> sink;

    /** Creates a job using the source and sink provided. */
    public HourlyTipsExercise(
            SourceFunction<TaxiFare> source, SinkFunction<Tuple3<Long, Long, Float>> sink) {

        this.source = source;
        this.sink = sink;
    }

    /**
     * Main method.
     *
     * @throws Exception which occurs during job execution.
     */
    public static void main(String[] args) throws Exception {

        HourlyTipsExercise job =
                new HourlyTipsExercise(new TaxiFareGenerator(), new PrintSinkFunction<>());

        job.execute();
    }

    private static class TipsByDriverWindowFunction extends ProcessWindowFunction<Float, Tuple3<Long, Long, Float>, Long, TimeWindow> {

        public void process(Long driverId,
                          Context context,
                          Iterable<Float> hourlyTips,
                          Collector<Tuple3<Long, Long, Float>> out) {
            Float hourlyTipsByDriver = hourlyTips.iterator().next();
            out.collect(new Tuple3<>(context.window().getEnd(), driverId, hourlyTipsByDriver));
        }
    }

    private static class TipsByDriver implements AggregateFunction<TaxiFare, Tuple2<Long, Float>, Float> {
        @Override
        public Tuple2<Long, Float> createAccumulator() {
            return new Tuple2<>(0L, 0F);
        }

        @Override
        public Tuple2<Long, Float> add(TaxiFare fare, Tuple2<Long, Float> accumulator) {
            return new Tuple2<>(fare.driverId, accumulator.f1 + fare.tip);
        }

        @Override
        public Float getResult(Tuple2<Long, Float> accumulator) {
            return accumulator.f1;
        }

        @Override
        public Tuple2<Long, Float> merge(Tuple2<Long, Float> accum1, Tuple2<Long, Float> accum2) {
            return new Tuple2<>(accum1.f0, accum1.f1 + accum2.f1);
        }
    }

    /**
     * Create and execute the hourly tips pipeline.
     *
     * @return {JobExecutionResult}
     * @throws Exception which occurs during job execution.
     */
    public JobExecutionResult execute() throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        WatermarkStrategy<TaxiFare> strategy = WatermarkStrategy.
                <TaxiFare>forMonotonousTimestamps()
                .withTimestampAssigner((fare, timestamp) -> fare.getEventTimeMillis());

        // start the data generator
        DataStream<TaxiFare> fares = env.addSource(source);

        DataStream<Tuple3<Long, Long, Float>> tipsByDriverId = fares.assignTimestampsAndWatermarks(strategy)
                .keyBy(fare -> fare.driverId)
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new TipsByDriver(), new TipsByDriverWindowFunction());

        DataStream<Tuple3<Long, Long, Float>> hourlyMax = tipsByDriverId
                .windowAll(TumblingEventTimeWindows.of(Time.hours(1))).maxBy(2);

        hourlyMax.addSink(sink);

        return env.execute("Hourly Tips");
    }
}
