package com.michalklempa.flink.protobuf.test;

import com.google.protobuf.util.JsonFormat;
import com.google.type.LatLng;
import com.michalklempa.flink.protobuf.ProtoTypeInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CentroidJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.getConfig().disableGenericTypes();
        env.enableCheckpointing(15000L);

        GeneratorFunction<Long, LatLng> generator = index -> LatLng.newBuilder()
                .setLatitude(48.14 + index * 0.0001)
                .setLongitude(17.10 + index * 0.0002)
                .build();

        DataGeneratorSource<LatLng> source = new DataGeneratorSource<>(
                generator,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
                ProtoTypeInfo.proto(LatLng.class));

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "position")
                .map(latLng -> JsonFormat.printer().omittingInsignificantWhitespace().print(latLng))
                .map(json -> {
                    LatLng.Builder builder = LatLng.newBuilder();
                    JsonFormat.parser().merge(json, builder);
                    return new PositionEvent(System.currentTimeMillis(), builder.build());
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PositionEvent>forMonotonousTimestamps().withTimestampAssigner((element, recordTimestamp) -> element.timestamp))
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1L)))
                .aggregate(new AggregateFunction<PositionEvent, IncrementalCentroidAccumulator, PositionEvent>() {
                    @Override
                    public IncrementalCentroidAccumulator createAccumulator() {
                        return new IncrementalCentroidAccumulator(0L, LatLng.newBuilder()
                                .setLatitude(0.0)
                                .setLongitude(0.0)
                                .build(), 0L);
                    }

                    @Override
                    public IncrementalCentroidAccumulator add(PositionEvent value, IncrementalCentroidAccumulator accumulator) {
                        accumulator.sum = LatLng.newBuilder()
                                .setLatitude(accumulator.sum.getLatitude() + value.latLng.getLatitude())
                                .setLongitude(accumulator.sum.getLongitude() + value.latLng.getLongitude())
                                .build();
                        accumulator.n += 1;
                        accumulator.timestamp = value.timestamp;
                        return accumulator;
                    }

                    @Override
                    public PositionEvent getResult(IncrementalCentroidAccumulator accumulator) {
                        return new PositionEvent(accumulator.timestamp,
                                LatLng.newBuilder()
                                        .setLatitude(accumulator.sum.getLatitude() / accumulator.n)
                                        .setLongitude(accumulator.sum.getLongitude() / accumulator.n)
                                        .build()
                        );
                    }

                    @Override
                    public IncrementalCentroidAccumulator merge(IncrementalCentroidAccumulator a, IncrementalCentroidAccumulator b) {
                        return new IncrementalCentroidAccumulator(
                                a.n + b.n,
                                LatLng.newBuilder()
                                        .setLatitude(a.sum.getLatitude() + b.sum.getLatitude())
                                        .setLongitude(a.sum.getLongitude() + b.sum.getLongitude())
                                        .build(),
                                Math.max(a.timestamp, b.timestamp)
                        );
                    }
                })
                .map(event -> "Timestamp: " + event.timestamp + ", " +
                        JsonFormat.printer().omittingInsignificantWhitespace().print(event.latLng))
                .print();

        env.execute("flink-protobuf-serde-1-19-test");
    }
}
