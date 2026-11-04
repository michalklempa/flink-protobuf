package com.michalklempa.flink.protobuf.test;

import com.google.protobuf.util.JsonFormat;
import com.google.type.LatLng;
import com.michalklempa.flink.protobuf.ProtoTypeInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SumJob {

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
                .aggregate(new AggregateFunction<PositionEvent, LatLng, PositionEvent>() {
                               @Override
                               public LatLng createAccumulator() {
                                   return LatLng.newBuilder().build();
                               }

                               @Override
                               public LatLng add(PositionEvent value, LatLng accumulator) {
                                   return accumulator.toBuilder().setLatitude(accumulator.getLatitude() + value.latLng.getLatitude()).setLongitude(accumulator.getLongitude() + value.latLng.getLongitude()).build();
                               }

                               @Override
                               public PositionEvent getResult(LatLng accumulator) {
                                   return new PositionEvent(System.currentTimeMillis(), accumulator);
                               }

                               @Override
                               public LatLng merge(LatLng a, LatLng b) {
                                   return a.toBuilder().setLatitude(a.getLatitude() + b.getLatitude()).setLongitude(a.getLongitude() + b.getLongitude()).build();
                               }
                           }, ProtoTypeInfo.proto(LatLng.class),
                        TypeInformation.of(PositionEvent.class))
                .map(event -> "Timestamp: " + event.timestamp + ", " +
                        JsonFormat.printer().omittingInsignificantWhitespace().print(event.latLng))
                .print();

        env.execute("flink-protobuf-serde-1-18-test");
    }
}
