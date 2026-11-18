package com.michalklempa.flink.protobuf.test;

import com.google.protobuf.util.JsonFormat;
import com.google.type.LatLng;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.List;

public class Job {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.SERIALIZATION_CONFIG, List.of(
                "com.google.protobuf.AbstractMessage: {type: typeinfo, class: com.michalklempa.flink.protobuf.ProtoFactory}"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.getConfig().disableGenericTypes();
        env.enableCheckpointing(5000L);

        GeneratorFunction<Long, LatLng> generator = index -> LatLng.newBuilder()
                .setLatitude(48.14 + index * 0.0001)
                .setLongitude(17.10 + index * 0.0002)
                .build();

        DataGeneratorSource<LatLng> source = new DataGeneratorSource<>(
                generator,
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1),
                TypeInformation.of(LatLng.class));

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "position")
                .map(latLng -> JsonFormat.printer().omittingInsignificantWhitespace().print(latLng))
                .map(json -> {
                    LatLng.Builder builder = LatLng.newBuilder();
                    JsonFormat.parser().merge(json, builder);
                    return new PositionEvent(System.currentTimeMillis(), builder.build());
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PositionEvent>forMonotonousTimestamps().withTimestampAssigner((element, recordTimestamp) -> element.timestamp))
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(15L)))
                .process(new ProcessAllWindowFunction<PositionEvent, PositionEvent, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<PositionEvent, PositionEvent, TimeWindow>.Context context, Iterable<PositionEvent> elements, Collector<PositionEvent> out) throws Exception {
                        if (elements.iterator().hasNext()) {
                            out.collect(elements.iterator().next());
                        }
                    }
                })
                .map(event -> "Timestamp: " + Instant.ofEpochMilli(event.timestamp) + ", " +
                        JsonFormat.printer().omittingInsignificantWhitespace().print(event.latLng))
                .print();

        env.execute("flink-protobuf-serde-1-19-test");
    }
}
