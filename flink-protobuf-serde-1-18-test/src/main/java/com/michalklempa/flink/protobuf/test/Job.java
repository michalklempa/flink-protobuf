package com.michalklempa.flink.protobuf.test;

import com.google.protobuf.util.JsonFormat;
import com.google.type.LatLng;
import com.michalklempa.flink.protobuf.ProtoTypeInfo;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Job {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.getConfig().disableGenericTypes();

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
                .map(value -> JsonFormat.printer().omittingInsignificantWhitespace().print(value))
                .print();

        env.execute("flink-protobuf-serde-1-18-test");
    }
}
