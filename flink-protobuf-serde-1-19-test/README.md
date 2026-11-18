# flink-protobuf-serde-1-19-test

The same jobs as [`flink-protobuf-serde-1-18-test`](../flink-protobuf-serde-1-18-test), run on
Flink 1.19 against the 1.18 build of [`flink-protobuf-serde`](../flink-protobuf-serde)
(`1.1.0-1.18`). It proves the 1.18 jar works unchanged on 1.19, including state serialization
via `ProtoSerializerSnapshot` on checkpoints.

## Differences from the 1.18 test

Only `Job.java` differs: instead of `ProtoTypeInfo.proto(LatLng.class)`, the source uses plain
`TypeInformation.of(LatLng.class)` and registration happens through the FLIP-398
`pipeline.serialization-config` route, which does not exist on 1.18:

```java
Configuration configuration = new Configuration();
configuration.set(PipelineOptions.SERIALIZATION_CONFIG, List.of(
        "com.google.protobuf.AbstractMessage: {type: typeinfo, class: com.michalklempa.flink.protobuf.ProtoFactory}"));

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
```

`SumJob`, `CentroidJob`, `PositionEvent`, `IncrementalCentroidAccumulator` and `LatLngPojoTest`
are identical to the 1.18 test. See its README for the pipeline description, the verification
approach (`disableOperatorChaining()`, `disableGenericTypes()`) and the expected output.

## Running

Toolchain is pinned via `.sdkmanrc` (Java 11, Maven 3.9) — `direnv`/`sdk env` picks it up.

```bash
mvn clean package
mvn exec:exec@run
```

Run another job by overriding the main class, e.g.
`mvn exec:exec@run -Dmain.class=com.michalklempa.flink.protobuf.test.SumJob`.
