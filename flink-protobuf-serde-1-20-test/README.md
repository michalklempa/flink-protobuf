# flink-protobuf-serde-1-20-test

Self-contained Flink 1.20 streaming job proving that
[`flink-protobuf-serde`](../flink-protobuf-serde) serializes protobuf messages on the
operator-to-operator wire, and that the FLIP-398 `pipeline.serialization-config` route
registers the serializer globally on 1.20 — there is no `ProtoTypeInfo.proto(...)` call
anywhere in the job.

## The pipeline

```
DataGeneratorSource<LatLng>  --(network exchange)-->  map(LatLng -> ProtoJSON)  -->  print
```

- The source generates one `google.type.LatLng` per second (`RateLimiterStrategy.perSecond(1)`),
  a publicly published protobuf class from `proto-google-common-protos` — no dependency on any
  other project in this repository.
- The source is constructed with plain `TypeInformation.of(LatLng.class)`. Registration happens
  purely through configuration (`PipelineOptions.SERIALIZATION_CONFIG`, the FLIP-398 route that
  is impossible on 1.18):

  ```java
  Configuration configuration = new Configuration();
  configuration.set(PipelineOptions.SERIALIZATION_CONFIG, List.of(
          "com.google.protobuf.AbstractMessage: {type: typeinfo, class: com.michalklempa.flink.protobuf.ProtoFactory}"));

  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
  ```

  `ProtoFactory` is registered for `com.google.protobuf.AbstractMessage`, the superclass of all
  generated messages, so type extraction resolves every protobuf class to `ProtoTypeInfo` and
  plugs `ProtoSerializer` into Flink's type system. Only stable public API is touched — the
  equivalent zero-code form is the same pair in the cluster `config.yaml`.
- The map operator converts each message to JSON via `JsonFormat` and prints it.

## How this proves serialization works

A Flink job can appear to work while never exercising a serializer at all: when consecutive
operators are **chained**, records are passed as Java object references within one task and no
serialization happens. A wrong serializer can also hide behind Flink's silent **Kryo fallback**.
This job closes both loopholes:

1. **`env.disableOperatorChaining()`** — forces source and map into separate tasks. Every record
   must cross a real task-to-task exchange, which means every record is written by
   `ProtoSerializer.serialize()` (`Message.toByteArray()`) and read back by
   `ProtoSerializer.deserialize()` (`Parser.parseFrom(byte[])`).

2. **`env.getConfig().disableGenericTypes()`** — makes Flink throw instead of falling back to
   Kryo for any type it cannot handle natively. Removing the `pipeline.serialization-config`
   entry makes the job fail at translation time with:

   ```
   UnsupportedOperationException: Generic types have been disabled in the ExecutionConfig
   and type com.google.type.LatLng is treated as a generic type.
   ```

   This is the negative control for this test — verified on Flink 1.20.4.

Successful output therefore means: records were generated as protobuf objects, serialized to
protobuf bytes, shipped across a task boundary, deserialized back, and converted to JSON —
with Kryo provably out of the picture:

```
{"latitude":48.14,"longitude":17.1}
{"latitude":48.140100000000004,"longitude":17.1002}
{"latitude":48.1402,"longitude":17.1004}
```

## Also verified on 1.20.4

- **The 1.18-built jar works unchanged** — this job also ran green against
  `flink-protobuf-serde:1.0.0-1.18-SNAPSHOT`, both with explicit `ProtoTypeInfo.proto(...)`
  wiring and with that jar's `Proto.register()` helper (dropped in the 1.20 build:
  it relied on `TypeExtractor.registerFactory`, which is `@Internal` API — the configuration
  route reaches the same registry through stable public surface only). Flink 1.20 keeps
  `createSerializer(ExecutionConfig)` abstract and its default
  `createSerializer(SerializerConfig)` delegates to it, so the 1.18-compiled override is still
  reached. The dedicated 1.20 build overrides the `SerializerConfig` variant directly, which
  Flink 2.x requires.

## Running

Toolchain is pinned via `.sdkmanrc` (Java 11, Maven 3.9) — `direnv`/`sdk env` picks it up.

Until `1.0.0-1.20-SNAPSHOT` is published to the Central Portal snapshot repository, install the
library locally first:

```bash
cd ../flink-protobuf-serde && mvn clean install
```

Then:

```bash
mvn clean package
mvn exec:exec@run
```

`env.setParallelism(1)` keeps the generated sequence starting at index 0 and the rate at
exactly one event per second (with higher parallelism the `Long.MAX_VALUE` sequence is split
into per-subtask ranges with large offsets).
