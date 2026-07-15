# flink-protobuf-serde-1-18-test

Self-contained Flink 1.18 streaming job whose sole purpose is to prove that
[`flink-protobuf-serde`](../flink-protobuf-serde) actually serializes protobuf messages on the
operator-to-operator wire — not just in unit tests.

## The pipeline

```
DataGeneratorSource<LatLng>  --(network exchange)-->  map(LatLng -> ProtoJSON)  -->  print
```

- The source generates one `google.type.LatLng` per second (`RateLimiterStrategy.perSecond(1)`),
  a publicly published protobuf class from `proto-google-common-protos` — no dependency on any
  other project in this repository.
- The source is constructed with `ProtoTypeInfo.proto(LatLng.class)`, which plugs
  `ProtoSerializer` into Flink's type system for the stream.
- The map operator converts each message to JSON via `JsonFormat` and prints it.

## How this proves serialization works

A Flink job can appear to work while never exercising a serializer at all: when consecutive
operators are **chained**, records are passed as Java object references within one task and no
serialization happens. A wrong serializer can also hide behind Flink's silent **Kryo fallback**.
This job closes both loopholes:

1. **`env.disableOperatorChaining()`** — forces source and map into separate tasks. Every record
   must cross a real task-to-task exchange, which means every record is written by
   `ProtoSerializer.serialize()` (`Message.toByteArray()`) and read back by
   `ProtoSerializer.deserialize()` (`Parser.parseFrom(byte[])`). If the serializer round-trip
   corrupted anything, the JSON output would be wrong or the job would crash.

2. **`env.getConfig().disableGenericTypes()`** — makes Flink throw instead of falling back to
   Kryo for any type it cannot handle natively. If `ProtoTypeInfo` were not wired in, the job
   would fail at translation time with:

   ```
   UnsupportedOperationException: Generic types have been disabled in the ExecutionConfig
   and type com.google.type.LatLng is treated as a generic type.
   ```

   This is not hypothetical — replacing `ProtoTypeInfo.proto(LatLng.class)` with plain
   `TypeInformation.of(LatLng.class)` produces exactly this failure on Flink 1.18 (there is no
   serializer autodiscovery in 1.18), which doubles as the negative control for this test.

Successful output therefore means: records were generated as protobuf objects, serialized to
protobuf bytes, shipped across a task boundary, deserialized back, and converted to JSON —
with Kryo provably out of the picture:

```
{"latitude":48.14,"longitude":17.1}
{"latitude":48.140100000000004,"longitude":17.1002}
{"latitude":48.1402,"longitude":17.1004}
```

The coordinates drift by a fixed step per generated index, so gaps or repetitions in the
sequence would be visible directly in the output.

## Running

Toolchain is pinned via `.sdkmanrc` (Java 11, Maven 3.9) — `direnv`/`sdk env` picks it up.

```bash
mvn clean package
mvn exec:exec@run
```

`env.setParallelism(1)` keeps the generated sequence starting at index 0 and the rate at
exactly one event per second (with higher parallelism the `Long.MAX_VALUE` sequence is split
into per-subtask ranges with large offsets).

The `flink-protobuf-serde` dependency resolves from the Central Portal snapshot repository
(declared in `pom.xml`), so this project also verifies that the published artifact is
consumable from Maven Central.
