# flink-protobuf-serde-1-18-test

Self-contained Flink 1.18 streaming jobs that prove
[`flink-protobuf-serde`](../flink-protobuf-serde) actually serializes protobuf messages on the
operator-to-operator wire — not just in unit tests.

## The Job

Job.java demonstrates both usage patterns for protobuf serialization in a single pipeline:

```
DataGeneratorSource<LatLng> (explicit ProtoTypeInfo.proto())
  --> map(LatLng -> ProtoJSON string)  
  --> map(parse JSON back to LatLng, wrap in PositionEvent)
  --> map(format and print)
```

**Explicit `ProtoTypeInfo.proto()` usage:**
- The source generates one `google.type.LatLng` per second with explicit type info:
  `ProtoTypeInfo.proto(LatLng.class)`

**Annotation-based `@TypeInfo(ProtoFactory.class)` discovery:**
- Second map parses JSON back into `LatLng` and wraps it in a `PositionEvent` POJO
- `PositionEvent` has its `latLng` field annotated with `@TypeInfo(ProtoFactory.class)`, so Flink
  automatically discovers and applies the protobuf serializer to that field

**Output:**
- Final map formats with timestamp and prints the result

## How serialization is verified

Both jobs close the loopholes that can hide serialization failures:

1. **`env.disableOperatorChaining()`** — forces operators into separate tasks. Every record must
   cross a task-to-task exchange via the network serializer. If the serializer round-trip
   corrupted anything, the output would be wrong or the job would crash.

2. **`env.getConfig().disableGenericTypes()`** — makes Flink fail at translation time if any type
   falls back to Kryo. Without this flag, a missing or wrong serializer is silent. With it, any
   type that isn't handled natively throws:

   ```
   UnsupportedOperationException: Generic types have been disabled in the ExecutionConfig
   and type com.google.type.LatLng is treated as a generic type.
   ```

Successful output proves that protobuf records were serialized by `ProtoSerializer.serialize()`
(`Message.toByteArray()`), shipped as bytes, and deserialized by `ProtoSerializer.deserialize()`
(`Parser.parseFrom(byte[])`), with Kryo provably out of the picture.

Expected output (coordinates drift by a fixed step per generated index):

```
Timestamp: 1234567890000, {"latitude":48.14,"longitude":17.1}
Timestamp: 1234567890001, {"latitude":48.140100000000004,"longitude":17.1002}
Timestamp: 1234567890002, {"latitude":48.1402,"longitude":17.1004}
```

Gaps or repetitions in the sequence would be visible directly in the output.

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
