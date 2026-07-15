# flink-protobuf

Protobuf serialization for the Apache Flink DataStream API.

Flink ships no operator-to-operator serializer for protobuf messages. `flink-avro` integrates
Avro into Flink's type system (`AvroSerializer`, `AvroTypeInfo`, state schema evolution), but
protobuf has no equivalent — the official `flink-protobuf` module is a Table/SQL format
(`RowData` ↔ bytes) and does not touch the DataStream path. Without help, every protobuf
message in a DataStream job silently falls back to Kryo generic serialization.

This project fills the gap:

- **[`flink-protobuf-serde/`](flink-protobuf-serde)** — the library. `ProtoSerializer` (a
  `TypeSerializer` doing `Message.toByteArray()` / `Parser.parseFrom(byte[])`), `ProtoSnapshot`,
  and `ProtoTypeInfo`, generic over any `com.google.protobuf.Message`. Targets Flink 1.18,
  Java 11. See its [README.md](flink-protobuf-serde/README.md) for publishing.
- **[`flink-protobuf-serde-1-18-test/`](flink-protobuf-serde-1-18-test)** — a real-world test
  job proving the serializer works on the wire; see its
  [README.md](flink-protobuf-serde-1-18-test/README.md).

## Using flink-protobuf-serde in your project

### pom.xml

Currently only a SNAPSHOT is published, so add the Central Portal snapshot repository together
with the dependency (once a release version is out, the `<repositories>` block is unnecessary):

```xml

<repositories>
    <repository>
        <id>central-portal-snapshots</id>
        <name>Central Portal Snapshots</name>
        <url>https://central.sonatype.com/repository/maven-snapshots/</url>
        <releases>
            <enabled>false</enabled>
        </releases>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
</repositories>

<dependencies>
    <dependency>
        <groupId>com.michalklempa</groupId>
        <artifactId>flink-protobuf-serde</artifactId>
        <version>1.0.0-1.18-SNAPSHOT</version>
    </dependency>
</dependencies>
```

The library declares `flink-core` and `protobuf-java` as `provided` — your job supplies its own
Flink and protobuf versions.

### Wiring into the application

Flink 1.18 has no serializer autodiscovery, so attach `ProtoTypeInfo.proto(YourMessage.class)`
explicitly wherever a protobuf-typed stream is produced. In source constructors that take a
`TypeInformation` (DataGen example):

```java
GeneratorFunction<Long, LatLng> generator = index -> LatLng.newBuilder()
        .setLatitude(48.14 + index * 0.0001)
        .setLongitude(17.10 + index * 0.0002)
        .build();

DataGeneratorSource<LatLng> source = new DataGeneratorSource<>(
        generator,
        Long.MAX_VALUE,
        RateLimiterStrategy.perSecond(1),
        ProtoTypeInfo.proto(LatLng.class));

DataStream<LatLng> positions =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "position");
```

After lambdas whose return type Flink cannot infer, use `.returns(...)`:

```java
DataStream<LatLng> positions = input
        .map(value -> toLatLng(value))
        .returns(ProtoTypeInfo.proto(LatLng.class));
```

### Verifying the serde is actually used

Flink falls back to Kryo silently — a job can run fine while never touching this library. To
verify the wiring, disable generic types:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.getConfig().disableGenericTypes();
```

With this set, any stream that would use Kryo fails fast at job translation:

```
UnsupportedOperationException: Generic types have been disabled in the ExecutionConfig
and type com.google.type.LatLng is treated as a generic type.
```

If the job starts, every protobuf record on the wire goes through `ProtoSerializer`. Keeping
the flag on permanently is a good guard against accidental Kryo regressions.

## Status

Operator-to-operator serialization on Flink 1.18 is implemented and verified. Keyed/operator
state, schema evolution are out-of-scope of this project. Do not use Protobuf for Flink state.

## Related work

Existing approaches to protobuf-in-Flink differ in *where* the serializer hooks into the stack,
and that placement decides everything downstream: performance envelope, state compatibility,
and whether Flink's Kryo kill-switch can be used at all.

| Library                                                                               | Hook level              | `disableGenericTypes()` usable | Notes                                                                                 |
|---------------------------------------------------------------------------------------|-------------------------|--------------------------------|---------------------------------------------------------------------------------------|
| [twitter/chill](https://github.com/twitter/chill) (`chill-protobuf`)                  | Kryo default serializer | ❌                              | `registerTypeWithKryoSerializer(...)`; Kryo 2.x, matches Flink 1.x                    |
| [magro/kryo-serializers](https://github.com/magro/kryo-serializers) (`de.javakaffee`) | Kryo default serializer | ❌                              | same approach; recent releases target Kryo 5, incompatible with Flink 1.x's Kryo 2.24 |
| [findify/flink-protobuf](https://github.com/findify/flink-protobuf)                   | `TypeInformation`       | ✅                              | Scala/ScalaPB-first, Flink 1.13 era, dormant since 2021                               |
| Apache `flink-protobuf` (official)                                                    | Table/SQL format        | n/a                            | `RowData` ↔ bytes for SQL connectors; does not touch the DataStream path              |
| Apache `flink-avro`                                                                   | `TypeInformation`       | ✅                              | the archetype — Avro only, includes state schema evolution                            |
| **this project**                                                                      | `TypeInformation`       | ✅                              | plain Java, any `Message`, Flink 1.18                                                 |

**Kryo-hook family** (chill-protobuf, de.javakaffee's `ProtobufSerializer`): both wrap
`toByteArray()`/`parseFrom()` in a `com.esotericsoftware.kryo.Serializer` registered via
`env.getConfig().registerTypeWithKryoSerializer(...)`. The payload bytes are protobuf, but the
message type remains a *generic type* to Flink — records still travel through `KryoSerializer`
as the outer envelope. The consequence is architectural, not cosmetic: calling
`env.getConfig().disableGenericTypes()` makes such jobs fail, because the setting forbids
exactly the code path these libraries live on. You must keep Kryo fallback enabled globally,
which means a forgotten registration for some *other* type degrades silently instead of failing
fast. There is also a version-coupling cost: the Kryo serializer API must match Flink's bundled
Kryo (2.24 for Flink 1.x), which rules out current `de.javakaffee` releases (Kryo 5).
Performance-wise it should not matter if coupling is done via Kryo or directly on `TypeInformation`,
same Protobuf `toBytes()` is used in both cases.

**TypeInformation family** (flink-avro, findify, this project): the serializer plugs into
Flink's own type system as a first-class `TypeSerializer`, so protobuf types stop being generic
types altogether. `disableGenericTypes()` becomes *usable as a guard* — the design goal here:
the strictest serialization setting stays on, protobuf flows through a purpose-built serializer,
and anything that would silently fall back to Kryo fails at translation time instead. Compared
to findify's library, this project is plain-Java-first (no ScalaPB/Scala dependency, no default
instance parameter at every call site).

## License

[Apache-2.0](LICENSE)
