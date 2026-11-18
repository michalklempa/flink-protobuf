# flink-protobuf

Protobuf serialization for the Apache Flink DataStream API.

Flink ships no operator-to-operator serializer for protobuf messages. `flink-avro` integrates
Avro into Flink's type system (`AvroSerializer`, `AvroTypeInfo`, state schema evolution), but
protobuf has no equivalent — the official `flink-protobuf` module is a Table/SQL format
(`RowData` ↔ bytes) and does not touch the DataStream path. Without help, every protobuf
message in a DataStream job silently falls back to Kryo generic serialization.

This project fills the gap:

- **[`flink-protobuf-serde/`](flink-protobuf-serde)** — the library. `ProtoSerializer` (a
  `TypeSerializer` doing `Message.toByteArray()` / `Parser.parseFrom(byte[])`),
  `ProtoSerializerSnapshot`, and `ProtoTypeInfo`, generic over any `com.google.protobuf.Message`. Targets Flink 1.18,
  Java 11. See its [README.md](flink-protobuf-serde/README.md) for publishing.
- **[`flink-protobuf-serde-1-18-test/`](flink-protobuf-serde-1-18-test)** — a real-world test
  job proving the serializer works on the wire; see its
  [README.md](flink-protobuf-serde-1-18-test/README.md).

## Using flink-protobuf-serde in your project

### pom.xml

Add to your `pom.xml`, matching your Flink version:

#### Flink 1.18

```xml
<dependencies>
    <dependency>
        <groupId>com.michalklempa</groupId>
        <artifactId>flink-protobuf-serde</artifactId>
        <version>1.1.0-1.18</version>
    </dependency>
</dependencies>
```

#### Flink 1.20

```xml
<dependencies>
    <dependency>
        <groupId>com.michalklempa</groupId>
        <artifactId>flink-protobuf-serde</artifactId>
        <version>1.0.3-1.20</version>
    </dependency>
</dependencies>
```

The library declares `flink-core` and `protobuf-java` as `provided` — your job supplies its own
Flink and protobuf versions.

### Wiring into the application

On Flink 1.18, protobuf fields require explicit type information — there is no autodiscovery.
There are two ways to specify it:

#### 1. Explicit `ProtoTypeInfo.proto()` for direct protobuf streams

When a stream contains protobuf messages directly, attach `ProtoTypeInfo.proto(...)` to source
constructors and `.returns(...)` after lambdas:

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

After lambdas whose return type Flink cannot infer:

```java
DataStream<LatLng> positions = input
        .map(value -> toLatLng(value))
        .returns(ProtoTypeInfo.proto(LatLng.class));
```

#### 2. Annotation-based discovery for POJO fields with protobuf

When a protobuf message is a field in a regular Java POJO, annotate it with `@TypeInfo(ProtoFactory.class)`.
Flink discovers this annotation via reflection and applies the protobuf serializer to that field:

```java
public class PositionEvent {
    public long timestamp;
    
    @TypeInfo(ProtoFactory.class)
    public LatLng latLng;
    
    public PositionEvent() {}
    
    public PositionEvent(long timestamp, LatLng latLng) {
        this.timestamp = timestamp;
        this.latLng = latLng;
    }
}
```

When your stream produces `PositionEvent` objects (via a map, filter, or other transformation),
Flink automatically discovers the `@TypeInfo` annotation on the `latLng` field and applies
`ProtoFactory` to create the correct serializer:

```java
DataStream<LatLng> latLngs = /* source of LatLng protobuf messages */;

DataStream<PositionEvent> events = latLngs.map(latLng -> 
    new PositionEvent(System.currentTimeMillis(), latLng)
);
```

The annotation eliminates the need for `.returns(ProtoTypeInfo.proto(...))` when the field type
is already known through the POJO class structure.

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

## Protobuf in Flink state

Protobuf types work in keyed and operator state (`ValueState`, `MapState`, windows, joins) with
checkpointing enabled. The serializer snapshot written into every checkpoint and savepoint holds
only the fully qualified name of the message class. On restore the new serializer is
`compatibleAsIs` when its message class has the same name; any other class, or a non-protobuf
serializer, is `incompatible` and the restore fails.

No schema check happens on restore: protobuf bytes are self-describing, so Flink parses the old
bytes with whatever class the state descriptor names. Field additions and removals follow the
usual protobuf rules; field number reuse, type changes and `required` changes produce wrong data
or `InvalidProtocolBufferException` at runtime. Schema discipline is yours.

### Restoring into a renamed or moved message class

When the message class was renamed or repackaged, or the old class is no longer on the
classpath, declare the former names explicitly:

```java
ValueStateDescriptor<NewEvent> descriptor = new ValueStateDescriptor<>(
        "event", ProtoTypeInfo.proto(NewEvent.class, "com.example.legacy.OldEvent"));
```

A former name is accepted as `compatibleAsIs` only when it is listed. Former names do not take
part in `equals`/`hashCode` of the type information or the serializer, and the next checkpoint is
written with the new class name, so the declaration can be removed after one successful
checkpoint or savepoint. The `@TypeInfo(ProtoFactory.class)` annotation and the
`pipeline.serialization-config` route have no way to pass former names; use
`ProtoTypeInfo.proto(...)` in the state descriptor for a migration.

## Status

Operator-to-operator serialization and state serialization on Flink 1.18 are implemented.
Schema evolution is left to protobuf wire-format rules; the snapshot carries no descriptor.

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

## Notes for Claude

- No parent pom: each directory is an independent Maven project (Java 11, Maven 3.9.10 via `.sdkmanrc`/`.envrc`). Version suffix of the library is the target Flink, e.g. `1.0.3-1.20`. Branches `flink-1.18` and `flink-1.20` carry per-version work; `.claude/PLAN.md` has design decisions and open tasks.
- The `*-test` modules depend on a released serde artifact, not the sibling source. Run `mvn install` in `flink-protobuf-serde/` first when testing an unreleased version.
- Library tests (in `flink-protobuf-serde/`): `mvn clean test`, `mvn test -Dtest=ProtoSerializerTest#methodName`. Release: `mvn clean deploy -Prelease`. Test jobs have no unit tests; success is the job printing ProtoJSON.
- `ProtoSerializerSnapshot` writes only the message class name (version 1). Any change to the written layout needs a new version and a `readSnapshot` branch for every version ever shipped. Compatibility compares class names as strings, never `Class` identity, so a missing old class still restores when declared as a former name.
- Flink-version differences are confined to `createSerializer`: 1.18 takes `ExecutionConfig`, 1.20 overrides the `SerializerConfig` overload. Keep both lines consistent.
- Global registration works only on 1.19+ via `pipeline.serialization-config` (FLIP-398) mapping `com.google.protobuf.AbstractMessage` to `ProtoFactory`. Do not add a `register()` calling `TypeExtractor.registerFactory` (`@Internal`, removed on purpose). On 1.18 use `ProtoTypeInfo.proto(...)` / `.returns(...)`.
- Unit tests use well-known types (e.g. `Timestamp`), so `protoc` is not needed. Test jobs must keep `disableOperatorChaining()` and `disableGenericTypes()`; removing the serialization-config entry in the 1.20 job is the negative control and must fail at translation time.
