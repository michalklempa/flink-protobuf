# flink-protobuf-serde

Flink DataStream `TypeSerializer`/`TypeInformation` for arbitrary Protobuf messages
(operator-to-operator serialization via `Message.toByteArray()` / `Parser.parseFrom(byte[])`).

## Usage

On Flink 1.19+, register `ProtoFactory` for `com.google.protobuf.AbstractMessage` via the
FLIP-398 `pipeline.serialization-config` option — every protobuf message type then resolves to
`ProtoTypeInfo` automatically (source constructors, lambda return types, POJO fields). In the
cluster `config.yaml`:

```yaml
pipeline.serialization-config:
  - com.google.protobuf.AbstractMessage: {type: typeinfo, class: com.michalklempa.flink.protobuf.ProtoFactory}
```

Or programmatically through the same option:

```java
Configuration configuration = new Configuration();
configuration.set(PipelineOptions.SERIALIZATION_CONFIG, List.of(
        "com.google.protobuf.AbstractMessage: {type: typeinfo, class: com.michalklempa.flink.protobuf.ProtoFactory}"));

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
```

Explicit wiring works everywhere and is the only route on Flink 1.18 (no autodiscovery there):
attach `ProtoTypeInfo.proto(...)` wherever the type is produced (source constructors,
`.returns(...)` after lambdas):

```java
DataGeneratorSource<LatLng> source = new DataGeneratorSource<>(
        generator,
        Long.MAX_VALUE,
        RateLimiterStrategy.perSecond(1),
        ProtoTypeInfo.proto(LatLng.class));
```

## Publishing to Maven Central

Publishing goes through the Central Publisher Portal. Follow the official guide:
<https://central.sonatype.org/publish/publish-portal-maven/#usage>

### Prerequisites

1. A verified namespace (`com.michalklempa` in this case) at <https://central.sonatype.com>.
2. A portal token — generate it as described in
   <https://central.sonatype.org/publish/generate-portal-token/> and put it into
   `~/.m2/settings.xml`:

   ```xml
   <settings>
     <servers>
       <server>
         <id>central</id>
         <username><!-- token username --></username>
         <password><!-- token password --></password>
       </server>
     </servers>
   </settings>
   ```

   The `<id>` must be `central` — it matches `publishingServerId` in the `release` profile.
3. A GPG key whose public part is published to a keyserver:

   ```bash
   gpg --keyserver keyserver.ubuntu.com --send-keys <KEYID>
   ```

### Deploy

```bash
mvn clean deploy -Prelease -s ~/.m2/settings.xml
```

`-s ~/.m2/settings.xml` selects the user settings file with the portal token explicitly,
regardless of what the global (sdkman) `conf/settings.xml` contains.

### Selecting the signing key

Signing is fully interactive — no passphrase is stored anywhere. The key is chosen
per invocation:

```bash
mvn clean deploy -Prelease -s ~/.m2/settings.xml -Dgpg.keyname=<KEYID>
```

`gpg.keyname` accepts a key id, fingerprint or uid (e-mail). List available keys with:

```bash
gpg --list-secret-keys --keyid-format=long
```

If `-Dgpg.keyname` is omitted, gpg uses its default key (the first secret key, or
`default-key` from `~/.gnupg/gpg.conf`). During the build a pinentry dialog prompts for
the key passphrase. Do not run with `-B`/`--batch-mode` — it suppresses the prompt.
gpg-agent caches the passphrase (~10 minutes); force a re-prompt with
`gpgconf --kill gpg-agent`.