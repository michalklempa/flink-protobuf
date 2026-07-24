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