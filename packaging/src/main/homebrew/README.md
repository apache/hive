# Homebrew formula / Scoop manifest for `apache-hive-beeline`

These files package the `hive-beeline` `standalone` classifier jar (the shaded fat jar
produced by `beeline/pom.xml`, containing `org.apache.hive.beeline.BeeLine` and all its
runtime dependencies) as a standalone CLI tool, installable without a full Hive
distribution:

- `packaging/src/main/homebrew/apache-hive-beeline.rb` — Homebrew formula, intended for a
  dedicated `apache/homebrew-hive` tap.
- `packaging/src/main/scoop/apache-hive-beeline.json` — Scoop manifest, intended for a
  dedicated `apache/scoop-hive` bucket.

Both pull the jar directly from Maven Central:

```
https://repo1.maven.org/maven2/org/apache/hive/hive-beeline/<version>/hive-beeline-<version>-standalone.jar
```

Neither file is built or validated by the Maven `packaging` module — they are reference
sources checked into the Hive repo. Publishing them requires copying them into the
respective tap/bucket repositories.

## Releasing a new version

On every Hive release that ships an updated `hive-beeline` `standalone` jar:

1. Wait for the artifact to appear on Maven Central (`hive-beeline-<version>-standalone.jar`).
2. Download the published `hive-beeline-<version>-standalone.jar.sha256` (Maven Central
   publishes a checksum file alongside every artifact) — do not compute the hash by hand.
3. Bump `version`/`url` (and `hash`/`sha256`) in both files to match.
4. Sync the updated files into `apache/homebrew-hive` (`Formula/apache-hive-beeline.rb`)
   and `apache/scoop-hive` (`bucket/apache-hive-beeline.json`).

The `livecheck` block (formula) and `checkver`/`autoupdate` blocks (manifest) let
`brew livecheck` / `scoop update` detect new releases automatically from Maven Central's
`maven-metadata.xml`, but the version/hash bump itself is still a manual step.

## JDK dependency

Both files pin to **JDK 21** (`openjdk@21` for brew, `java/openjdk21` for scoop), matching
`maven.compiler.target=21` in the root `pom.xml`. Scoop users must first run
`scoop bucket add java` — Scoop does not auto-add dependency buckets.

## JVM flags

The `--add-opens java.base/...=ALL-UNNAMED` flags and
`-Dlog4j.configurationFile=beeline-log4j2.properties` are carried over from
`bin/ext/beeline.sh`, which requires them for BeeLine to run correctly under modern JDKs.
