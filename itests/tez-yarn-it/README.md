
<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Tez-on-YARN integration test

An opt-in integration test module that runs Hive-on-Tez against a real Docker-containerized
HDFS + YARN cluster. It is used to verify YARN container localization behavior (including
`hive-exec.jar`) and is also useful as a small "real" Hadoop cluster for manual Tez-on-YARN
testing and debugging.

MiniTezCluster/MiniDFSCluster are not sufficient for this class of testing because they do
not exercise the real YARN NodeManager localization path (`LocalResource` download/extract
to container-local directories before AM/task launch). In mini-clusters, Tez tasks run without
that real container-localization lifecycle.

## Prerequisites

- Java 21
- Maven 3.6.3 or later
- Docker Desktop (or Docker Engine) with at least **4 GB** of memory assigned

Docker Desktop 25+ exposes a newer Docker API than Testcontainers' default helper image
expects. This module ships `ComposeImageSubstitutor` and `testcontainers.properties` to
substitute a compatible helper image automatically; no extra configuration is required on
supported setups.

## First-time setup

Build the full Hive distribution once to populate `$HIVE_HOME/lib/` and install all
artifacts to `~/.m2`:

```bash
mvn clean install -DskipTests -Pitests,dist
```



## Running the automated tests

This module is **opt-in**: tests are skipped by default. Activate the `tez-yarn` profile
with `-Ptez-yarn` to run them. Running `mvn test -pl itests/tez-yarn-it` without the
profile skips test execution by design.

```bash
mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it
```

By default, this module assembles `target/tez-libs.tar.gz` from Maven dependencies and
uses that staged archive for `tez.lib.uris`. You can also select an alternative source:

- **Default staged archive** (same as above):
  ```bash
  mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it \
      -Dtez.dist.source=staged
  ```
- **Local archive** (for example from a local Tez checkout):
  ```bash
  mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it \
      -Dtez.dist.source=local \
      -Dtez.dist.tarball=$HOME/apache/tez/tez-dist/target/tez-0.10.6-SNAPSHOT-minimal.tar.gz
  ```
- **Download archive** (defaults to Apache archive URL for `${tez.version}`):
  ```bash
  mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it \
      -Dtez.dist.source=download
  ```
- **Download from a custom URL/mirror**:
  ```bash
  mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it \
      -Dtez.dist.source=download \
      -Dtez.dist.download.url=https://<mirror>/apache-tez-<version>-bin.tar.gz
  ```

The selected archive must be a Tez distribution tarball suitable for `tez.lib.uris`
(for example Apache Tez `*-bin.tar.gz` or Tez dist `*-minimal.tar.gz` outputs).

If you have not run a full install recently, add `-am` to build required upstream modules:

```bash
mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it -am
```

To run a single test:

```bash
mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it \
    -Dtest=TestTezYarnLocalization#testQuerySucceedsWithAppJar
```



## Starting a keep-alive cluster for manual testing

To start the cluster and keep it running for manual Beeline testing, explicitly
run `StartTezYarnCluster`.

```bash
mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it -Dtest=StartTezYarnCluster
```

Once ready, a startup banner is printed to standard output with JDBC, HDFS/YARN endpoints,
and the log file location. Connect from a second terminal:

```bash
beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n hive
```

Useful web UIs while the cluster is up:

- NameNode UI: `http://localhost:9870`
- ResourceManager UI: `http://localhost:8088`
- NodeManager UI: `http://localhost:8042`
- NodeManager AM RPC range: `nodemanager:41000-41020` (shown in the startup banner)

Run a Tez-on-YARN query to exercise jar localization (including `INSERT ... VALUES`):

```sql
CREATE TABLE test_tez (id INT, name STRING) STORED AS ORC;
INSERT INTO test_tez VALUES (1, 'hello'), (2, 'world');
set hive.fetch.task.conversion=none;
SELECT * FROM test_tez;
```



## Troubleshooting

- **Compose hangs or fails during image pull** — confirm Docker is running and that at
least 4 GB of memory is assigned to the Docker VM. Retry after `docker system prune` if
disk space is low.
- **Cluster appears to be left running** — tear down the compose stack manually:
  ```bash
  docker compose -f itests/tez-yarn-it/src/test/docker/hadoop-yarn/docker-compose.yml down -v --remove-orphans
  ```
- **Docker Desktop 25+ API errors** — the bundled `ComposeImageSubstitutor` rewrites
Testcontainers' helper image to a version compatible with recent Docker daemons. Override
with `-Dtez.yarn.compose.image=docker:<tag>` if needed.
- **`tez.dist.source` validation failure** — accepted values are `staged` (default),
  `local`, and `download`.
- **`tez.dist.source=local` fails** — pass an existing path with
  `-Dtez.dist.tarball=/absolute/path/to/tez.tar.gz`.
- **Zero tests executed** — activate the `tez-yarn` profile with `-Pitests,tez-yarn`;
see [Running the automated tests](#running-the-automated-tests).
- **Missing dependency / compile errors on a partial build** — use `-am` when running only
this module (see above).



## Re-deploying after changing `hive-exec`

The `hive-exec.jar` localized into YARN task containers is resolved from the Maven test
classpath (`ql/target/hive-exec-*.jar`). To pick up code changes:

1. Rebuild `hive-exec`:
  ```bash
   mvn package -DskipTests -pl ql
  ```
2. Re-run the integration test or `StartTezYarnCluster`.

