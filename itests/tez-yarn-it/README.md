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

# Tez-on-YARN localization integration test

An opt-in integration test module that runs Hive-on-Tez against a real Docker-containerized
HDFS + YARN cluster to verify that `hive-exec.jar` is correctly localized for Tez AM and
task containers — a scenario that in-process mini-clusters (MiniTezCluster, MiniDFSCluster)
do not exercise.

## Prerequisites

* Java 21
* Maven 3.6.3 or later
* Docker Desktop (or Docker Engine) with at least **4 GB** of memory assigned

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

This module is **opt-in**: tests are skipped by default. You must pass `-Pitests,tez-yarn`
(or `-Drun.tez.yarn.tests=true`) for Surefire to execute anything. Running
`mvn test -pl itests/tez-yarn-it` without those flags reports zero tests by design.

```bash
mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it
```

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

`StartTezYarnCluster` starts the full HDFS + YARN + HiveServer2 stack and blocks until
`Ctrl+C`. An optional port can be passed via `-Dtez.yarn.cluster.hs2.port` (default: 10000).

```bash
mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it \
    -Dtest=StartTezYarnCluster \
    -Dtez.yarn.cluster.run=true
```

Once ready, the log prints the JDBC URL and Beeline command. Connect from a second terminal:

```bash
beeline -u 'jdbc:hive2://localhost:10000/default;auth=noSasl' -n hive
```

Run a Tez-on-YARN query to exercise jar localization (including `INSERT ... VALUES`):

```sql
CREATE TABLE test_tez (id INT, name STRING) STORED AS ORC;
INSERT INTO test_tez VALUES (1, 'hello'), (2, 'world');
SELECT * FROM test_tez;
```

## Troubleshooting

* **Compose hangs or fails during image pull** — confirm Docker is running and that at
  least 4 GB of memory is assigned to the Docker VM. Retry after `docker system prune` if
  disk space is low.
* **Docker Desktop 25+ API errors** — the bundled `ComposeImageSubstitutor` rewrites
  Testcontainers' helper image to a version compatible with recent Docker daemons. Override
  with `-Dtez.yarn.compose.image=docker:<tag>` if needed.
* **Zero tests executed** — pass `-Pitests,tez-yarn` or `-Drun.tez.yarn.tests=true`; see
  [Running the automated tests](#running-the-automated-tests).
* **Missing dependency / compile errors on a partial build** — use `-am` when running only
  this module (see above).

## Re-deploying after changing `hive-exec`

The `hive-exec.jar` localized into YARN task containers is resolved from the Maven test
classpath (`ql/target/hive-exec-*.jar`). To pick up code changes:

1. Rebuild `hive-exec`:

   ```bash
   mvn package -DskipTests -pl ql
   ```

2. Re-run the integration test or `StartTezYarnCluster`.
