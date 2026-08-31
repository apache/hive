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

## First-time setup

Build the full Hive distribution once to populate `$HIVE_HOME/lib/` and install all
artifacts to `~/.m2`:

```bash
mvn clean install -DskipTests -Pitests,dist
```

## Running the automated tests

```bash
mvn test -Pitests,tez-yarn -pl itests/tez-yarn-it
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

## Re-deploying after changing `hive-exec`

The `hive-exec.jar` localized into YARN task containers is resolved from the Maven test
classpath (`ql/target/hive-exec-*.jar`). To pick up code changes:

1. Rebuild `hive-exec`:

   ```bash
   mvn package -DskipTests -pl ql
   ```

2. Re-run the integration test or `StartTezYarnCluster`.

## Module layout

```
itests/tez-yarn-it/
├── pom.xml
└── src/test/
    ├── docker/hadoop-yarn/
    │   ├── Dockerfile                         # Custom Hadoop + JDK 21 image
    │   └── config                             # Hadoop daemon environment variables
    ├── java/.../tez/yarn/
    │   ├── TezYarnClusterContainer.java       # Testcontainers cluster orchestration
    │   ├── TestTezYarnLocalization.java       # End-to-end localization assertion
    │   ├── TestHiveServer2Connectivity.java   # Basic JDBC connectivity smoke test
    │   ├── TestTezYarnClusterContainer.java   # Cluster smoke tests (no Hive)
    │   └── StartTezYarnCluster.java           # Keep-alive cluster for manual testing
    └── resources/
        ├── hive-site-yarn-it.xml              # Hive/Tez/HS2 constants for the test JVM
        └── yarn-site.xml                      # YARN NM memory check overrides
```
