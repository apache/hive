<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
### Introduction

---
Run Apache Hive Metastore inside docker container
- Quick-start for Hive Metastore


## Quickstart
### STEP 1: Pull the image
- Pull the image from DockerHub: https://hub.docker.com/r/apache/hive/tags. 

Here are the latest images:
- standalone-metastore-4.0.0

```shell
docker pull apache/hive:standalone-metastore-4.0.0
```
### STEP 2: Export the Hive version
```shell
export HIVE_VERSION=4.0.0
```

### STEP 3: Launch Standalone Metastore backed by Derby,
```shell
docker run -d -p 9083:9083 --name metastore-standalone apache/hive:standalone-metastore-${HIVE_VERSION}
```

### Detailed Setup
---
#### Build image
Apache Hive Metastore relies on Hadoop and some others to facilitate managing metadata of large datasets. 
The `build.sh` provides ways to build the image against specified version of the dependent, as well as build from source.

##### Build from source
```shell
mvn clean package -DskipTests -Pdocker
```
##### Build with specified version
There are some arguments to specify the component version:
```shell
-hadoop <hadoop version>
-hive <hive version> 
```
If the version is not provided, it will read the version from current `pom.xml`:
`project.version`, `hadoop.version` for Hive and Hadoop respectively. 

For example, the following command uses Hive 4.0.0 and Hadoop `hadoop.version` to build the image,
```shell
./build.sh -hive 4.0.0
```
If the command does not specify the Hive version, it will use the local `hive-standalone-metastore-${project.version}-bin.tar.gz`(will trigger a build if it doesn't exist),
together with Hadoop 3.1.0 to build the image,
```shell
./build.sh -hadoop 3.1.0
```
After building successfully, we can get a Docker image named `apache/hive` tagged with `standalone-metastore` prefix and the provided Hive version.

#### Run services

Before going further, we should define the environment variable `HIVE_VERSION` first.
For example, if `-hive 4.0.0` is specified to build the image,
```shell
export HIVE_VERSION=4.0.0
```
or assuming that you're relying on current `project.version` from pom.xml,
```shell
export HIVE_VERSION=$(mvn -f pom.xml -q help:evaluate -Dexpression=project.version -DforceStdout)
```
- Metastore

For a quick start, launch the Metastore with Derby,
  ```shell
  docker run -d -p 9083:9083 --name metastore-standalone apache/hive:standalone-metastore-${HIVE_VERSION}
  ```
  Everything would be lost when the service is down. In order to save the Hive table's schema and data, start the container with an external Postgres and Volume to keep them,

  ```shell
  docker run -d -p 9083:9083 --env DB_DRIVER=postgres \
       --env SERVICE_OPTS="-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres:5432/metastore_db -Djavax.jdo.option.ConnectionUserName=hive -Djavax.jdo.option.ConnectionPassword=password" \
       --mount source=warehouse,target=/opt/hive/data/warehouse \
       --mount type=bind,source=`mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout`/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar,target=/opt/hive/lib/postgres.jar \
       --name metastore-standalone apache/hive:standalone-metastore-${HIVE_VERSION}
  ```

  If you want to use your own `hdfs-site.xml` for the service, you can provide the environment variable `HIVE_CUSTOM_CONF_DIR` for the command. For instance, put the custom configuration file under the directory `/opt/hive/conf`, then run,

  ```shell
   docker run -d -p 9083:9083 --env DB_DRIVER=postgres \
        -v /opt/hive/conf:/hive_custom_conf --env HIVE_CUSTOM_CONF_DIR=/hive_custom_conf \
        --mount type=bind,source=`mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout`/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar,target=/opt/hive/lib/postgres.jar \
        --name metastore apache/hive:standalone-metastore-${HIVE_VERSION}
  ```

NOTE:

1) For Hive releases before 4.0, if you want to upgrade the existing external Metastore schema to the target version,
then add "--env SCHEMA_COMMAND=upgradeSchema" to the command.

2) If the full Acid support (Compaction) is needed, use the Hive docker image to bring up the container.

- Metastore with Postgres

To spin up Metastore with a remote DB, there is a `docker-compose.yml` placed under `packaging/src/docker` for this purpose,
specify the `POSTGRES_LOCAL_PATH` first:
```shell
export POSTGRES_LOCAL_PATH=your_local_path_to_postgres_driver
```
Example:
```shell
mvn dependency:copy -Dartifact="org.postgresql:postgresql:42.7.3" && \
export POSTGRES_LOCAL_PATH=`mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout`/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar 
```
If you don't install maven or have problem in resolving the postgres driver, you can always download this jar yourself,
change the `POSTGRES_LOCAL_PATH` to the path of the downloaded jar.

- Metastore with S3 support

Download aws-java-sdk-bundle-xxx.jar and place it under the jars directory:
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.770/aws-java-sdk-bundle-1.12.770.jar -P jars/

Add the `fs.s3a.access.key` and `fs.s3a.secret.key` properties in `metastore-site.xml` under the conf directory.

Then,
```shell
docker compose up -d
```
Metastore and Postgres services will be started as a consequence.

Volumes:
- hive_db

  The volume persists the metadata of Hive tables inside Postgres container.
- warehouse

  The volume stores tables' files inside HiveServer2 container.

To stop/remove them all,
```shell
docker compose down
```