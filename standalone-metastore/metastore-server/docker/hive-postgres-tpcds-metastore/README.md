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
# Postgres TPC-DS metastore

A dockerized Postgres database with a Hive metastore dump from a
[TPC-DS 30TB dataset](https://github.com/zabetak/hive-test-datasets/releases/download/1.0/metastore_tpcds30tb_3_1_3000.dump.gz).

## Build and deploy 

### Docker Hub

Use the GitHub CI workflow `postgres-tpcds-metastore.yml` for building and deploying the image to 
the official ASF Docker Hub registry. 

## Manual

Build and tag the docker image: `docker build --tag apache/hive-postgres-tpcds-metastore:1.4 .`

## Usage

-   Create and start Postgres container:
    `docker run --name postgres_metastore -p 5432:5432 -e POSTGRES_PASSWORD=postgres -d apache/hive-postgres-tpcds-metastore:1.4`
-   Verify that the container is running: `docker ps`
-   Stop Postgres container: `docker stop postgres_metastore`
-   Remove Postgres container: `docker rm postgres_metastore`

If you want to check the contents of the metastore the easiest way would be to
open a shell in the container and connect to the database via psql.

    docker exec -it postgres_metastore bash
    su postgres
    psql -U hive -d metastore

The default configuration binds the host port 5432 to the database running in
the container. You can access the database via JDBC using the following
information:

-   URL: `jdbc:postgresql://localhost:5432/metastore`
-   DRIVER: `org.postgresql.Driver`
-   USER: `hive`
-   PASSWORD: `hive`

If you want to start Hive and instruct it to use this database as the metastore
you have to set the following properties in `hive-site.xml`:

-   `javax.jdo.option.ConnectionURL`
-   `javax.jdo.option.ConnectionDriverName`
-   `javax.jdo.option.ConnectionUserName`
-   `javax.jdo.option.ConnectionPassword`

If you need to use the current dumps with a more recent version of Hive then
after creating and starting the Postgres container you can use the
[schematool](https://hive.apache.org/docs/latest/admin/hive-schema-tool/)
to upgrade the metastore:

    schematool -dbType postgres -upgradeSchemaFrom 3.1.3000 -driver org.postgresql.Driver -url jdbc:postgresql://localhost:5432/metastore -userName hive -passWord hive
