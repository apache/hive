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

# Hive + Polaris: Docker-Compose Setup

This package contains a docker-compose-based setup integrating Apache Hive and Polaris. 
It allows Hive to use an Iceberg REST catalog secured with oauth2 provided by Polaris.

## Table of Contents
- Architecture Overview
- Prerequisites
- Quickstart
- Configuration
  - Polaris
  - Hive
- Networking Notes

## Architecture Overview
This diagram illustrates the key docker-compose components and their interactions in this setup:
```
+-------------------+               +-------------------+
|                   |  RESTCatalog  |                   |
|     Hive          |   (REST API)  |      Polaris      |<-------+
|  (HiveServer2)    +-------------->|      Server       |        |
|                   |    oAuth2     |                   |        |  
+--------+----------+  (REST API)   +---------+---------+        | creates:
         |                                    |                  |     catalog,
  data   |           metadata files           |                  |     principal,
  files  +------------------------------------+                  |     roles,
         |                                                       |     grants (REST API)
         v                                                       |
+-------------------+               +-------------------+        |
|                   |  creates dir  |                   |        |
|     /warehouse    |<--------------+    Polaris-init   +--------+
|  (Docker volume)  |     syncs     |      container    |
|                   |  permissions  |                   |
+-------------------+               +-------------------+
```

- Hive: 
  - Runs HiveServer2, connects to Polaris via Iceberg REST catalog. 
  - Write Iceberg data files to shared warehouse volume.
- Polaris: 
  - Exposes REST API for Iceberg catalog and provides oauth2 for authentication. 
  - Supports serving as oauth2 provider, so this example doesn't need an external OAuth2 component.
  - Writes Iceberg metadata files to shared warehouse volume (.metadata.json).
- /warehouse: 
  - Shared Docker volume for Iceberg table data and metadata.
- Polaris-init
  - Bootstraps Polaris for Hive-Iceberg.
  - Creates and configures Polaris resources via REST API.
  - Continuously synchronizes filesystem permissions for the shared /warehouse/* folders.
    - required because Polaris and Hive run as different users in their respective containers.

## Prerequisites
- Docker & Docker Compose
- Java (for local Hive beeline client)
- ```$HIVE_HOME``` environment variable pointing to Hive installation (for connecting to Beeline)

## Quickstart
### STEP 1: Build Hive Docker Image
- Currently, only Hive 4.2.0-SNAPSHOT has the required Iceberg REST catalog client.
- To build the Hive Docker image with Iceberg REST catalog client on your local machine, follow these steps:
    - Build Hive using Docker profile.
    - Copy ```apache-hive-4.2.0-SNAPSHOT-bin.tar.gz``` to ```packaging/cache``` folder.
    - Execute ```build.sh``` from ```packaging/src/docker/``` folder.

### STEP 2: Export the Hive version
```shell
export HIVE_VERSION=4.2.0-SNAPSHOT
```

### STEP 3: Start services
```shell
docker-compose up -d
```

### STEP 4: Connect to beeline
```shell
"${HIVE_HOME}/bin/beeline" -u "jdbc:hive2://localhost:10001/default" -n hive -p hive
```

### STEP 5: Stop services:
```shell
docker-compose down -v
```

## Configuration

### Polaris

- HTTP port: 8181
- Warehouse: /warehouse (shared with Hive)
- Key Polaris configs (defined via env variables in docker-compose.yml) :
     ```
      # A realm provides logical isolation for different Polaris environments.
      polaris.realm-context.realms: POLARIS
  
      # Initial bootstrap credentials for the Polaris server.
      # The format is: <realm-name>,<client-id>,<client-secret>
      POLARIS_BOOTSTRAP_CREDENTIALS: POLARIS,iceberg-client,iceberg-client-secret`
    ```

### Hive

- Uses ```HiveRESTCatalogClient``` for connecting to Iceberg REST catalog (Polaris).
- Catalog configuration in ```hive-site.xml```:
    ```
    <property>
      <name>metastore.catalog.default</name>
      <value>ice01</value>
      <description>Sets the default Iceberg catalog for Hive. Here, "ice01" is used.</description>
    </property>
    
    <property>
      <name>metastore.client.impl</name>
      <value>org.apache.iceberg.hive.client.HiveRESTCatalogClient</value>
      <description>Specifies the client implementation to use for accessing Iceberg via REST.</description>
    </property>
    
    <property>
      <name>iceberg.catalog.ice01.uri</name>
      <value>http://polaris:8181/api/catalog</value>
      <description>URI of the Iceberg REST server (Polaris). Hive will send catalog requests here.</description>
    </property>
    
    <property>
      <name>iceberg.catalog.ice01.type</name>
      <value>rest</value>
      <description>Defines the catalog type as "rest", indicating it uses a REST API backend.</description>
    </property>
  
    <property>
      <name>hive.metastore.warehouse.dir</name>
      <value>file:///warehouse</value>
      <description>Defines the warehouse location, required for Polaris</description>
    </property>
    
    <!-- Iceberg REST Catalog: OAuth2 authentication -->
    
    <property>
      <name>iceberg.catalog.ice01.rest.auth.type</name>
      <value>oauth2</value>
      <description>Configures Hive to use OAuth2 for authenticating requests to the REST catalog.</description>
    </property>
    
    <property>
      <name>iceberg.catalog.ice01.oauth2-server-uri</name>
      <value>http://polaris:8181/api/catalog/v1/oauth/tokens</value>
      <description>URL of the Polaris OAuth2 token endpoint used to request access tokens.</description>
    </property>
    
    <property>
      <name>iceberg.catalog.ice01.credential</name>
      <value>iceberg-client:iceberg-client-secret</value>
      <description>Client credentials (ID and secret) used to authenticate with Keycloak.</description>
    </property>
  
    <property>
      <name>iceberg.catalog.ice01.scope</name>
      <value>PRINCIPAL_ROLE:ALL</value>
      <description>oAuth2 scope tied to the principal role defined in Polaris</description>
    </property>
    ```
- HiveServer2 port: 10000 (mapped to 10001 in Docker Compose)

## Networking Notes

- All containers share a custom bridge network ```hive-net```.
- Services communicate via container names: hive and polaris
- Ports mapped for host access:
  - Polaris → 8181
  - HiveServer2 → 10001

