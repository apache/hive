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

# Hive + Gravitino + Keycloak: Docker-Compose Setup

This repository contains a docker-compose-based setup integrating Apache Hive, Gravitino Iceberg REST server, and Keycloak for OAuth2 authentication. It allows Hive to use an Iceberg REST catalog secured via Keycloak.

## Table of Contents
- Architecture Overview
- Prerequisites
- Quickstart
- Configuration
  - Keycloak
  - Gravitino
  - Hive
- Networking Notes

## Architecture Overview
This diagram illustrates the key docker-compose components and their interactions in this setup:

```
                                  oAuth2 (REST API)
         +-------------------------------------------------------------------+
         |                                                                   |
         |                                                                   v
+--------+----------+               +-------------------+            +-----------------+
|                   |  RESTCatalog  |                   |   oauth2   |                 |
|     Hive          |   (REST API)  |      Gravitino    | (REST API) |    Keycloak     |
|  (HiveServer2)    +-------------->|    Iceberg REST   +----------->|  OAuth2 Auth    |
|                   |               |       Server      |            |     Server      |
+--------+----------+               +---------+---------+            +-----------------+
         |                                    |                    
  data   |          metadata files            |                    
  files  +------------------------------------+                    
         |                                                 
         v                                                 
+-------------------+               +-------------------+     
|                   |  creates dir  |                   |     
|     /warehouse    |<--------------+       init        |
|  (Docker volume)  |     sets      |     container     |
|                   |  permissions  |                   |
+-------------------+               +-------------------+
```

- Hive:
    - Runs HiveServer2, connects to Gravitino via Iceberg REST catalog.
    - Write Iceberg data files to the shared warehouse volume.
- Gravitino:
    - Exposes REST API for Iceberg catalog.
    - Writes Iceberg metadata files to shared warehouse volume (.metadata.json).
    - Doesn't supports serving as oauth2 provider, so this example uses an external OAuth2 provider (Keyclock).
- Keycloak: 
  - OAuth2 server providing authentication and token issuance for Hive/Gravitino.
- /warehouse:
    - Shared Docker volume for Iceberg table data and metadata.
- Init container:
    - Creates shared /warehouse folder and sets filesystem permissions as a one time initialization step.

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

### Configuration

#### Keycloak

- Realm: hive
- Client: iceberg-client
  - Secret: iceberg-client-secret
  - Protocol: OpenID Connect
  - Audience: hive-iceberg
- Imported via `realm-export.json` in Keycloak container.
- Port: 8080

#### Gravitino

- HTTP port: 9001
- Catalog backend: JDBC H2 (/tmp/gravitino_h2_db)
- Warehouse: /warehouse (shared with Hive)
- Iceberg REST Catalog Backend config:
    ```
    # Backend type for the catalog. Here we use JDBC (H2 database) as the metadata store.
    gravitino.iceberg-rest.catalog-backend = jdbc
    
    # JDBC connection URI for the H2 database storing catalog metadata.
    gravitino.iceberg-rest.uri = jdbc:h2:file:/tmp/gravitino_h2_db;AUTO_SERVER=TRUE
    
    # JDBC driver class used to connect to the metadata database.
    gravitino.iceberg-rest.jdbc-driver = org.h2.Driver
    
    # Database username for connecting to the metadata store.
    gravitino.iceberg-rest.jdbc-user = sa
    
    # Database password for connecting to the metadata store (empty here).
    gravitino.iceberg-rest.jdbc-password = ""
    
    # Whether to initialize the catalog schema on startup.
    gravitino.iceberg-rest.jdbc-initialize = true
    
    # --- Warehouse Location (shared folder) ---
    
    # Path to the Iceberg warehouse directory shared with Hive.
    gravitino.iceberg-rest.warehouse = file:///warehouse
    ```
- OAuth2 config pointing to Keycloak:
    ```
    # Enables OAuth2 as the authentication mechanism for Gravitino.
    gravitino.authenticators = oauth
    
    # URL of the Keycloak realm to request tokens from.
    gravitino.authenticator.oauth.serverUri = http://keycloak:8080/realms/hive
    
    # Path to the OAuth2 token endpoint on Keycloak.
    gravitino.authenticator.oauth.tokenPath = /protocol/openid-connect/token
    
    # OAuth2 scopes requested when obtaining a token. Includes "openid" and the custom "catalog" scope.
    gravitino.authenticator.oauth.scope = openid catalog
    
    # OAuth2 client ID registered in Keycloak.
    gravitino.authenticator.oauth.clientId = iceberg-client
    
    # OAuth2 client secret associated with the client ID.
    gravitino.authenticator.oauth.clientSecret = iceberg-client-secret
    
    # Java class used to validate incoming JWT tokens using the JWKS endpoint.
    gravitino.authenticator.oauth.tokenValidatorClass = org.apache.gravitino.server.authentication.JwksTokenValidator
    
    # URL to fetch JSON Web Key Set (JWKS) for verifying token signatures.
    gravitino.authenticator.oauth.jwksUri = http://keycloak:8080/realms/hive/protocol/openid-connect/certs
    
    # Identifier for the OAuth2 provider configuration in Gravitino.
    gravitino.authenticator.oauth.provider = default
    
    # JWT claim field(s) to extract as the principal/username (here, 'sub' claim).
    gravitino.authenticator.oauth.principalFields = sub
    
    # Acceptable clock skew (in seconds) when validating token expiration times.
    gravitino.authenticator.oauth.allowSkewSecs = 60
    
    # Expected audience claim in the token to ensure it is intended for this service.
    gravitino.authenticator.oauth.serviceAudience = hive-iceberg
    ```

#### Hive

- Uses ```HiveRESTCatalogClient``` for connecting to Iceberg REST catalog (Gravitino).
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
      <value>http://gravitino:9001/iceberg</value>
      <description>URI of the Iceberg REST server (Gravitino). Hive will send catalog requests here.</description>
    </property>
    
    <property>
      <name>iceberg.catalog.ice01.type</name>
      <value>rest</value>
      <description>Defines the catalog type as "rest", indicating it uses a REST API backend.</description>
    </property>
    
    <!-- Iceberg REST Catalog: OAuth2 authentication -->
    
    <property>
      <name>iceberg.catalog.ice01.rest.auth.type</name>
      <value>oauth2</value>
      <description>Configures Hive to use OAuth2 for authenticating requests to the REST catalog.</description>
    </property>
    
    <property>
      <name>iceberg.catalog.ice01.oauth2-server-uri</name>
      <value>http://keycloak:8080/realms/hive/protocol/openid-connect/token</value>
      <description>URL of the Keycloak OAuth2 token endpoint used to request access tokens.</description>
    </property>
    
    <property>
      <name>iceberg.catalog.ice01.credential</name>
      <value>iceberg-client:iceberg-client-secret</value>
      <description>Client credentials (ID and secret) used to authenticate with Keycloak.</description>
    </property>
    ```
- HiveServer2 port: 10000 (mapped to 10001 in Docker Compose)

## Networking Notes

- All containers share a custom bridge network ```hive-net```.
- Services communicate via container names: hive, gravitino, keycloak.
- Ports mapped for host access:
  - Keycloak → 8080
  - Gravitino → 9001
  - HiveServer2 → 10001

