<!--
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
-->

# Hive Kubernetes Operator

A Java-based Kubernetes operator that manages Apache Hive clusters declaratively
using a single `HiveCluster` custom resource. Built with
[Java Operator SDK (JOSDK)](https://javaoperatorsdk.io/) and
[fabric8 Kubernetes client](https://github.com/fabric8io/kubernetes-client).

## Features

- **Single CRD** (`HiveCluster`) manages all Hive components
- **Four Hive services**: Metastore, HiveServer2, LLAP, and Tez AM
- **Helm chart** with sensible defaults — provide DB + ZK + storage, get a full-HA cluster
- **Storage-agnostic**: works with any Hadoop-compatible filesystem (S3A,
  ABFS, GCS, HDFS, Ozone)
- **Automatic dependency ordering**: schema init -> Metastore -> HiveServer2 -> LLAP/TezAM
- **Optional components**: LLAP and Tez AM enabled/disabled via flags
- **External Metastore**: point HiveServer2 at an existing Metastore
- **Status reporting**: per-component readiness tracked on the CRD status

---

## Build from Source

```bash
# Build the operator JAR + CRD + Helm chart (no Docker image)
mvn clean package -pl packaging/src/kubernetes -DskipTests

# Build everything including the Docker image (includes the above)
mvn clean package -pl packaging/src/kubernetes -Pkubernetes -DskipTests
```

| Artifact | Path |
|----------|------|
| Shaded JAR | `target/hive-kubernetes-operator-*-shaded.jar` |
| CRD YAML | `helm/hive-operator/crds/hiveclusters.hive.apache.org-v1.yml` |
| Helm chart | `helm/hive-operator/` |
| Docker image | `apache/hive:operator-<version>` |

---

## Quick Start (Helm)

The Helm chart defaults to a **Full-HA** cluster (Metastore x2, HiveServer2 x2,
LLAP x2, TezAM x2). You only need to provide three things: database, ZooKeeper,
and storage.

### Prerequisites

- Kubernetes 1.25+
- Helm 3.x
- A ZooKeeper instance (or install one below)
- A storage backend (Ozone, S3, ABFS, GCS, HDFS)
- A supported RDBMS for the Metastore (or install one below)

### Step 1: Install Dependencies

```bash
# ZooKeeper
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install zookeeper bitnami/zookeeper \
  --set replicaCount=1 --set auth.enabled=false \
  --set image.repository=bitnamilegacy/zookeeper \
  --set image.tag=3.9.3-debian-12-r21 \
  --set global.security.allowInsecureImages=true --wait

# PostgreSQL
helm install postgres bitnami/postgresql \
  --set auth.username=hive --set auth.password=hive123 \
  --set auth.database=metastore --wait

# Create the DB password secret
kubectl create secret generic hive-db-secret --from-literal=password=hive123
```

If using **Ozone** as the storage backend:

```bash
helm repo add ozone https://apache.github.io/ozone-helm-charts/
helm install ozone ozone/ozone --version 0.2.0 --wait
sleep 50
kubectl exec statefulset/ozone-om -- ozone sh volume create /s3v
kubectl exec statefulset/ozone-om -- ozone sh bucket create /s3v/hive
```

### Step 2: Install the Hive Operator + Cluster

Choose your storage backend from the examples below. Each shows the CLI command
and an equivalent values file.

---

## Storage Backend Examples

Each example below shows both the `helm install` CLI command and the equivalent
`values.yaml` file. Use whichever approach you prefer.

### Ozone (Full-HA, default behavior)

**CLI:**

```bash
helm install hive ./helm/hive-operator \
  --set cluster.database.type=postgres \
  --set cluster.database.url="jdbc:postgresql://postgres-postgresql:5432/metastore" \
  --set cluster.database.driver="org.postgresql.Driver" \
  --set cluster.database.username=hive \
  --set cluster.database.passwordSecretRef.name=hive-db-secret \
  --set cluster.database.passwordSecretRef.key=password \
  --set cluster.database.driverJarUrl="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar" \
  --set cluster.zookeeper.quorum="zookeeper:2181" \
  --set cluster.storage.coreSiteOverrides."fs\.defaultFS"="s3a://hive" \
  --set cluster.storage.coreSiteOverrides."fs\.s3a\.endpoint"="http://ozone-s3g-rest:9878" \
  --set-string cluster.storage.coreSiteOverrides."fs\.s3a\.path\.style\.access"=true \
  --set 'cluster.storage.envVars[0].name=HADOOP_OPTIONAL_TOOLS' \
  --set 'cluster.storage.envVars[0].value=hadoop-aws' \
  --set 'cluster.storage.envVars[1].name=AWS_ACCESS_KEY_ID' \
  --set 'cluster.storage.envVars[1].value=ozone' \
  --set 'cluster.storage.envVars[2].name=AWS_SECRET_ACCESS_KEY' \
  --set 'cluster.storage.envVars[2].value=ozone'
```

**Values file:**

```yaml
# values.yaml
cluster:
  database:
    type: postgres
    url: "jdbc:postgresql://postgres-postgresql:5432/metastore"
    driver: "org.postgresql.Driver"
    username: hive
    passwordSecretRef:
      name: hive-db-secret
      key: password
    driverJarUrl: "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar"

  zookeeper:
    quorum: "zookeeper:2181"

  storage:
    coreSiteOverrides:
      fs.defaultFS: "s3a://hive"
      fs.s3a.endpoint: "http://ozone-s3g-rest:9878"
      fs.s3a.path.style.access: "true"
    envVars:
      - name: HADOOP_OPTIONAL_TOOLS
        value: "hadoop-aws"
      - name: AWS_ACCESS_KEY_ID
        value: "ozone"
      - name: AWS_SECRET_ACCESS_KEY
        value: "ozone"
```

```bash
helm install hive ./helm/hive-operator -f values.yaml
```

---

### AWS S3

**CLI:**

Create the secret with your AWS credentials:
```bash
kubectl create secret generic aws-s3-creds \
  --from-literal=accessKey="<KEY>" \
  --from-literal=secretKey="<KEY>"
```

Then install the operator and HiveCluster with the appropriate storage config:

```bash
helm install hive ./helm/hive-operator \
  --set cluster.database.type=postgres \
  --set cluster.database.url="jdbc:postgresql://postgres-postgresql:5432/metastore" \
  --set cluster.database.driver="org.postgresql.Driver" \
  --set cluster.database.username=hive \
  --set cluster.database.passwordSecretRef.name=hive-db-secret \
  --set cluster.database.passwordSecretRef.key=password \
  --set cluster.database.driverJarUrl="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar" \
    --set cluster.zookeeper.quorum="zookeeper:2181" \
  --set cluster.storage.coreSiteOverrides."fs\.defaultFS"="s3a://hive-k8s-bucket" \
  --set 'cluster.storage.envVars[0].name=HADOOP_OPTIONAL_TOOLS' \
  --set 'cluster.storage.envVars[0].value=hadoop-aws' \
  --set 'cluster.storage.envVars[1].name=AWS_ACCESS_KEY_ID' \
  --set 'cluster.storage.envVars[1].valueFrom.secretKeyRef.name=aws-s3-creds' \
  --set 'cluster.storage.envVars[1].valueFrom.secretKeyRef.key=accessKey' \
  --set 'cluster.storage.envVars[2].name=AWS_SECRET_ACCESS_KEY' \
  --set 'cluster.storage.envVars[2].valueFrom.secretKeyRef.name=aws-s3-creds' \
  --set 'cluster.storage.envVars[2].valueFrom.secretKeyRef.key=secretKey'
```

**Values file:**

```yaml
# values.yaml
cluster:
  database:
    type: postgres
    url: "jdbc:postgresql://postgres-postgresql:5432/metastore"
    driver: "org.postgresql.Driver"
    username: hive
    passwordSecretRef:
      name: hive-db-secret
      key: password
    driverJarUrl: "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar"

  zookeeper:
    quorum: "zookeeper:2181"

  storage:
    coreSiteOverrides:
      fs.defaultFS: "s3a://hive-k8s-bucket"
    envVars:
      - name: HADOOP_OPTIONAL_TOOLS
        value: "hadoop-aws"
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: aws-s3-creds
            key: accessKey
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: aws-s3-creds
            key: secretKey
```

```bash
helm install hive ./helm/hive-operator -f values.yaml
```

---

### Google Cloud Storage (GCS)

Create the secret with your GCS service account key:

```bash
kubectl create secret generic gcs-creds  --from-file=key.json=<PATH>.json
```

**CLI:**

```bash
helm install hive ./helm/hive-operator \
  --set cluster.database.type=postgres \
  --set cluster.database.url="jdbc:postgresql://postgres-postgresql:5432/metastore" \
  --set cluster.database.driver="org.postgresql.Driver" \
  --set cluster.database.username=hive \
  --set cluster.database.passwordSecretRef.name=hive-db-secret \
  --set cluster.database.passwordSecretRef.key=password \
  --set cluster.database.driverJarUrl="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar" \
  --set cluster.zookeeper.quorum="zookeeper:2181" \
  --set 'cluster.storage.coreSiteOverrides.fs\.defaultFS=gs://hive-bucket' \
  --set 'cluster.storage.coreSiteOverrides.fs\.gs\.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem' \
  --set 'cluster.storage.coreSiteOverrides.fs\.gs\.auth\.type=SERVICE_ACCOUNT_JSON_KEYFILE' \
  --set 'cluster.storage.coreSiteOverrides.fs\.gs\.auth\.service\.account\.json\.keyfile=/etc/gcs/key.json' \
  --set-string 'cluster.storage.coreSiteOverrides.fs\.gs\.reported\.permissions=777' \
  --set 'cluster.storage.externalJars[0]=https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.25/gcs-connector-hadoop3-2.2.25-shaded.jar' \
  --set 'cluster.storage.volumes[0].name=gcs-key' \
  --set 'cluster.storage.volumes[0].secret.secretName=gcs-creds' \
  --set 'cluster.storage.volumeMounts[0].name=gcs-key' \
  --set 'cluster.storage.volumeMounts[0].mountPath=/etc/gcs' \
  --set 'cluster.storage.volumeMounts[0].readOnly=true'
```

**Values file:**

```yaml
# values.yaml
cluster:
  database:
    type: postgres
    url: "jdbc:postgresql://postgres-postgresql:5432/metastore"
    driver: "org.postgresql.Driver"
    username: hive
    passwordSecretRef:
      name: hive-db-secret
      key: password
    driverJarUrl: "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar"

  zookeeper:
    quorum: "zookeeper:2181"

  storage:
    coreSiteOverrides:
      fs.defaultFS: "gs://hive-bucket"
      fs.gs.impl: "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
      fs.gs.auth.type: "SERVICE_ACCOUNT_JSON_KEYFILE"
      fs.gs.auth.service.account.json.keyfile: "/etc/gcs/key.json"
      fs.gs.reported.permissions: "777"
    externalJars:
      - "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.25/gcs-connector-hadoop3-2.2.25-shaded.jar"
    volumes:
      - name: gcs-key
        secret:
          secretName: gcs-creds
    volumeMounts:
      - name: gcs-key
        mountPath: /etc/gcs
        readOnly: true
```

```bash
helm install hive ./helm/hive-operator -f values.yaml
```

---

## Deployment Modes

### Minimal Cluster (no LLAP/TezAM)

**CLI:**

```bash
helm install hive ./helm/hive-operator \
  --set cluster.database.type=postgres \
  --set cluster.database.url="jdbc:postgresql://postgres-postgresql:5432/metastore" \
  --set cluster.database.driver="org.postgresql.Driver" \
  --set cluster.database.username=hive \
  --set cluster.database.passwordSecretRef.name=hive-db-secret \
  --set cluster.database.passwordSecretRef.key=password \
  --set cluster.database.driverJarUrl="https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar" \
  --set cluster.zookeeper.quorum="zookeeper:2181" \
  --set cluster.storage.coreSiteOverrides."fs\.defaultFS"="s3a://hive" \
  --set cluster.storage.coreSiteOverrides."fs\.s3a\.endpoint"="http://ozone-s3g-rest:9878" \
  --set-string cluster.storage.coreSiteOverrides."fs\.s3a\.path\.style\.access"=true \
  --set 'cluster.storage.envVars[0].name=HADOOP_OPTIONAL_TOOLS' \
  --set 'cluster.storage.envVars[0].value=hadoop-aws' \
  --set 'cluster.storage.envVars[1].name=AWS_ACCESS_KEY_ID' \
  --set 'cluster.storage.envVars[1].value=ozone' \
  --set 'cluster.storage.envVars[2].name=AWS_SECRET_ACCESS_KEY' \
  --set 'cluster.storage.envVars[2].value=ozone' \
  --set cluster.metastore.replicas=1 \
  --set cluster.hiveServer2.replicas=1 \
  --set cluster.llap.enabled=false \
  --set cluster.tezAm.enabled=false
```

**Values file:**

```yaml
# values.yaml
cluster:
  database:
    type: postgres
    url: "jdbc:postgresql://postgres-postgresql:5432/metastore"
    driver: "org.postgresql.Driver"
    username: hive
    passwordSecretRef:
      name: hive-db-secret
      key: password
    driverJarUrl: "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar"

  zookeeper:
    quorum: "zookeeper:2181"

  storage:
    coreSiteOverrides:
      fs.defaultFS: "s3a://hive"
      fs.s3a.endpoint: "http://ozone-s3g-rest:9878"
      fs.s3a.path.style.access: "true"
    envVars:
      - name: HADOOP_OPTIONAL_TOOLS
        value: "hadoop-aws"
      - name: AWS_ACCESS_KEY_ID
        value: "ozone"
      - name: AWS_SECRET_ACCESS_KEY
        value: "ozone"

  metastore:
    replicas: 1
  hiveServer2:
    replicas: 1
  llap:
    enabled: false
  tezAm:
    enabled: false
```

```bash
helm install hive ./helm/hive-operator -f values.yaml
```

---

### External Metastore (skip Metastore deployment)

**CLI:**

```bash
helm install hive ./helm/hive-operator \
  --set cluster.zookeeper.quorum="zookeeper:2181" \
  --set cluster.metastore.enabled=false \
  --set cluster.metastore.externalUri="thrift://my-external-metastore:9083" \
  --set cluster.storage.coreSiteOverrides."fs\.defaultFS"="s3a://hive" \
  --set 'cluster.storage.envVars[0].name=HADOOP_OPTIONAL_TOOLS' \
  --set 'cluster.storage.envVars[0].value=hadoop-aws' \
  --set 'cluster.storage.envVars[1].name=AWS_ACCESS_KEY_ID' \
  --set 'cluster.storage.envVars[1].value=ozone' \
  --set 'cluster.storage.envVars[2].name=AWS_SECRET_ACCESS_KEY' \
  --set 'cluster.storage.envVars[2].value=ozone'
```

**Values file:**

```yaml
# values.yaml
cluster:
  database: {}   # Not needed when metastore is external

  zookeeper:
    quorum: "zookeeper:2181"

  metastore:
    enabled: false
    externalUri: "thrift://my-external-metastore:9083"

  storage:
    coreSiteOverrides:
      fs.defaultFS: "s3a://hive"
    envVars:
      - name: HADOOP_OPTIONAL_TOOLS
        value: "hadoop-aws"
      - name: AWS_ACCESS_KEY_ID
        value: "ozone"
      - name: AWS_SECRET_ACCESS_KEY
        value: "ozone"
```

```bash
helm install hive ./helm/hive-operator -f values.yaml
```

---

### External Iceberg REST Catalog with Apache Polaris (AWS S3)

[Apache Polaris](https://polaris.apache.org/) is an Iceberg REST catalog with
built-in OAuth2. Requires **real AWS S3** (Polaris uses STS credential vending).
See `packaging/src/docker/thirdparties/polaris/` for the Docker Compose equivalent.

**Step 1: Create AWS secret and deploy Polaris**

```bash
kubectl create secret generic aws-s3-creds \
  --from-literal=accessKey="<YOUR_AWS_KEY>" \
  --from-literal=secretKey="<YOUR_AWS_SECRET>"

kubectl run polaris --image=apache/polaris:latest --port=8181 \
  --overrides='{
    "spec": {
      "containers": [{
        "name": "polaris",
        "image": "apache/polaris:latest",
        "ports": [{"containerPort": 8181}],
        "env": [
          {"name": "POLARIS_BOOTSTRAP_CREDENTIALS", "value": "POLARIS,iceberg-client,iceberg-client-secret"},
          {"name": "POLARIS_REALM_CONTEXT_REALMS", "value": "POLARIS"},
          {"name": "QUARKUS_OTEL_SDK_DISABLED", "value": "true"},
          {"name": "POLARIS_READINESS_IGNORE_SEVERE_ISSUES", "value": "true"},
          {"name": "AWS_REGION", "value": "ap-south-1"},
          {"name": "AWS_ACCESS_KEY_ID", "valueFrom": {"secretKeyRef": {"name": "aws-s3-creds", "key": "accessKey"}}},
          {"name": "AWS_SECRET_ACCESS_KEY", "valueFrom": {"secretKeyRef": {"name": "aws-s3-creds", "key": "secretKey"}}}
        ]
      }]
    }
  }'
kubectl expose pod polaris --port=8181 --name=polaris
kubectl wait --for=condition=Ready pod/polaris --timeout=120s
```

**Step 2: Bootstrap Polaris catalog**

```bash
kubectl run polaris-init --rm -it --restart=Never --image=alpine/curl -- sh -c '
  apk add --no-cache jq > /dev/null 2>&1

  # Wait for Polaris
  until curl -sf http://polaris:8181/api/catalog/v1/oauth/tokens \
    --user "iceberg-client:iceberg-client-secret" \
    -H "Polaris-Realm: POLARIS" \
    -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL > /dev/null 2>&1; do
    sleep 2
  done

  TOKEN=$(curl -s http://polaris:8181/api/catalog/v1/oauth/tokens \
    --user "iceberg-client:iceberg-client-secret" \
    -H "Polaris-Realm: POLARIS" \
    -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
  echo "Token: ${TOKEN:0:20}..."

  # Create catalog with S3 storage
  curl -s -H "Authorization: Bearer $TOKEN" -H "Polaris-Realm: POLARIS" \
    -H "Content-Type: application/json" \
    http://polaris:8181/api/management/v1/catalogs \
    -d "{\"catalog\":{\"name\":\"ice01\",\"type\":\"INTERNAL\",\"readOnly\":false,
         \"properties\":{\"default-base-location\":\"s3://ayush-k8s-bucket  /warehouse\"},
         \"storageConfigInfo\":{\"storageType\":\"S3\",
           \"roleArn\":\"arn:aws:iam::<YOUR_ACCOUNT>:role/<YOUR_ROLE>\",
           \"allowedLocations\":[\"s3://ayush-k8s-bucket  /\"]}}}"
  echo ""
  echo "Polaris bootstrap complete."
'
```

**Step 3: Install Hive**

```bash
helm install hive ./helm/hive-operator \
  --set cluster.metastore.enabled=false \
  --set cluster.zookeeper.quorum="zookeeper:2181" \
  --set 'cluster.storage.envVars[0].name=HADOOP_OPTIONAL_TOOLS' \
  --set 'cluster.storage.envVars[0].value=hadoop-aws' \
  --set 'cluster.storage.envVars[1].name=AWS_ACCESS_KEY_ID' \
  --set 'cluster.storage.envVars[1].valueFrom.secretKeyRef.name=aws-s3-creds' \
  --set 'cluster.storage.envVars[1].valueFrom.secretKeyRef.key=accessKey' \
  --set 'cluster.storage.envVars[2].name=AWS_SECRET_ACCESS_KEY' \
  --set 'cluster.storage.envVars[2].valueFrom.secretKeyRef.name=aws-s3-creds' \
  --set 'cluster.storage.envVars[2].valueFrom.secretKeyRef.key=secretKey' \
  --set 'cluster.hiveServer2.configOverrides.hive\.metastore\.warehouse\.dir=s3a://<YOUR_BUCKET>/warehouse' \
  --set 'cluster.hiveServer2.configOverrides.metastore\.catalog\.default=ice01' \
  --set 'cluster.hiveServer2.configOverrides.metastore\.client\.impl=org.apache.iceberg.hive.client.HiveRESTCatalogClient' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.uri=http://polaris:8181/api/catalog' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.type=rest' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.warehouse=ice01' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.rest\.auth\.type=oauth2' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.oauth2-server-uri=http://polaris:8181/api/catalog/v1/oauth/tokens' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.credential=iceberg-client:iceberg-client-secret' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.scope=PRINCIPAL_ROLE:ALL' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.scheduled\.queries\.executor\.enabled=false' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.materializedview\.rebuild\.incremental=false' \
  --set 'cluster.hiveServer2.configOverrides.hive\.metastore\.transactional\.event\.listeners=' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.notification\.event\.poll\.interval=0' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.stats\.autogather=false' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.stats\.fetch\.column\.stats=false' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.stats\.estimate=false'
```

**Cleanup:**

```bash
helm uninstall hive
kubectl delete pod polaris
kubectl delete svc polaris
kubectl delete secret aws-s3-creds
```

---

### External Iceberg REST Catalog with Apache Gravitino (Ozone Storage)

[Apache Gravitino](https://gravitino.apache.org/) is an Iceberg REST catalog
that uses an external OAuth2 provider (Keycloak) for authentication. This
setup mirrors the working Docker Compose configuration in
`packaging/src/docker/thirdparties/gravitino/` but adapted for Kubernetes with
Ozone S3 storage.

**Step 1: Deploy Keycloak with the Hive realm**

```bash
# Create Keycloak realm config (defines iceberg-client with service account)
kubectl create configmap keycloak-realm --from-file=realm-export.json=<(cat <<'EOF'
{
  "realm": "hive",
  "enabled": true,
  "clients": [
    {
      "clientId": "iceberg-client",
      "secret": "iceberg-client-secret",
      "enabled": true,
      "redirectUris": ["*"],
      "serviceAccountsEnabled": true,
      "protocol": "openid-connect",
      "publicClient": false,
      "directAccessGrantsEnabled": false,
      "standardFlowEnabled": false,
      "defaultClientScopes": ["catalog"],
      "optionalClientScopes": [],
      "protocolMappers": [
        {
          "name": "audience",
          "protocol": "openid-connect",
          "protocolMapper": "oidc-audience-mapper",
          "consentRequired": false,
          "config": {
            "included.client.audience": "hive-iceberg",
            "id.token.claim": "false",
            "access.token.claim": "true"
          }
        }
      ],
      "attributes": {
        "access.token.lifespan": "3600"
      }
    }
  ],
  "clientScopes": [
    {
      "name": "catalog",
      "protocol": "openid-connect",
      "attributes": {},
      "protocolMappers": []
    }
  ]
}
EOF
)

# Deploy Keycloak with the realm import
kubectl run keycloak --image=quay.io/keycloak/keycloak:25.0.1 --port=8080 \
  --overrides='{
    "spec": {
      "containers": [{
        "name": "keycloak",
        "image": "quay.io/keycloak/keycloak:25.0.1",
        "args": ["start-dev", "--import-realm", "--health-enabled=true"],
        "ports": [{"containerPort": 8080}],
        "env": [
          {"name": "KEYCLOAK_ADMIN", "value": "admin"},
          {"name": "KEYCLOAK_ADMIN_PASSWORD", "value": "admin"}
        ],
        "volumeMounts": [{"name": "realm", "mountPath": "/opt/keycloak/data/import"}]
      }],
      "volumes": [{"name": "realm", "configMap": {"name": "keycloak-realm"}}]
    }
  }'
kubectl expose pod keycloak --port=8080 --name=keycloak
kubectl wait --for=condition=Ready pod/keycloak --timeout=180s
```

**Step 2: Deploy Gravitino**

```bash
# Create Gravitino config (matches Docker thirdparties/gravitino setup, with s3a warehouse)
kubectl create configmap gravitino-conf --from-file=gravitino-iceberg-rest-server.conf=<(cat <<'EOF'
gravitino.iceberg-rest.httpPort = 9001
gravitino.iceberg-rest.catalog-backend = jdbc
gravitino.iceberg-rest.uri = jdbc:h2:file:/tmp/gravitino_h2_db;AUTO_SERVER=TRUE
gravitino.iceberg-rest.jdbc-driver = org.h2.Driver
gravitino.iceberg-rest.jdbc-user = sa
gravitino.iceberg-rest.jdbc-password =
gravitino.iceberg-rest.jdbc-initialize = true
gravitino.iceberg-rest.warehouse = s3a://hive/warehouse
gravitino.authenticators = oauth
gravitino.authenticator.oauth.serverUri = http://keycloak:8080/realms/hive
gravitino.authenticator.oauth.tokenPath = /protocol/openid-connect/token
gravitino.authenticator.oauth.scope = openid catalog
gravitino.authenticator.oauth.clientId = iceberg-client
gravitino.authenticator.oauth.clientSecret = iceberg-client-secret
gravitino.authenticator.oauth.tokenValidatorClass = org.apache.gravitino.server.authentication.JwksTokenValidator
gravitino.authenticator.oauth.jwksUri = http://keycloak:8080/realms/hive/protocol/openid-connect/certs
gravitino.authenticator.oauth.provider = default
gravitino.authenticator.oauth.principalFields = sub
gravitino.authenticator.oauth.allowSkewSecs = 60
gravitino.authenticator.oauth.serviceAudience = hive-iceberg
EOF
)

# Deploy Gravitino Iceberg REST server
kubectl run gravitino --image=apache/gravitino-iceberg-rest:1.0.0 --port=9001 \
  --overrides='{
    "spec": {
      "containers": [{
        "name": "gravitino",
        "image": "apache/gravitino-iceberg-rest:1.0.0",
        "command": ["/bin/bash", "-c"],
        "args": ["cp /tmp/gravitino-conf/gravitino-iceberg-rest-server.conf /root/gravitino-iceberg-rest-server/conf/gravitino-iceberg-rest-server.conf && mkdir -p /root/gravitino-iceberg-rest-server/libs && curl -sL -o /root/gravitino-iceberg-rest-server/libs/h2-2.2.220.jar https://repo1.maven.org/maven2/com/h2database/h2/2.2.220/h2-2.2.220.jar && /bin/bash /root/gravitino-iceberg-rest-server/bin/iceberg-rest-server.sh start && tail -f /dev/null"],
        "ports": [{"containerPort": 9001}],
        "volumeMounts": [{"name": "conf", "mountPath": "/tmp/gravitino-conf"}]
      }],
      "volumes": [{"name": "conf", "configMap": {"name": "gravitino-conf"}}]
    }
  }'
kubectl expose pod gravitino --port=9001 --name=gravitino
kubectl wait --for=condition=Ready pod/gravitino --timeout=120s
```

**Step 3: Install Hive with Gravitino as external catalog**

**CLI:**

```bash
helm install hive ./helm/hive-operator \
  --set cluster.metastore.enabled=false \
  --set cluster.zookeeper.quorum="zookeeper:2181" \
  --set cluster.storage.coreSiteOverrides."fs\.defaultFS"="s3a://hive" \
  --set cluster.storage.coreSiteOverrides."fs\.s3a\.endpoint"="http://ozone-s3g-rest:9878" \
  --set-string cluster.storage.coreSiteOverrides."fs\.s3a\.path\.style\.access"=true \
  --set 'cluster.storage.envVars[0].name=HADOOP_OPTIONAL_TOOLS' \
  --set 'cluster.storage.envVars[0].value=hadoop-aws' \
  --set 'cluster.storage.envVars[1].name=AWS_ACCESS_KEY_ID' \
  --set 'cluster.storage.envVars[1].value=ozone' \
  --set 'cluster.storage.envVars[2].name=AWS_SECRET_ACCESS_KEY' \
  --set 'cluster.storage.envVars[2].value=ozone' \
  --set 'cluster.hiveServer2.configOverrides.metastore\.catalog\.default=ice01' \
  --set 'cluster.hiveServer2.configOverrides.metastore\.client\.impl=org.apache.iceberg.hive.client.HiveRESTCatalogClient' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.uri=http://gravitino:9001/iceberg' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.type=rest' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.rest\.auth\.type=oauth2' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.oauth2-server-uri=http://keycloak:8080/realms/hive/protocol/openid-connect/token' \
  --set 'cluster.hiveServer2.configOverrides.iceberg\.catalog\.ice01\.credential=iceberg-client:iceberg-client-secret' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.scheduled\.queries\.executor\.enabled=false' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.materializedview\.rebuild\.incremental=false' \
  --set 'cluster.hiveServer2.configOverrides.hive\.metastore\.transactional\.event\.listeners=' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.notification\.event\.poll\.interval=0' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.stats\.autogather=false' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.stats\.fetch\.column\.stats=false' \
  --set-string 'cluster.hiveServer2.configOverrides.hive\.stats\.estimate=false'
```

**Values file:**

```yaml
# values-gravitino.yaml
cluster:
  metastore:
    enabled: false

  zookeeper:
    quorum: "zookeeper:2181"

  storage:
    coreSiteOverrides:
      fs.defaultFS: "s3a://hive"
      fs.s3a.endpoint: "http://ozone-s3g-rest:9878"
      fs.s3a.path.style.access: "true"
    envVars:
      - name: HADOOP_OPTIONAL_TOOLS
        value: "hadoop-aws"
      - name: AWS_ACCESS_KEY_ID
        value: "ozone"
      - name: AWS_SECRET_ACCESS_KEY
        value: "ozone"

  hiveServer2:
    configOverrides:
      # Iceberg REST catalog connection
      metastore.catalog.default: "ice01"
      metastore.client.impl: "org.apache.iceberg.hive.client.HiveRESTCatalogClient"
      iceberg.catalog.ice01.uri: "http://gravitino:9001/iceberg"
      iceberg.catalog.ice01.type: "rest"
      iceberg.catalog.ice01.rest.auth.type: "oauth2"
      iceberg.catalog.ice01.oauth2-server-uri: "http://keycloak:8080/realms/hive/protocol/openid-connect/token"
      iceberg.catalog.ice01.credential: "iceberg-client:iceberg-client-secret"
      # Disable HMS-dependent features (not available with REST catalog)
      hive.scheduled.queries.executor.enabled: "false"
      hive.materializedview.rebuild.incremental: "false"
      hive.metastore.transactional.event.listeners: ""
      hive.notification.event.poll.interval: "0"
      hive.stats.autogather: "false"
      hive.stats.fetch.column.stats: "false"
      hive.stats.estimate: "false"
```

```bash
helm install hive ./helm/hive-operator -f values-gravitino.yaml
```

**Test the connection:**

```bash
kubectl exec -it deployment/hive-hiveserver2 -- beeline -u "jdbc:hive2://localhost:10000/" \
  -e "CREATE TABLE test (id INT, name STRING); INSERT INTO test VALUES (1, 'hello'); SELECT * FROM test;"
```

**Cleanup:**

```bash
helm uninstall hive
kubectl delete pod gravitino keycloak
kubectl delete svc gravitino keycloak
kubectl delete configmap keycloak-realm gravitino-conf
```

---

### Custom Replicas and Resources

**Values file:**

```yaml
# values.yaml
cluster:
  # ... database, zookeeper, storage as above ...

  metastore:
    replicas: 3
    resources:
      requestsMemory: "1Gi"
      limitsMemory: "2Gi"

  hiveServer2:
    replicas: 4
    serviceType: LoadBalancer
    resources:
      requestsCpu: "1"
      requestsMemory: "2Gi"
      limitsMemory: "4Gi"

  llap:
    enabled: true
    replicas: 3
    executors: 2
    memoryMb: 4096
    resources:
      requestsMemory: "4Gi"
      limitsMemory: "6Gi"

  tezAm:
    replicas: 3
    scratchStorageSize: "5Gi"
```

```bash
helm install hive ./helm/hive-operator -f values.yaml
```

---

## Verify

```bash
kubectl get pods -w
kubectl get hiveclusters
kubectl describe hivecluster hive
```

## Connect to HiveServer2

```bash
kubectl exec -it deployment/hive-hiveserver2 -- beeline -u "jdbc:hive2://hive-hiveserver2:10000/"
```

Or via port-forward:

```bash
kubectl port-forward svc/hive-hiveserver2 10000:10000
beeline -u "jdbc:hive2://localhost:10000/"
```

---

## Helm Values Reference

### Operator

| Value | Default | Description |
|-------|---------|-------------|
| `operator.image.repository` | `apache/hive` | Operator image repository |
| `operator.image.tag` | `operator-4.3.0-SNAPSHOT` | Operator image tag |
| `operator.image.pullPolicy` | `IfNotPresent` | Image pull policy |
| `operator.resources` | `{requests: {cpu: 200m, memory: 256Mi}, limits: {memory: 512Mi}}` | Operator pod resources |

### Cluster (HiveCluster CR)

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.enabled` | `true` | Create a HiveCluster CR (set `false` to install only the operator) |
| `cluster.name` | `hive` | HiveCluster resource name |
| `cluster.image` | `apache/hive:4.3.0-SNAPSHOT` | Hive component image |

### Database (Required)

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.database.type` | `postgres` | DB type: `postgres`, `mysql`, `derby` |
| `cluster.database.url` | | JDBC URL |
| `cluster.database.driver` | | JDBC driver class |
| `cluster.database.username` | | DB username |
| `cluster.database.passwordSecretRef.name` | | K8s Secret name |
| `cluster.database.passwordSecretRef.key` | | Key in the Secret (e.g. `password`) |
| `cluster.database.driverJarUrl` | | URL to download JDBC driver |

### ZooKeeper (Required)

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.zookeeper.quorum` | | ZooKeeper connection string (e.g. `zookeeper:2181`) |

### Storage (Required)

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.storage.coreSiteOverrides` | `{}` | `core-site.xml` properties (`fs.defaultFS`, `fs.s3a.*`, etc.) |
| `cluster.storage.envVars` | `[]` | Env vars for all pods (credentials, `HADOOP_OPTIONAL_TOOLS`) |
| `cluster.storage.externalJars` | `[]` | Connector JAR URLs downloaded at startup |
| `cluster.storage.volumes` | `[]` | Volumes for all pods (credential files) |
| `cluster.storage.volumeMounts` | `[]` | Volume mounts for all containers |

### Metastore

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.metastore.enabled` | `true` | Deploy a managed Metastore |
| `cluster.metastore.externalUri` | | Thrift URI when `enabled: false` |
| `cluster.metastore.replicas` | `2` | Replica count |
| `cluster.metastore.warehouseDir` | `/hive/warehouse` | Warehouse directory |
| `cluster.metastore.resources` | `{}` | CPU/memory |
| `cluster.metastore.configOverrides` | `{}` | Extra `metastore-site.xml` properties |

### HiveServer2

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.hiveServer2.replicas` | `2` | Replica count |
| `cluster.hiveServer2.serviceType` | `ClusterIP` | K8s Service type |
| `cluster.hiveServer2.thriftPort` | `10000` | Thrift port |
| `cluster.hiveServer2.webUiPort` | `10002` | Web UI port |
| `cluster.hiveServer2.resources` | `{}` | CPU/memory |
| `cluster.hiveServer2.configOverrides` | `{}` | Extra `hive-site.xml` properties |
| `cluster.hiveServer2.externalJars` | `[]` | HS2-specific JARs |

### LLAP

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.llap.enabled` | `true` | Enable LLAP daemons |
| `cluster.llap.replicas` | `2` | Replica count |
| `cluster.llap.executors` | `1` | Executors per daemon |
| `cluster.llap.memoryMb` | `1024` | Memory per daemon (MB) |
| `cluster.llap.serviceHosts` | `@llap0` | LLAP ZK identity |
| `cluster.llap.resources` | `{}` | CPU/memory |

### Tez AM

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.tezAm.enabled` | `true` | Enable Tez Application Master |
| `cluster.tezAm.replicas` | `2` | Replica count |
| `cluster.tezAm.scratchStorageSize` | `1Gi` | Shared scratch PVC size |
| `cluster.tezAm.scratchStorageClassName` | | StorageClass (must support RWX) |
| `cluster.tezAm.resources` | `{}` | CPU/memory |

---

## Upgrade and Uninstall

### Upgrade (values only, no CRD changes)

```bash
helm upgrade hive ./helm/hive-operator -f my-values.yaml
```

### Upgrade (with CRD schema changes)

Helm does **not** update CRDs on `helm upgrade`. If the operator version
includes CRD changes (new status fields, new spec fields), you must
re-apply the CRD manually:

```bash
kubectl apply -f helm/hive-operator/crds/hiveclusters.hive.apache.org-v1.yml
helm upgrade hive ./helm/hive-operator -f my-values.yaml
```

### Full Uninstall and Reinstall (clean slate)

```bash
# Uninstall (removes operator + HiveCluster CR + all managed pods)
helm uninstall hive

# IMPORTANT: Always delete the CRD before reinstalling to ensure
# the updated schema is applied. Helm only creates CRDs on install,
# it never updates existing ones.
kubectl delete crd hiveclusters.hive.apache.org

# Reinstall
helm install hive ./helm/hive-operator -f my-values.yaml
```

### Remove Everything (including dependencies)

```bash
helm uninstall hive
kubectl delete crd hiveclusters.hive.apache.org
helm uninstall ozone postgres zookeeper --ignore-not-found
kubectl delete pvc data-zookeeper-0 --ignore-not-found
kubectl delete pvc data-postgres-postgresql-0 --ignore-not-found
kubectl delete secret hive-db-secret --ignore-not-found
```

---

## Advanced: Deploy via Operator Only (without Helm)

If you prefer raw manifests over Helm, you can deploy the operator and create
HiveCluster CRs manually. This example uses Ozone as the storage backend.

### 1. Install the CRD

```bash
kubectl apply -f helm/hive-operator/crds/hiveclusters.hive.apache.org-v1.yml
```

### 2. Deploy RBAC and the Operator

```bash
kubectl create namespace hive-operator
kubectl apply -f config/rbac/
export HIVE_VERSION=4.3.0-SNAPSHOT
envsubst < config/operator/deployment.yaml | kubectl apply -f -
```

### 3. Deploy Ozone

```bash
helm repo add ozone https://apache.github.io/ozone-helm-charts/
helm install ozone ozone/ozone --version 0.2.0 --wait
sleep 50
kubectl exec statefulset/ozone-om -- ozone sh volume create /s3v
kubectl exec statefulset/ozone-om -- ozone sh bucket create /s3v/hive
```

### 4. Create a HiveCluster CR

Full-HA (Metastore x2, HS2 x2, LLAP x2, TezAM x2):

```bash
envsubst < config/samples/hivecluster-full-ha.yaml | kubectl apply -f -
```

Or minimal (Metastore x1, HS2 x1, no LLAP/TezAM):

```bash
envsubst < config/samples/hivecluster-minimal.yaml | kubectl apply -f -
```

### 5. Cleanup

```bash
kubectl delete hivecluster hive
envsubst < config/operator/deployment.yaml | kubectl delete -f -
kubectl delete -f config/rbac/
kubectl delete namespace hive-operator
# Always delete CRD to ensure a clean reinstall picks up schema changes
kubectl delete crd hiveclusters.hive.apache.org
kubectl delete pvc data-zookeeper-0 --ignore-not-found
kubectl delete pvc data-postgres-postgresql-0 --ignore-not-found
kubectl delete secret hive-db-secret --ignore-not-found
helm uninstall ozone postgres zookeeper --ignore-not-found
```

---

## Architecture

```
HiveCluster CR
  |
  v
HiveClusterReconciler
  |
  +-- HadoopConfigMapDependent          (core-site.xml)
  +-- MetastoreConfigMapDependent       (metastore-site.xml)
  +-- HiveServer2ConfigMapDependent     (hive-site.xml + tez-site.xml)
  +-- SchemaInitJobDependent            (schematool -initOrUpgradeSchema)
  +-- MetastoreDeploymentDependent      --> MetastoreServiceDependent
  +-- HiveServer2DeploymentDependent    --> HiveServer2ServiceDependent
  +-- LlapStatefulSetDependent          --> LlapServiceDependent          (optional)
  +-- ScratchPvcDependent               (shared scratch PVC, optional)
  +-- TezAmStatefulSetDependent         --> TezAmServiceDependent         (optional)
```

**Startup order:**
1. ConfigMaps (Hadoop, Metastore, HiveServer2)
2. Schema Init Job [if Metastore enabled]
3. Metastore Deployment + Service [if enabled]
4. HiveServer2 Deployment + Service
5. LLAP + TezAM [if enabled]
