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
LLAP x2, TezAM x2 — one TezAM per LLAP cluster). You only need to provide three
things: database, ZooKeeper, and storage.

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
  --set cluster.llapClusters=[] \
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
  llapClusters: []
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

  llapClusters:
  - name: llap0
    enabled: true
    replicas: 3
    executors: 2
    memoryMb: 4096
    resources:
      requestsMemory: "4Gi"
      limitsMemory: "6Gi"

  tezAm:
    enabled: true
    scratchStorageSize: "5Gi"
```

```bash
helm install hive ./helm/hive-operator -f values.yaml
```

---

## Multi-Tenant LLAP

Multi-tenant LLAP allows you to run multiple independent LLAP clusters within a single
HiveCluster, each with its own resource pool, autoscaling policy, and TezAM instance.
HS2 routes sessions to clusters server-side based on admin-defined user/group rules.

### How It Works

```
                        Multi-Tenant LLAP Architecture

  beeline -n alice
    |
    v
  HiveServer2 (resolves user→cluster via routing rules)
    |
    +-- alice (user:alice=llap0) --> TezAM-llap0 --> LLAP daemon llap0-0, llap0-1, ...
    +-- bob   (user:bob=llap1)   --> TezAM-llap1 --> LLAP daemon llap1-0, llap1-1, ...
    +-- carol (default=llap2)    --> TezAM-llap2 --> LLAP daemon llap2-0, llap2-1, ...
```

Each LLAP cluster is fully isolated:
- **Separate LLAP daemon StatefulSet** with independent executor count, memory, and replicas
- **Separate TezAM Deployment** (one per LLAP cluster) with its own ZooKeeper registration
- **Separate autoscaling** — each cluster scales independently based on its own metrics
- **Shared scratch PVC** (ReadWriteMany) for HS2 ↔ TezAM coordination files

### Configuration

**Values file:**

```yaml
# values-multi-tenant.yaml
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

  hiveServer2:
    replicas: 2

  tezAm:
    enabled: true
    scratchStorageSize: "2Gi"

  # Server-side routing: map users/groups to LLAP clusters
  llapClusterRouting: "user:alice=production,group:eng=analytics,default=dev"

  # Three LLAP clusters: production (large), analytics (medium), dev (small)
  llapClusters:
  - name: production
    enabled: true
    replicas: 6
    executors: 4
    memoryMb: 8192
    resources:
      requestsMemory: "8Gi"
      limitsMemory: "10Gi"
    autoscaling:
      enabled: true
      minReplicas: 2
      scaleUpThreshold: 2

  - name: analytics
    enabled: true
    replicas: 4
    executors: 2
    memoryMb: 4096
    resources:
      requestsMemory: "4Gi"
      limitsMemory: "6Gi"
    autoscaling:
      enabled: true
      minReplicas: 0        # scales to zero when idle

  - name: dev
    enabled: true
    replicas: 2
    executors: 1
    memoryMb: 1024
    resources:
      requestsMemory: "1Gi"
      limitsMemory: "2Gi"
    autoscaling:
      enabled: true
      minReplicas: 0        # scales to zero when idle
```

```bash
helm install hive ./helm/hive-operator -f values-multi-tenant.yaml
```

### Resulting Kubernetes Resources

For the above configuration, the operator creates:

| Resource | Name | Purpose |
|----------|------|---------|
| StatefulSet | `hive-production` | LLAP daemons for production cluster |
| Deployment | `hive-tezam-production` | TezAM for production cluster |
| StatefulSet | `hive-analytics` | LLAP daemons for analytics cluster |
| Deployment | `hive-tezam-analytics` | TezAM for analytics cluster |
| StatefulSet | `hive-dev` | LLAP daemons for dev cluster |
| Deployment | `hive-tezam-dev` | TezAM for dev cluster |
| Service (headless) | `hive-production`, `hive-analytics`, `hive-dev` | LLAP daemon discovery |
| Service (headless) | `hive-tezam-production`, `hive-tezam-analytics`, `hive-tezam-dev` | TezAM discovery |
| ConfigMap | `hive-production-config`, etc. | `llap-daemon-site.xml` per cluster |
| ConfigMap | `hive-tezam-production-config`, etc. | `tez-site.xml` per cluster |
| PVC | `hive-scratch` | Shared scratch (ReadWriteMany) for HS2 ↔ TezAM |

### Routing Queries to a Specific LLAP Cluster

Routing is server-side — administrators define rules in the CR that map users/groups to
LLAP clusters. HS2 resolves the target cluster at session open and sets all required
properties automatically. Clients do not need any cluster-specific configuration.

```yaml
spec:
  llapClusterRouting: "user:alice=production,group:eng=analytics,default=dev"
  llapClusters:
    - name: production
    - name: analytics
    - name: dev
```

Clients just connect with their identity:

```bash
beeline -u "jdbc:hive2://hive-hiveserver2:10001/;transportMode=http;httpPath=cliservice" -n alice
```

HS2 resolves `alice → production` and sets the following on the session:
- `hive.server2.tez.external.sessions.namespace = /tez-external-sessions/production`
- `tez.am.registry.namespace = /production`
- `hive.llap.daemon.service.hosts = @production`

Priority: user match > group match > default.

The operator auto-generates per-cluster definitions from the LLAP spec names:
- `hive.llap.cluster.<name>.sessions.namespace = /tez-external-sessions/<name>`
- `hive.llap.cluster.<name>.registry.namespace = /<name>`
- `hive.llap.cluster.<name>.service.hosts = @<name>`

### ZooKeeper Registration

Each LLAP cluster registers independently in ZooKeeper:

| Cluster | LLAP daemons register at | TezAM registers session at |
|---------|--------------------------|----------------------------|
| `production` | `@production` (ZK service record) | `/tez-external-sessions/production/<appId>` |
| `analytics` | `@analytics` (ZK service record) | `/tez-external-sessions/analytics/<appId>` |
| `dev` | `@dev` (ZK service record) | `/tez-external-sessions/dev/<appId>` |

HS2 discovers available TezAM sessions via `hive.server2.tez.external.sessions.namespace` and
uses `tez.am.registry.namespace` for client cache isolation.

### Per-Cluster Autoscaling Isolation

When autoscaling is enabled, metrics are fully isolated per LLAP cluster:

- **LLAP executor metrics**: The operator selects pods by label `hive.apache.org/llap-cluster=<name>`.
  Only that cluster's pods are scraped and included in the scaling formula.
- **HS2 activation gate**: The operator reads `hs2_llap_target_sessions_<name>` from HS2 pods.
  Each cluster only wakes when sessions specifically target it.
- **TezAM scaling**: Each TezAM scales based on session demand for its paired LLAP cluster.

This means scaling up `production` never affects `analytics` or `dev` replicas.

### Adding/Removing LLAP Clusters

To add a new cluster, append to `llapClusters[]` and run `helm upgrade`:

```bash
helm upgrade hive ./helm/hive-operator -f values-multi-tenant.yaml
```

To remove a cluster, delete the entry from `llapClusters[]` and upgrade. The operator
automatically garbage-collects the removed cluster's StatefulSet, Service, ConfigMap,
and PDB via label-based discovery.

---

## Verify

```bash
kubectl get pods -w
kubectl get hiveclusters
kubectl describe hivecluster hive
```

---

## Autoscaling

The operator supports metric-based autoscaling for all four Hive components using
an **operator-driven control loop** that scrapes JMX Exporter metrics directly from
pods. No Prometheus server or external autoscaling tools are needed. Autoscaling is
opt-in per component and designed for **zero query failures** during scale-down.

### Prerequisites

- No external dependencies — the operator handles all scaling decisions internally

### How It Works

When `autoscaling.enabled: true` is set for a component, the operator:
1. Attaches the JMX Exporter javaagent (port 9404) to each pod
2. Polls `/metrics` on each pod at `metricsScrapeIntervalSeconds` intervals
3. Computes desired replicas using component-specific formulas
4. Applies HPA-like stabilization windows (scale-up/scale-down)
5. Patches the workload `spec.replicas` directly

### Graceful Scale-Down Architecture

```
                        Scale Down Flow                                
 1. Operator reduces desired replicas (metric below threshold,      
    stabilization window elapsed)                  
 2. PodDisruptionBudget ensures minAvailable=1 (at least one pod    
    always running)                                                 
 3. Kubernetes sends SIGTERM to selected pod                        
 4. preStop hook runs:                                              
    - HS2: deregisters from ZK, drains open sessions, kills JVM    
    - HMS: kills JVM (stateless HTTP — no drain needed)             
    - LLAP: waits until all executors become idle, kills JVM        
    - TezAM: no drain (DAGAppMaster does not expose JMX metrics)            
 5. terminationGracePeriodSeconds = gracePeriodSeconds (safety cap) 
 6. Pod terminates immediately once drain completes (does NOT wait  
    the full grace period — it's only the upper safety bound)
```

> **Note:** Shell entrypoints (PID 1) in containers don't forward SIGTERM to child
> processes. The preStop hook explicitly sends SIGTERM to the Hive/Tez Java process
> after drain completes, ensuring prompt shutdown without waiting for the grace period
> to expire.

### Scaling Timers

The autoscaling system uses three independent timing controls:

| Timer | Config Field | Default | Purpose |
|-------|-------------|---------|---------|
| **Metrics scrape interval** | `metricsScrapeIntervalSeconds` | `10` | How often the operator scrapes JMX Exporter `/metrics` on each pod. This is the **biggest bottleneck** for autoscaling reaction time. |
| **Scale-up stabilization** | `scaleUpStabilizationSeconds` | `60` | Window: picks the highest recommendation within this period before scaling up. Prevents flapping when metrics oscillate. Set to `0` for LLAP and TezAM (reactive dependents). |
| **Scale-down stabilization** | `scaleDownStabilizationSeconds` | `300-900` | Window: picks the most conservative (highest) recommendation within this period before scaling down. Also acts as the cooldown between consecutive scale-downs — no separate cooldown needed. |

**How they interact:**
- Load spike detected → operator scrapes metrics within `metricsScrapeIntervalSeconds` → waits `scaleUpStabilizationSeconds` then scales up
- Load drops → operator waits `scaleDownStabilizationSeconds` (stabilization window must confirm low demand consistently) then scales down

**Tuning reaction time:** With defaults (`metricsScrapeIntervalSeconds: 10`, `scaleUpStabilizationSeconds: 0` for LLAP/TezAM), scale-up latency is ~10-20s (one scrape cycle). For HS2 with `scaleUpStabilizationSeconds: 60`, expect ~70s.

### Per-Component Scaling Logic

| Component | Scale-Up Formula | Scale-Down | JMX Metric |
|-----------|-----------------|------------|------------|
| **HiveServer2** | `max(ceil(sessions / threshold), cpu_desired)` | Sessions drop to 0 AND CPU below threshold → scale to minReplicas | `hs2_open_sessions`, `jvm_process_cpu_load` |
| **Metastore** | `max(ceil(api_rate / threshold), cpu_desired)` | Rate drops to 0 AND CPU below threshold → scale to minReplicas | `api_*_total`, `jvm_process_cpu_load` |
| **LLAP** | `ceil(avg(queued + configured - available) / scaleUpThreshold)` | All executors idle + no HS2 sessions | `hadoop_llapdaemon_executor*` |
| **Tez AM** | `max(sum(hs2_open_sessions), count(HS2_pods) * sessions_per_queue)` | All HS2 sessions closed | `hs2_open_sessions` (from HS2 pods) |

**TezAM Scaling Model:** TezAM uses demand-driven scaling with two formulas (max wins):
1. **Session demand** — `sum(hs2_open_sessions)`: scales to match the total number of
   concurrent sessions across all HS2 pods (each session needs its own exclusive TezAM).
2. **Pre-warm** — `count(HS2 pods with sessions) × hive.server2.tez.sessions.per.default.queue` (default 1):
   ensures every active HS2 pod has enough TezAM sessions pre-claimed from ZooKeeper.

The operator takes the maximum across both formulas. This ensures TezAM capacity
is always sufficient for both current demand and eager session pre-warming.
TezAM scaling is purely demand-driven from HS2 metrics.

### Scale-to-Zero Architecture

When `minReplicas: 0` is configured (LLAP, TezAM), the cluster scales those
components down to zero pods when HS2 has no active sessions. HS2 itself always
maintains at least 1 replica (`minReplicas >= 1`) so it is always available to
accept connections.

```
                   Scale-to-Zero (Idle Detection)                    

  1. HS2 reports hs2_open_sessions = 0 for scaleDownStabilization     
     → operator scales HS2 to minReplicas (>= 1)                    
                                                                     
  2. Operator sees hs2_open_sessions = 0 on next LLAP/TezAM eval     
     → activation gate fails                                         
     → scale LLAP and TezAM to 0 (if minReplicas=0)                 
                                                                     
  3. HMS stays at minReplicas=1 (always available)                   

```

```
                   Wake-from-Zero (LLAP/TezAM)                       

  1. Beeline connects to HS2 (always running, at least 1 pod)        
                                                                     
  2. HS2 reports hs2_open_sessions > 0 via JMX Exporter              
                                                                     
  3. Operator detects HS2 sessions on next scrape cycle:             
     - LLAP activation gate passes → scales up from 0                
     - TezAM activation gate passes → scales up from 0              
                                                                     
  4. Query executes once LLAP/TezAM pods are ready                   

```

**Session protection:** The HS2 Service uses `sessionAffinity: ClientIP` to ensure
beeline clients always reach the same pod. The preStop hook deregisters the pod from
ZooKeeper (preventing new sessions) and waits for `hs2_open_sessions` to drain to 0
before terminating. The `gracePeriodSeconds` (default 3600s) is a safety cap — the pod
terminates immediately once sessions drain, not after the full grace period.

**Component-specific behavior:**

| Component | minReplicas | Scale-to-Zero Trigger | Wake Trigger |
|-----------|-------------|----------------------|--------------|
| **HS2** | 1 | N/A (always running) | N/A |
| **HMS** | 1 | Never (always running) | N/A |
| **LLAP** | 0 | No HS2 sessions targeting this cluster | HS2 has sessions targeting this cluster (`hs2_llap_target_sessions_{name}`) |
| **TezAM** | 0 | No HS2 sessions (activation gate fails) | HS2 has open sessions (next scrape) |

**Per-cluster LLAP wake:** When multiple LLAP clusters are configured (e.g., `llap0`, `llap1`),
each cluster wakes independently based on the `hs2_llap_target_sessions_{name}` metric.
If HS2 does not expose per-target metrics (older builds), the operator falls back to the generic
`hs2_open_sessions` metric (which wakes all LLAP clusters on any session).

### Auto-Suspend (Full Cluster Hibernation)

Auto-suspend goes beyond scale-to-zero — it fully hibernates the **entire** cluster
(including HS2 and HMS) to 0 replicas after a configurable idle timeout. This is
useful for dev/test clusters that should not consume resources when nobody is using
them.

**Prerequisites:** Auto-suspend requires autoscaling to be enabled on ALL active
components (HS2, LLAP if enabled, TezAM if enabled, and HMS if `includeMetastore=true`).
The operator will not auto-suspend unless it can confirm all components are at their
minimum state.

**Idle criteria (all must hold simultaneously for `idleTimeoutMinutes`):**

| Component | Idle Condition |
|-----------|---------------|
| **HS2** | At `minReplicas` with 0 open sessions |
| **HMS** | At `minReplicas` (only checked if `includeMetastore=true`) |
| **LLAP** | At `minReplicas` (default 0) |
| **TezAM** | At `minReplicas` (default 0) |

**Important:** HS2 can **only** scale to 0 replicas via auto-suspend. Normal
autoscaling always maintains `minReplicas >= 1` for HS2. Auto-suspend is the
only mechanism that overrides this to achieve full hibernation.

```
                    Auto-Suspend Flow

  1. Autoscaling scales all components to their minReplicas
     (HS2≥1, HMS≥1, LLAP/TezAM to configured min)

  2. Operator detects idle state:
     - HS2 has 0 open sessions
     - HMS at minReplicas (if includeMetastore=true)
     - LLAP/TezAM at minReplicas

  3. Idle timer starts (status: clusterPhase=Idle, idleSince=<now>)

  4. After idleTimeoutMinutes (default 15):
     - ALL components scaled to 0 (HMS excluded if includeMetastore=false)
     - spec.suspend set to true (cluster stays suspended until user wakes it)
     - Status: clusterPhase=Suspended, suspendedSince=<now>

  5. To wake: kubectl patch hivecluster hive --type=merge -p '{"spec":{"suspend":false}}'
     All components restored to minReplicas
     (HS2/HMS ≥1, LLAP/TezAM ≥1 for immediate usability)

```

**Configuration:**

```yaml
cluster:
  autoSuspend:
    enabled: true
    idleTimeoutMinutes: 15    # minutes idle before full hibernation
    includeMetastore: true    # set false to keep HMS running during suspend
```

**Manual Suspend/Wake Commands:**

```bash
# Suspend immediately (bypasses idle timer)
kubectl patch hivecluster hive --type=merge -p '{"spec":{"suspend":true}}'

# Wake cluster (restores to minReplicas)
kubectl patch hivecluster hive --type=merge -p '{"spec":{"suspend":false}}'
```

Manual suspend works regardless of whether `autoSuspend.enabled` is true — it
immediately scales all components to 0 without waiting for the idle timeout.
When `includeMetastore: false`, HMS stays running even during manual suspend.

**Observing cluster state:**

```bash
# Quick view — printer columns show phase and idle time
kubectl get hivecluster
```
```
NAME   PHASE   IDLE (MIN)   AGE
hive   Idle    12           2h
```

```bash
# After suspend triggers
kubectl get hivecluster
```
```
NAME   PHASE       IDLE (MIN)   AGE
hive   Suspended                2h
```

```bash
# Full status (kubectl get hivecluster hive -o yaml)
```
```yaml
status:
  clusterPhase: Suspended
  idleSince: "2026-06-08T10:00:00Z"
  idleForMinutes: 15
  suspendedSince: "2026-06-08T10:15:00Z"
  conditions:
    - type: Suspended
      status: "True"
      reason: AutoSuspend        # or ManualSuspend
      message: "Cluster suspended after idle timeout"
      lastTransitionTime: "2026-06-08T10:15:00Z"
```

When the cluster is running normally:
```
NAME   PHASE     IDLE (MIN)   AGE
hive   Running                2h
```

**Full example (autoscaling + auto-suspend):**

```yaml
cluster:
  autoSuspend:
    enabled: true
    idleTimeoutMinutes: 15
    includeMetastore: false   # keep HMS running during suspend

  hiveServer2:
    replicas: 10
    autoscaling:
      enabled: true
      minReplicas: 1

  metastore:
    replicas: 6
    autoscaling:
      enabled: true
      minReplicas: 1

  llapClusters:
  - name: llap0
    replicas: 8
    autoscaling:
      enabled: true
      minReplicas: 0        # scales to 0 via normal autoscaling when no sessions target this cluster

  tezAm:
    replicas: 10
    autoscaling:
      enabled: true
      minReplicas: 0        # scales to 0 via normal autoscaling when HS2 idle
```

With this configuration, the cluster lifecycle is:
1. Under load → all components scaled up by autoscaler
2. Load drops → autoscaler scales to minReplicas (HS2=1, HMS=1, LLAP clusters=0, TezAM=0)
3. HS2 idle (0 sessions) for 15 minutes → auto-suspend kicks in → HS2, LLAP, TezAM to 0 (HMS stays at minReplicas)
4. `kubectl patch hivecluster hive --type=merge -p '{"spec":{"suspend":false}}'` → wake → HS2=1, each LLAP cluster=1, TezAM=1
5. User connects → autoscaler detects sessions → scales up as needed

### CPU-Based Scaling (HS2 and HMS)

In addition to the primary metrics (sessions for HS2, API request rate for HMS),
the operator supports a secondary **CPU-based scaling signal** for HiveServer2 and
Metastore. The final desired replica count is:

```
final_desired = max(metric_desired, cpu_desired)
```

Either signal can trigger scale-up; neither can force scale-down below what the
other recommends. CPU-based scaling uses the same stabilization windows as metric-based
scaling (no separate CPU stabilization).

**How it works:**

1. The operator scrapes `ProcessCpuLoad` from `java.lang:type=OperatingSystem` via JMX
   Exporter (exported as `jvm_process_cpu_load`, a 0.0–1.0 fraction)
2. Averages across all pods, converts to percentage (0–100)
3. If avg CPU >= `cpuScaleUpThreshold`: scales up proportionally
   (`ceil(avgCpu * currentReplicas / cpuScaleUpThreshold)`)
4. If avg CPU < `cpuScaleDownThreshold`: scales down
   (`ceil(avgCpu * currentReplicas / cpuScaleUpThreshold)`, floored at `minReplicas`)
5. Between thresholds: holds current replica count

**Configuration:**

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.<component>.autoscaling.cpuScaleUpThreshold` | `90` | CPU percentage (0-100) that triggers scale-up. Set to `0` to disable CPU-based scaling. |
| `cluster.<component>.autoscaling.cpuScaleDownThreshold` | `30` | CPU percentage (0-100) below which scale-down is considered. |

**Example:**

```yaml
cluster:
  hiveServer2:
    replicas: 10
    resources:
      limitsCpu: "2"        # Recommended: set CPU limits so ProcessCpuLoad is relative to pod allocation
    autoscaling:
      enabled: true
      cpuScaleUpThreshold: 90
      cpuScaleDownThreshold: 30

  metastore:
    replicas: 6
    resources:
      limitsCpu: "2"
    autoscaling:
      enabled: true
      cpuScaleUpThreshold: 90
      cpuScaleDownThreshold: 30
```

**Important: CPU limits and metric accuracy**

`ProcessCpuLoad` reports CPU usage as a fraction of **available processors**. Without
CPU limits, the JVM sees all node cores (e.g., 8 cores), so even heavy single-pod
load only shows ~12.5%. With `limitsCpu: "2"`, the JVM sees 2 processors and the
metric becomes "% of allocated CPU" — making thresholds meaningful.

| Pod CPU Limit | JVM sees | 90% threshold means |
|---------------|----------|---------------------|
| None (no limit) | All node cores (e.g., 8) | Using 7.2 of 8 cores — very hard to reach |
| `2` | 2 cores | Using 1.8 of 2 allocated cores |
| `4` | 4 cores | Using 3.6 of 4 allocated cores |

**Recommendation:** Always set `resources.limitsCpu` when using CPU-based autoscaling.

**Status output:**

The operator reports CPU metrics in the HiveCluster status:

```yaml
status:
  hiveServer2:
    autoscaling:
      currentMetricValue: 5           # total sessions
      scaleUpThreshold: 100
      currentCpuPercent: 72.45        # avg ProcessCpuLoad * 100
      cpuScaleUpThreshold: 90
      cpuProposedReplicas: 2          # what CPU alone would recommend
      proposedReplicas: 2
      lastScaleTime: "2026-05-31T04:23:07Z"
```

**Applicability:** CPU-based scaling only applies to HS2 and HMS. LLAP and TezAM
do not use CPU as a scaling signal (LLAP scales on busy executor slots which already
correlates with CPU; TezAM is demand-based from HS2 session count).

---

### Enabling Autoscaling

**CLI (with Ozone storage backend):**

Each component has sensible per-component defaults (see [Configuration Reference](#configuration-reference)).
Only `enabled=true` is needed to turn on autoscaling:

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
  --set cluster.hiveServer2.autoscaling.enabled=true \
  --set cluster.metastore.autoscaling.enabled=true \
  --set 'cluster.llapClusters[0].autoscaling.enabled=true' \
  --set cluster.tezAm.autoscaling.enabled=true
```

**Values file (for customizing beyond defaults):**

```yaml
# values-autoscaling.yaml — only override what you need
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

  hiveServer2:
    replicas: 10              # Acts as maxReplicas when autoscaling is enabled
    autoscaling:
      enabled: true
      # minReplicas: 1        # default — always keep at least 1 HS2 running
      # scaleUpThreshold: 80  # default — avg open sessions per pod triggering scale-up
      # scaleUpStabilizationSeconds: 60   # default — scale-up window
      # scaleDownStabilizationSeconds: 600 # default — scale-down window (also acts as cooldown)
      # metricsScrapeIntervalSeconds: 10  # default — operator scrape interval (lower = faster reaction)

  metastore:
    replicas: 6               # Acts as maxReplicas when autoscaling is enabled
    autoscaling:
      enabled: true
      # minReplicas: 1        # default — always keep at least 1 metastore running
      # scaleUpThreshold: 75  # default — API request rate (req/s) triggering scale-up
      # scaleUpStabilizationSeconds: 60   # default — scale-up window
      # scaleDownStabilizationSeconds: 300 # default — scale-down window (also acts as cooldown)
      # gracePeriodSeconds: 60 # default — fast drain (HMS is stateless)
      # metricsScrapeIntervalSeconds: 10  # default — operator scrape interval

  llapClusters:
  - name: llap0
    replicas: 8               # Acts as maxReplicas when autoscaling is enabled
    autoscaling:
      enabled: true
      # minReplicas: 0        # default — scale to zero when no sessions target this cluster
      # scaleUpThreshold: 1   # default — total busy slots (queued+running) triggering scale-up
      # scaleUpStabilizationSeconds: 60   # default — scale-up window
      # scaleDownStabilizationSeconds: 900 # default — scale-down window (long — scaling down destroys cache)
      # gracePeriodSeconds: 600 # default — 10 min drain for in-flight fragments
      # metricsScrapeIntervalSeconds: 10  # default — operator scrape interval (lower = faster reaction)

  tezAm:
    replicas: 10              # Acts as maxReplicas when autoscaling is enabled
    autoscaling:
      enabled: true
      # minReplicas: 0        # default — scale to zero when no HS2 sessions
      # scaleUpThreshold: 1   # default — threshold for demand metric (1 = match HS2 pod count)
      # scaleUpStabilizationSeconds: 60   # default — HPA scale-up window
      # scaleDownStabilizationSeconds: 300 # default — HPA scale-down window
      # gracePeriodSeconds: 120 # default — 2 min drain for DAG completion
      # metricsScrapeIntervalSeconds: 10  # default — operator scrape interval (lower = faster reaction)
```

```bash
helm install hive ./helm/hive-operator -f values-autoscaling.yaml
```

When autoscaling is enabled, the operator automatically:
- Deploys the JMX Exporter javaagent (port 9404, `/metrics`)
- Enables `hive.server2.metrics.enabled` / `metastore.metrics.enabled` (JMX reporter)
- Attaches JMX Exporter javaagent (port 9404, `/metrics`) to each pod
- Creates PodDisruptionBudgets (minAvailable: 1)
- Configures preStop lifecycle hooks for graceful drain
- Sets `terminationGracePeriodSeconds` to the configured grace period
- LLAP/TezAM use HS2 metrics as activation gate (only scale when HS2 has sessions)

**JMX Metrics Scraped by Operator (per component):**

| Component | Key Metrics | Purpose |
|-----------|---------|---------|
| **HiveServer2** | `hs2_open_sessions`, `jvm_process_cpu_load` | Session count for primary scaling + CPU for secondary scaling signal |
| **Metastore** | `api_*_total`, `jvm_process_cpu_load` | API call counters (operator computes request rate from deltas) + CPU for secondary scaling signal |
| **LLAP** | `hadoop_llapdaemon_executornumqueuedrequests`, `hadoop_llapdaemon_executornumexecutorsconfigured`, `hadoop_llapdaemon_executornumexecutorsavailable` | Total busy slots = queued + configured - available |
| **Tez AM** | N/A (scales on HS2 metrics) | TezAM scaling is demand-driven from `hs2_open_sessions` — no TezAM-specific metrics needed |

### Enabling Autoscaling — Example

To enable autoscaling for HS2 and Metastore:

```yaml
cluster:
  hiveServer2:
    replicas: 4                 # max replicas ceiling
    autoscaling:
      enabled: true
      scaleUpThreshold: 1       # scale up when total sessions > 1
      minReplicas: 1            # always keep at least 1 HS2 pod running

  metastore:
    replicas: 3                 # max replicas ceiling
    autoscaling:
      enabled: true
      minReplicas: 1            # always keep at least 1 running
      scaleUpThreshold: 75      # API requests/sec threshold
```

> **Note:** LLAP scales on total busy slots (queued + running executors).
> TezAM scales on demand — the number of active HS2 pods multiplied by
> `hive.server2.tez.sessions.per.default.queue` (default 1).

### Helm Values Reference (Autoscaling)

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.<component>.replicas` | `1-2` | Static replica count, or max replicas ceiling when autoscaling is enabled |
| `cluster.<component>.autoscaling.enabled` | `false` | Enable operator-driven autoscaling |
| `cluster.<component>.autoscaling.minReplicas` | `1` (HS2/HMS), `0` (LLAP/TezAM) | Minimum replica count. Set to 0 for scale-to-zero (LLAP, TezAM only; HS2 minimum is 1) |
| `cluster.<component>.autoscaling.scaleUpThreshold` | varies | Metric threshold triggering scale-up |
| `cluster.<component>.autoscaling.scaleUpStabilizationSeconds` | `60` | Stabilization window for scale-up (picks highest recommendation in window) |
| `cluster.<component>.autoscaling.scaleDownStabilizationSeconds` | `300-900` | Stabilization window for scale-down (picks most conservative recommendation in window). Also acts as cooldown between consecutive scale-downs. |
| `cluster.<component>.autoscaling.gracePeriodSeconds` | `3600` | Safety cap: max drain time before forced termination. Pod exits immediately once drain completes. |
| `cluster.<component>.autoscaling.metricsScrapeIntervalSeconds` | `10` | How often the operator scrapes JMX metrics from pods. Lower = faster reaction. |
| `cluster.<component>.autoscaling.cpuScaleUpThreshold` | `90` | CPU percentage (0-100) triggering scale-up. Only HS2/HMS. Set to 0 to disable. |
| `cluster.<component>.autoscaling.cpuScaleDownThreshold` | `30` | CPU percentage (0-100) below which scale-down is considered. Only HS2/HMS. |

---

## Connect to HiveServer2

HiveServer2 runs in **HTTP transport mode** by default (recommended for Kubernetes
environments as it works well with load balancers, ingress controllers, and proxies).

### Standard Connection (minReplicas >= 1)

When HS2 always has at least one pod running, connect directly to the service:

```bash
kubectl exec -it deployment/hive-hiveserver2 -- beeline -u "jdbc:hive2://hive-hiveserver2:10001/;transportMode=http;httpPath=cliservice"
```

Or via port-forward:

```bash
kubectl port-forward svc/hive-hiveserver2 10001:10001
beeline -u "jdbc:hive2://localhost:10001/;transportMode=http;httpPath=cliservice"
```

### LLAP/TezAM Scale-to-Zero Behavior

When LLAP and TezAM are configured with `minReplicas: 0` (the default), they start
with zero pods on fresh install. The operator automatically scales them up when HS2
reports open sessions, and scales them back to zero when HS2 is idle.

Since HS2 always runs at least 1 pod (`minReplicas >= 1`), no special connection
setup is needed — simply connect to HS2 and the operator wakes LLAP/TezAM as needed.

> **Note:** The operator sets `hive.server2.transport.mode=http`,
> `hive.server2.thrift.http.port=10001`, and
> `hive.server2.thrift.http.path=cliservice` by default. The binary Thrift
> port (10000) is still exposed for backward compatibility but HTTP mode
> is the primary transport. To override, use `configOverrides` in the
> HiveServer2 spec.

> **Metastore HTTP Mode:** The operator configures HMS in HTTP transport mode
> (`metastore.server.thrift.transport.mode=http`) and sets the matching client
> config (`hive.metastore.client.thrift.transport.mode=http`) on HS2 and TezAM.
> HTTP mode makes Metastore connections stateless — each RPC is an independent
> HTTP request, so Metastore pods can scale down safely without breaking active
> connections from HiveServer2. The port remains 9083 (same as binary mode).

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
| `cluster.imagePullPolicy` | `IfNotPresent` | Image pull policy: `Always`, `Never`, or `IfNotPresent` |

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
| `cluster.metastore.extraVolumes` | `[]` | Additional volumes for Metastore pods |
| `cluster.metastore.extraVolumeMounts` | `[]` | Additional volume mounts for Metastore containers |

### HiveServer2

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.hiveServer2.replicas` | `2` | Replica count |
| `cluster.hiveServer2.serviceType` | `ClusterIP` | K8s Service type |
| `cluster.hiveServer2.resources` | `{}` | CPU/memory |
| `cluster.hiveServer2.configOverrides` | `{}` | Extra `hive-site.xml` properties (use `hive.server2.thrift.port` / `hive.server2.webui.port` to override ports) |
| `cluster.hiveServer2.externalJars` | `[]` | HS2-specific JARs |
| `cluster.hiveServer2.extraVolumes` | `[]` | Additional volumes for HS2 pods |
| `cluster.hiveServer2.extraVolumeMounts` | `[]` | Additional volume mounts for HS2 containers |

### LLAP Clusters

LLAP is configured as an array (`llapClusters`) to support multi-tenant deployments with
independent scaling. Each entry creates a separate LLAP StatefulSet, Service, ConfigMap,
and a paired TezAM Deployment (when `tezAm.enabled: true`).

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.llapClusters[].name` | *(required)* | Unique name for this LLAP cluster (e.g., `llap0`) |
| `cluster.llapClusters[].enabled` | `true` | Enable this LLAP cluster |
| `cluster.llapClusters[].replicas` | `2` | Replica count (maxReplicas when autoscaling enabled) |
| `cluster.llapClusters[].executors` | `1` | Executors per daemon |
| `cluster.llapClusters[].memoryMb` | `1024` | Memory per daemon (MB) |
| `cluster.llapClusters[].resources` | `{}` | CPU/memory |
| `cluster.llapClusters[].configOverrides` | `{}` | Extra LLAP config properties |
| `cluster.llapClusters[].extraVolumes` | `[]` | Additional volumes for LLAP pods |
| `cluster.llapClusters[].extraVolumeMounts` | `[]` | Additional volume mounts for LLAP containers |
| `cluster.llapClusters[].autoscaling.enabled` | `false` | Enable per-cluster autoscaling |
| `cluster.llapClusters[].autoscaling.minReplicas` | `0` | Min replicas (0 = scale to zero) |
| `cluster.llapClusters[].autoscaling.scaleUpThreshold` | `1` | Busy-slot threshold for scale-up |

HS2 routes sessions to LLAP clusters server-side based on user/group identity:

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.llapClusterRouting` | `""` | Routing rules (e.g., `"user:alice=llap1,default=llap0"`) |

The operator auto-generates per-cluster namespace definitions in hive-site.xml.
Clients connect with just their identity — no cluster-specific JDBC URL params needed.

### Tez AM

TezAM is deployed as one Deployment per LLAP cluster. The global `tezAm` section
controls shared settings (enabled flag, scratch PVC). Per-LLAP TezAM settings
(replicas, autoscaling) can be overridden in each `llapClusters[].tezAm` entry.

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.tezAm.enabled` | `true` | Enable Tez Application Master (one per LLAP cluster) |
| `cluster.tezAm.replicas` | `2` | Default replica count per TezAM (overridable per LLAP cluster) |
| `cluster.tezAm.scratchStorageSize` | `1Gi` | Shared scratch PVC size (single PVC shared by all HS2 and TezAM pods) |
| `cluster.tezAm.scratchStorageClassName` | | StorageClass (must support ReadWriteMany) |
| `cluster.tezAm.resources` | `{}` | CPU/memory |
| `cluster.tezAm.configOverrides` | `{}` | Extra TezAM config properties |
| `cluster.tezAm.extraVolumes` | `[]` | Additional volumes for TezAM pods |
| `cluster.tezAm.extraVolumeMounts` | `[]` | Additional volume mounts for TezAM containers |

### Auto-Suspend

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.autoSuspend.enabled` | `false` | Enable full cluster hibernation after idle timeout. Requires autoscaling enabled on all active components (HMS only if `includeMetastore=true`). |
| `cluster.autoSuspend.idleTimeoutMinutes` | `15` | Minutes of idle time (HS2=0 sessions, LLAP/TezAM at minReplicas) before the cluster suspends. |
| `cluster.autoSuspend.includeMetastore` | `true` | Whether HMS participates in auto-suspend. When false, HMS stays at minReplicas during suspend and HMS autoscaling is not required. |

### Autoscaling (per component)

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.<component>.autoscaling.enabled` | `false` | Enable operator-driven autoscaling for this component |
| `cluster.<component>.autoscaling.minReplicas` | `0` | Floor replica count. 0 enables scale-to-zero (LLAP, TezAM only; HS2 minimum is 1) |
| `cluster.<component>.autoscaling.scaleUpThreshold` | `100` (HS2/HMS), `10` (LLAP) | Metric threshold per pod triggering scale-up (sessions for HS2, connections for HMS, busy slots for LLAP). TezAM scales 1:1 with demand (no threshold). |
| `cluster.<component>.autoscaling.scaleUpStabilizationSeconds` | `60` | Stabilization window for scale-up decisions (prevents flapping) |
| `cluster.<component>.autoscaling.scaleDownStabilizationSeconds` | `300-900` | Stabilization window for scale-down decisions (also acts as cooldown between consecutive scale-downs) |
| `cluster.<component>.autoscaling.gracePeriodSeconds` | `3600` | Safety cap (seconds) — pod terminates immediately once drain completes, this is only the upper bound |
| `cluster.<component>.autoscaling.metricsScrapeIntervalSeconds` | `10` | How often the operator polls JMX metrics from pods. Lower = faster reaction time. |
| `cluster.<component>.autoscaling.cpuScaleUpThreshold` | `90` | CPU percentage (0-100) triggering scale-up. Only HS2/HMS. Set to 0 to disable. |
| `cluster.<component>.autoscaling.cpuScaleDownThreshold` | `30` | CPU percentage (0-100) below which scale-down is considered. Only HS2/HMS. |

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
kubectl delete hivecluster --all -A --wait=false --ignore-not-found
helm uninstall hive --ignore-not-found
kubectl delete crd hiveclusters.hive.apache.org --wait=false --ignore-not-found
helm uninstall ozone --ignore-not-found
helm uninstall postgres --ignore-not-found
helm uninstall zookeeper --ignore-not-found
kubectl delete pvc data-zookeeper-0 data-postgres-postgresql-0 --ignore-not-found
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
  +-- [JOSDK Workflow Dependents]
  |     +-- HadoopConfigMapDependent          (core-site.xml)
  |     +-- MetastoreConfigMapDependent       (metastore-site.xml)
  |     +-- HiveServer2ConfigMapDependent     (hive-site.xml + tez-site.xml)
  |     +-- SchemaInitJobDependent            (schematool -initOrUpgradeSchema)
  |     +-- MetastoreDeploymentDependent      --> MetastoreServiceDependent
  |     +-- HiveServer2DeploymentDependent    --> HiveServer2ServiceDependent
  |     +-- ScratchPvcDependent               (shared scratch PVC for HS2 ↔ TezAM)
  |
  +-- [Imperative] Per-LLAP-Cluster Resources (for each llapClusters[] entry):
        +-- LLAP StatefulSet + headless Service + ConfigMap + PDB
        +-- TezAM Deployment + headless Service + ConfigMap (one TezAM per LLAP cluster)
```

LLAP clusters and their paired TezAM instances are managed imperatively by the reconciler
(not via JOSDK workflow dependents) because the number of clusters is dynamic — determined
at runtime from the CR spec. Each `llapClusters[]` entry produces:
- **LLAP**: StatefulSet (`{cluster}-{name}`), headless Service, ConfigMap (`llap-daemon-site.xml`), PDB
- **TezAM**: Deployment (`{cluster}-tezam-{name}`), headless Service, ConfigMap (`tez-site.xml`)

All imperative resources are applied via `serverSideApply()`. Removed LLAP clusters (and
their TezAMs) are garbage-collected automatically using label-based discovery.

**Startup order:**
1. ConfigMaps (Hadoop, Metastore, HiveServer2)
2. Schema Init Job [if Metastore enabled]
3. Metastore Deployment + Service [if enabled]
4. HiveServer2 Deployment + Service + Shared Scratch PVC
5. LLAP clusters + paired TezAM instances [if enabled]
