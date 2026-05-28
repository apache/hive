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

---

## Autoscaling

The operator supports metric-based autoscaling for all four Hive components using
[KEDA](https://keda.sh/) ScaledObjects and Kubernetes-native HPA. Autoscaling is
opt-in per component and designed for **zero query failures** during scale-down.

### Prerequisites

- [KEDA](https://keda.sh/) installed in the cluster
- [Prometheus](https://prometheus.io/) scraping Hive pod metrics (for HS2, HMS, LLAP custom metrics)
- Kubernetes metrics-server (for CPU-based triggers on Tez AM)
- [KEDA HTTP Add-on](https://github.com/kedacore/http-add-on) — **required for `minReplicas: 0`**, enables automatic wake-from-zero for HS2

### Installing KEDA

KEDA must be installed **before** enabling autoscaling on any Hive component.
The operator creates KEDA `ScaledObject` custom resources which require the KEDA
CRDs to be present on the cluster.

```bash
# Add the KEDA Helm repo
helm repo add kedacore https://kedacore.github.io/charts
helm install keda kedacore/keda --namespace keda --create-namespace --wait
```

Verify KEDA is running:

```bash
kubectl get pods -n keda
# Expected: keda-operator, keda-metrics-apiserver, keda-admission-webhooks
kubectl get crd | grep keda
# Expected: scaledobjects.keda.sh, scaledjobs.keda.sh, triggerauthentications.keda.sh, etc.
```

**For HS2 scale-to-zero** (`minReplicas: 0`), install the KEDA HTTP Add-on:

```bash
helm install http-add-on kedacore/keda-add-ons-http \
  --namespace keda --wait
```

Verify the interceptor is running:

```bash
kubectl get pods -n keda -l app=keda-add-ons-http-interceptor-proxy
# Expected: keda-add-ons-http-interceptor-proxy-... Running
```

> **Note:** The HTTP Add-on is required when `minReplicas: 0`. The operator creates
> an `InterceptorRoute` CRD that configures the interceptor proxy to route traffic
> to HS2. When HS2 has zero pods, the interceptor holds incoming requests and triggers
> scale-up via an `external-push` trigger on the HS2 ScaledObject. The first request
> takes ~30-60s while the pod starts.

**For Prometheus-based triggers** (HS2, HMS, LLAP), install Prometheus:

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/prometheus \
  --namespace monitoring --create-namespace --wait
```

> **Note:** If autoscaling is enabled in the HiveCluster spec but KEDA is not
> installed, the operator will fail to reconcile with errors like
> `"Could not find the metadata for the given apiVersion and kind"`.
> Always install KEDA before setting `autoscaling.enabled: true`.

### Graceful Scale-Down Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Scale Down Flow                                │
├─────────────────────────────────────────────────────────────────────┤
│  1. KEDA reduces desired replicas (cooldown elapsed, metric below   │
│     threshold)                                                       │
│  2. PodDisruptionBudget ensures minAvailable=1 (at least one pod    │
│     always running)                                                  │
│  3. Kubernetes sends SIGTERM to selected pod                        │
│  4. preStop hook runs:                                              │
│     - HS2: deregisters from ZK, drains open sessions                │
│     - HMS: sleeps 30s for in-flight Thrift RPCs                     │
│     - LLAP: waits until all executors become idle                   │
│     - TezAM: waits for current DAG completion                       │
│  5. terminationGracePeriodSeconds = gracePeriodSeconds (safety net) │
│  6. Pod terminates only after drain completes                       │
└─────────────────────────────────────────────────────────────────────┘
```

### Per-Component Scaling Logic

| Component | Scale-Up Trigger | Scale-Down Trigger | Cooldown | Native Metric |
|-----------|-----------------|-------------------|----------|---------------|
| **HiveServer2** | `hs2_open_sessions` > scaleUpThreshold **OR** CPU > 75% | Sessions below threshold **AND** CPU below scaleDownThreshold | 5 min | `hs2_open_sessions` |
| **Metastore** | `hive_metastore_open_connections` > scaleUpThreshold **OR** CPU > 75% | Connections below threshold **AND** CPU below scaleDownThreshold | 5 min | `hive_metastore_open_connections` |
| **LLAP** | Total busy slots > scaleUpThreshold (queued + busy executors) | All executors idle + no HS2 sessions | 15 min | `NumQueuedRequests`, `NumExecutorsConfigured`, `NumExecutorsAvailable` |
| **Tez AM** (with CPU resources) | Pod CPU > scaleUpThreshold% | Pod CPU < scaleDownThreshold% + no HS2 sessions | 5 min | Standard K8s CPU |
| **Tez AM** (without CPU resources) | `tez_session_pending_tasks` > scaleUpThreshold | No pending tasks + no HS2 sessions | 5 min | `tez_session_pending_tasks` |

### Scale-to-Zero Architecture

When `minReplicas: 0` is configured (default for HS2, LLAP, TezAM), the cluster
scales down to zero pods when completely idle. The operator uses a **unified
ScaledObject + InterceptorRoute** architecture — a single KEDA ScaledObject per
component handles both Prometheus-based scaling and wake-from-zero, while an
`InterceptorRoute` (from the KEDA HTTP Add-on) provides routing-only configuration
without creating a conflicting second ScaledObject.

```
                   Scale-to-Zero (Idle Detection)                    

  1. No open sessions/queries for cooldownPeriod seconds              
     → KEDA detects all triggers inactive                            
     → scales HS2 to 0 (idleReplicaCount)                           
                                                                     
  2. LLAP/TezAM ScaledObjects see hs2_open_sessions = 0              
     → activation triggers inactive for cooldownPeriod               
     → scale LLAP and TezAM to 0                                    
                                                                     
  3. HMS stays at minReplicas=1 (always available)                   

```

```
                   Wake-from-Zero (with KEDA HTTP Add-on)            

  1. Beeline connects → KEDA HTTP interceptor proxy queues the       
     request and triggers HS2 scale-up via external-push trigger     
                                                                     
  2. HS2 pod starts, reports hs2_open_sessions > 0 to Prometheus     
                                                                     
  3. KEDA detects cross-component activation trigger:                
     - LLAP ScaledObject sees hs2_open_sessions > 0 → scales up      
     - TezAM ScaledObject sees hs2_open_sessions > 0 → scales up   
                                                                     
  4. Query executes once LLAP/TezAM pods are ready                   

```

The HS2 ScaledObject combines three trigger types in a single resource:
- **Prometheus trigger** (`hs2_open_sessions`) — session-aware scaling
- **CPU trigger** (`AverageValue` in millicores) — load-based scaling when `targetCpuValue` is configured
- **external-push trigger** — wake-from-zero via the KEDA HTTP Add-on interceptor

The `InterceptorRoute` CRD (`http.keda.sh/v1beta1`) configures only the interceptor
routing (host matching, backend target) without auto-creating a ScaledObject — this
avoids the dual-HPA conflict that `HTTPScaledObject` would cause.

> **Important:** Automatic wake-from-zero requires the KEDA HTTP Add-on. Traffic
> must flow through the interceptor proxy (via Ingress or port-forward). Without the
> HTTP Add-on, HS2 must be manually woken (`kubectl scale deployment/hive-hiveserver2 --replicas=1`).
> LLAP and TezAM wake automatically once HS2 reports open sessions. See
> [Connect to HiveServer2 > Connecting with Scale-to-Zero](#connecting-with-scale-to-zero-minreplicas--0)
> for setup instructions.

**Component-specific behavior:**

| Component | minReplicas | Scale-to-Zero Trigger | Wake Trigger |
|-----------|-------------|----------------------|--------------|
| **HS2** | 0 | `hs2_open_sessions = 0` for cooldown | HTTP request via KEDA interceptor (`external-push`) |
| **HMS** | 1 | Never (always running) | N/A |
| **LLAP** | 0 | `hs2_open_sessions = 0` for cooldown | `hs2_open_sessions > 0` (cross-component) |
| **TezAM** | 0 | `hs2_open_sessions = 0` + no pending tasks | `hs2_open_sessions > 0` (cross-component) |

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
  --set cluster.llap.autoscaling.enabled=true \
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
      # minReplicas: 0        # default — scale to zero when idle (requires KEDA HTTP Add-on)
      # scaleUpThreshold: 80  # default — avg open sessions per pod triggering scale-up
      # cooldownSeconds: 300  # default — 5 min before scaling down

  metastore:
    replicas: 6               # Acts as maxReplicas when autoscaling is enabled
    autoscaling:
      enabled: true
      # minReplicas: 0        # default — scale to zero when no connections
      # scaleUpThreshold: 75  # default — total open connections triggering scale-up
      # cooldownSeconds: 300  # default — 5 min cooldown
      # gracePeriodSeconds: 60 # default — fast drain (HMS is stateless)

  llap:
    replicas: 8               # Acts as maxReplicas when autoscaling is enabled
    autoscaling:
      enabled: true
      # minReplicas: 0        # default — scale to zero when no HS2 sessions
      # scaleUpThreshold: 1   # default — total busy slots (queued+running) triggering scale-up
      # cooldownSeconds: 900  # default — 15 min (scaling down destroys in-memory cache)
      # gracePeriodSeconds: 600 # default — 10 min drain for in-flight fragments

  tezAm:
    replicas: 10              # Acts as maxReplicas when autoscaling is enabled
    autoscaling:
      enabled: true
      # minReplicas: 0        # default — scale to zero when no HS2 sessions
      # scaleUpThreshold: 5   # default — CPU% (with resources) or pending tasks (without)
      # gracePeriodSeconds: 120 # default — 2 min drain for DAG completion
```

```bash
helm install hive ./helm/hive-operator -f values-autoscaling.yaml
```

When autoscaling is enabled, the operator automatically:
- Deploys the Prometheus JMX Exporter agent sidecar (port 9404, `/metrics`)
- Enables `hive.server2.metrics.enabled` / `metastore.metrics.enabled` (JMX reporter)
- Adds Prometheus scrape annotations to pods
- Creates KEDA ScaledObjects with the configured thresholds
- Creates PodDisruptionBudgets (minAvailable: 1)
- Configures preStop lifecycle hooks for graceful drain
- Sets `terminationGracePeriodSeconds` to the configured grace period
- Adds cross-component activation triggers for LLAP/TezAM (wake when HS2 has open sessions)

**Exported Prometheus Metrics (per component):**

| Component | Key Metrics | Purpose |
|-----------|---------|---------|
| **HiveServer2** | `hs2_open_sessions`, `hs2_active_sessions`, `tez_session_pending_tasks` | Session count (scaling trigger), Tez AM demand |
| **Metastore** | `hive_metastore_open_connections`, `api_*_total` | Connection count (scaling trigger), API call rates |
| **LLAP** | `hadoop_llapdaemon_executornumqueuedrequests`, `hadoop_llapdaemon_executornumexecutorsconfigured`, `hadoop_llapdaemon_executornumexecutorsavailable` | Total busy slots = queued + configured - available (scaling trigger) |
| **Tez AM** | Standard K8s CPU metrics or `tez_session_pending_tasks` (from HS2) | CPU utilization or pending task count (scaling trigger) |

### CPU-Based Scaling

The operator can include a **CPU trigger** in the ScaledObject for HS2, Metastore, and Tez AM.
The trigger uses KEDA's `AverageValue` metric type with **absolute millicore targets** that
you specify directly. This handles burstable QoS pods correctly — unlike `Utilization`
(which measures against the CPU request), `AverageValue` uses actual CPU consumption in
absolute terms, so pods with a small request but high limit won't show perpetual >100%
utilization that prevents scale-down.

**The CPU trigger is opt-in:** it is only added to the ScaledObject when you explicitly set
both `targetCpuValue` and `activationCpuValue` in the autoscaling config. If omitted, the
operator relies solely on the Prometheus-based trigger (sessions, connections, etc.).

**How it works:**

- `targetCpuValue` — the average CPU per pod (e.g., `"1500m"` or `"1"`) that triggers scale-up
- `activationCpuValue` — below this CPU value, the trigger is completely inactive
  (doesn't participate in scaling decisions at all)
- Both the CPU trigger and the Prometheus-based trigger are evaluated independently —
  if **either** exceeds its threshold, the component scales up (OR logic)
- Scale-down only happens when **both** triggers agree load is low
- The component must also have `resources` defined on its pods; if `targetCpuValue` is set
  but `resources` is missing, the operator logs a warning and skips the CPU trigger

**Example:** With `targetCpuValue: "1600m"` and `activationCpuValue: "400m"`, KEDA scales up
when average pod CPU exceeds 1600m and considers the trigger inactive below 400m.

For Tez AM specifically, without CPU targets the operator uses `tez_session_pending_tasks`
(queued tasks waiting for AM slots) as the proportional scaler — this reflects real query
demand rather than connection count, avoiding spurious scale-ups from idle sessions.

To enable both Prometheus and CPU-based scaling:

```yaml
cluster:
  hiveServer2:
    resources:
      requestsCpu: "500m"
      limitsCpu: "2"
      requestsMemory: "2Gi"
    autoscaling:
      enabled: true
      scaleUpThreshold: 1       # scale up when avg sessions > 1 per pod
      targetCpuValue: "1600m"   # scale up when avg CPU > 1600m per pod
      activationCpuValue: "400m" # CPU trigger inactive below 400m

  metastore:
    resources:
      requestsCpu: "500m"
      limitsCpu: "1"
      requestsMemory: "1Gi"
    autoscaling:
      enabled: true
      targetCpuValue: "750m"
      activationCpuValue: "200m"

  tezAm:
    resources:
      requestsCpu: "250m"
      limitsCpu: "1"
      requestsMemory: "1Gi"
    autoscaling:
      enabled: true
      targetCpuValue: "600m"
      activationCpuValue: "100m"
```

| Setting | Effect on CPU trigger |
|---------|----------------------|
| `targetCpuValue` | Absolute CPU target (e.g., `"1500m"` or `"1"`). **Required** to enable CPU trigger. |
| `activationCpuValue` | CPU below which trigger is inactive. **Required** with targetCpuValue. |
| `resources` | Pod resources must be defined — operator warns and skips CPU trigger otherwise. |

> **Note:** LLAP scaling uses only Prometheus triggers (total busy slots)
> and does not include a CPU trigger, so LLAP does not require `targetCpuValue`
> for autoscaling to work.

### Helm Values Reference (Autoscaling)

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.<component>.replicas` | `1-2` | Static replica count, or max replicas ceiling when autoscaling is enabled |
| `cluster.<component>.autoscaling.enabled` | `false` | Enable KEDA-based autoscaling |
| `cluster.<component>.autoscaling.minReplicas` | `0` (HS2/LLAP/TezAM), `1` (HMS) | Minimum replica count. Set to 0 for scale-to-zero |
| `cluster.<component>.autoscaling.scaleUpThreshold` | varies | Metric threshold triggering scale-up |
| `cluster.<component>.autoscaling.scaleDownThreshold` | varies | Metric threshold triggering scale-down |
| `cluster.<component>.autoscaling.cooldownSeconds` | varies | Cooldown after a scaling event |
| `cluster.<component>.autoscaling.gracePeriodSeconds` | varies | Max drain time before forced termination |

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

### Connecting with Scale-to-Zero (minReplicas = 0)

When HS2 is configured with `minReplicas: 0`, the deployment starts with zero pods.
Connections go through the **KEDA HTTP interceptor proxy** which automatically wakes
HS2 when a request arrives (first request takes ~30-60s while the pod starts).

```
Traffic flow:
Client → KEDA HTTP Interceptor → (if 0 pods: scale up, wait) → HS2 Service → HS2 Pod
```

**Via kubectl exec (no local Hive install needed):**

The Metastore pod is always running (`minReplicas=1`) and has beeline pre-installed.
Connecting through the interceptor wakes HS2 from zero automatically:

```bash
kubectl exec -it deploy/hive-metastore -- beeline -u "jdbc:hive2://keda-add-ons-http-interceptor-proxy.keda.svc:8080/;transportMode=http;httpPath=cliservice"
```

Or connect directly when HS2 is already running:

```bash
kubectl exec -it deploy/hive-metastore -- beeline -u "jdbc:hive2://hive-hiveserver2:10001/;transportMode=http;httpPath=cliservice"
```

**Via port-forward (local development):**

```bash
# Port-forward the KEDA HTTP interceptor proxy
kubectl port-forward -n keda svc/keda-add-ons-http-interceptor-proxy 8080:8080

# Connect — interceptor auto-wakes HS2 (first request may take 30-60s)
beeline -u "jdbc:hive2://localhost:8080/;transportMode=http;httpPath=cliservice"
```

**Via Ingress:**

Create an Ingress that routes to the KEDA interceptor. Uses [nip.io](https://nip.io)
wildcard DNS so no `/etc/hosts` editing is needed — `hive.127.0.0.1.nip.io` resolves
to `127.0.0.1` automatically:

```bash
kubectl create ingress hive-interceptor -n keda --class=nginx \
  --rule="hive.127.0.0.1.nip.io/*=keda-add-ons-http-interceptor-proxy:8080" \
  --annotation="nginx.ingress.kubernetes.io/upstream-vhost=hive-hiveserver2.default.svc.cluster.local"
```

> The `upstream-vhost` annotation rewrites the Host header to the internal service
> name so the KEDA interceptor can match and route the request.

Connect via beeline using the Ingress:

```bash
beeline -u "jdbc:hive2://hive.127.0.0.1.nip.io:80/;transportMode=http;httpPath=cliservice"
```

> For production, replace `hive.127.0.0.1.nip.io` with your actual domain
> (e.g., `hive.example.com`) and ensure DNS points to your ingress controller.

**Manual wake (fallback without HTTP Add-on):**

```bash
kubectl scale deployment/hive-hiveserver2 --replicas=1
kubectl wait --for=condition=ready pod -l app.kubernetes.io/component=hiveserver2 --timeout=120s
kubectl exec -it deployment/hive-hiveserver2 -- beeline -u "jdbc:hive2://hive-hiveserver2:10001/;transportMode=http;httpPath=cliservice"
```

> **Note:** The operator sets `hive.server2.transport.mode=http`,
> `hive.server2.thrift.http.port=10001`, and
> `hive.server2.thrift.http.path=cliservice` by default. The binary Thrift
> port (10000) is still exposed for backward compatibility but HTTP mode
> is the primary transport. To override, use `configOverrides` in the
> HiveServer2 spec.

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

### LLAP

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.llap.enabled` | `true` | Enable LLAP daemons |
| `cluster.llap.replicas` | `2` | Replica count |
| `cluster.llap.executors` | `1` | Executors per daemon |
| `cluster.llap.memoryMb` | `1024` | Memory per daemon (MB) |
| `cluster.llap.serviceHosts` | `@llap0` | LLAP ZK identity |
| `cluster.llap.resources` | `{}` | CPU/memory |
| `cluster.llap.configOverrides` | `{}` | Extra LLAP config properties |
| `cluster.llap.extraVolumes` | `[]` | Additional volumes for LLAP pods |
| `cluster.llap.extraVolumeMounts` | `[]` | Additional volume mounts for LLAP containers |

### Tez AM

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.tezAm.enabled` | `true` | Enable Tez Application Master |
| `cluster.tezAm.replicas` | `2` | Replica count |
| `cluster.tezAm.scratchStorageSize` | `1Gi` | Shared scratch PVC size |
| `cluster.tezAm.scratchStorageClassName` | | StorageClass (must support RWX) |
| `cluster.tezAm.resources` | `{}` | CPU/memory |
| `cluster.tezAm.configOverrides` | `{}` | Extra TezAM config properties |
| `cluster.tezAm.extraVolumes` | `[]` | Additional volumes for TezAM pods |
| `cluster.tezAm.extraVolumeMounts` | `[]` | Additional volume mounts for TezAM containers |

### Autoscaling (per component)

| Value | Default | Description |
|-------|---------|-------------|
| `cluster.<component>.autoscaling.enabled` | `false` | Enable KEDA-based autoscaling for this component |
| `cluster.<component>.autoscaling.minReplicas` | `0` | Floor replica count. 0 enables scale-to-zero (HS2 requires KEDA HTTP Add-on) |
| `cluster.<component>.autoscaling.scaleUpThreshold` | `80` | Metric threshold triggering scale-up (sessions for HS2, connections for HMS, busy slots for LLAP, pending tasks or CPU% for TezAM) |
| `cluster.<component>.autoscaling.scaleDownThreshold` | `30` | Prometheus metric threshold for scale-down (component-specific) |
| `cluster.<component>.autoscaling.targetCpuValue` | — | Absolute CPU target for scale-up (e.g., `1500m`). Omit to disable CPU trigger. |
| `cluster.<component>.autoscaling.activationCpuValue` | — | CPU value below which CPU trigger is inactive. Required with targetCpuValue. |
| `cluster.<component>.autoscaling.cooldownSeconds` | `300` | Seconds to wait after last scale event before scaling down again |
| `cluster.<component>.autoscaling.gracePeriodSeconds` | `60-600` | Max time (seconds) to wait for graceful drain before forced termination |

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
kubectl delete ingress hive-interceptor -n keda --ignore-not-found
helm uninstall hive --ignore-not-found
kubectl delete crd hiveclusters.hive.apache.org --wait=false --ignore-not-found
kubectl delete crd --wait=false --ignore-not-found scaledobjects.keda.sh scaledjobs.keda.sh triggerauthentications.keda.sh clustertriggerauthentications.keda.sh httpscaledobjects.http.keda.sh interceptorroutes.http.keda.sh
helm uninstall http-add-on -n keda --ignore-not-found
helm uninstall keda -n keda --ignore-not-found
helm uninstall prometheus -n monitoring --ignore-not-found
helm uninstall ozone --ignore-not-found
helm uninstall postgres --ignore-not-found
helm uninstall zookeeper --ignore-not-found
kubectl delete pvc data-zookeeper-0 data-postgres-postgresql-0 --ignore-not-found
kubectl delete secret hive-db-secret --ignore-not-found
kubectl delete namespace keda --wait=false --ignore-not-found
kubectl delete namespace monitoring --wait=false --ignore-not-found
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
