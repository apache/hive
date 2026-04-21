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
- **S3-compatible storage**: point Hive at any S3 endpoint (Apache Ozone,
  MinIO, AWS S3, etc.)
- **Automatic dependency ordering**: schema init before Metastore, Metastore
  before HiveServer2, etc.
- **Optional components**: LLAP and Tez AM are enabled/disabled via spec flags
- **Status reporting**: per-component readiness tracked on the CRD status

## Architecture

```
HiveCluster CR
  |
  v
HiveClusterReconciler
  |
  +-- HadoopConfigMapDependent          (core-site.xml with auto-injected S3A config)
  +-- MetastoreConfigMapDependent       (metastore-site.xml)
  +-- HiveServer2ConfigMapDependent     (hive-site.xml + tez-site.xml)
  +-- SchemaInitJobDependent            (schematool -initOrUpgradeSchema)
  +-- MetastoreDeploymentDependent      --> MetastoreServiceDependent
  +-- HiveServer2DeploymentDependent    --> HiveServer2ServiceDependent
  +-- LlapStatefulSetDependent          --> LlapServiceDependent          (optional)
  +-- ScratchPvcDependent               (shared scratch PVC for HS2+TezAM, optional)
  +-- TezAmStatefulSetDependent         --> TezAmServiceDependent         (optional)
```

**Startup order:**

1. ConfigMaps (Hadoop, Metastore, HiveServer2)
2. Schema Init Job (`schematool -initOrUpgradeSchema`)
3. Metastore Deployment + Service
4. HiveServer2 Deployment + Service
5. LLAP StatefulSet + Scratch PVC + Tez AM StatefulSet (if enabled)

## Prerequisites

- Kubernetes cluster (minikube, kind, EKS, GKE, etc.)
- `kubectl` configured to talk to the cluster
- Java 21+ and Maven 3.8+ (for building)
- A ZooKeeper instance accessible from the cluster
- An S3-compatible storage endpoint (see [Storage Setup](#storage-setup) below)

## Build

The operator module is activated via a Maven profile:

```bash
mvn clean package -pl packaging/src/kubernetes -Pkubernetes -DskipTests
```

This produces:

| Artifact | Path |
|----------|------|
| Shaded JAR | `target/hive-kubernetes-operator-*-shaded.jar` |
| CRD YAML (v1) | `target/classes/META-INF/fabric8/hiveclusters.hive.apache.org-v1.yml` |

## Build the Operator Docker Image

```bash
cd packaging/src/kubernetes

docker build -t apache/hive-kubernetes-operator:latest .
```

For **minikube**, build inside the minikube Docker daemon:

```bash
eval $(minikube docker-env)
docker build -t apache/hive-kubernetes-operator:latest .
```

For **kind**, load the image into the cluster:

```bash
docker build -t apache/hive-kubernetes-operator:latest .
kind load docker-image apache/hive-kubernetes-operator:latest
```

## Install the CRD and Operator

These steps are the same regardless of which deployment scenario you choose.

### 1. Install the CRD

```bash
kubectl apply -f target/classes/META-INF/fabric8/hiveclusters.hive.apache.org-v1.yml
```

Verify:

```bash
kubectl get crd hiveclusters.hive.apache.org
```

### 2. Deploy RBAC and the Operator

```bash
kubectl create namespace hive-operator

kubectl apply -f config/rbac/service-account.yaml
kubectl apply -f config/rbac/cluster-role.yaml
kubectl apply -f config/rbac/cluster-role-binding.yaml
kubectl apply -f config/operator/deployment.yaml
```

Verify the operator is running:

```bash
kubectl -n hive-operator get pods
```

### 3. Deploy ZooKeeper (if you don't have one)

ZooKeeper is required for Tez session management. Skip this if you already
have a ZooKeeper instance.

```bash
kubectl apply -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zookeeper
spec:
  replicas: 1
  selector:
    matchLabels:
      app: zookeeper
  template:
    metadata:
      labels:
        app: zookeeper
    spec:
      containers:
      - name: zookeeper
        image: zookeeper:3.9
        ports:
        - containerPort: 2181
---
apiVersion: v1
kind: Service
metadata:
  name: zookeeper
spec:
  selector:
    app: zookeeper
  ports:
  - port: 2181
EOF
```

---

## Storage Setup

The operator requires an S3-compatible storage endpoint. It does **not** deploy
a storage cluster itself — you provide one externally. Below are common options.

### Using Apache Ozone (Helm Chart)

Apache Ozone provides an S3-compatible object store. Use the official Helm
chart to deploy it alongside the operator.

#### Step 1: Install Ozone via Helm

```bash
helm repo add ozone https://apache.github.io/ozone-helm-charts/
helm install ozone ozone/ozone --version 0.2.0 --wait
```

For resource-constrained environments (e.g., CI, minikube), create a
`ozone-values.yaml`:

```yaml
datanode:
  replicas: 1
env:
- name: OZONE-SITE.XML_hdds.datanode.volume.min.free.space
  value: "256MB"
- name: OZONE-SITE.XML_hdds.scm.safemode.enabled
  value: "false"
- name: OZONE-SITE.XML_ozone.scm.container.size
  value: 128MB
- name: OZONE-SITE.XML_ozone.scm.block.size
  value: 32MB
- name: OZONE-SITE.XML_ozone.server.default.replication
  value: "1"
```

Then install with:

```bash
helm install ozone ozone/ozone --version 0.2.0 --values ozone-values.yaml --wait
```

#### Step 2: Create the Ozone bucket

```bash
kubectl exec statefulset/ozone-om -- ozone sh volume create /s3v
kubectl exec statefulset/ozone-om -- ozone sh bucket create /s3v/hive
```

#### Step 3: Configure the HiveCluster CR

Point the `storage` section at the Ozone S3 Gateway service deployed by Helm:

```yaml
  storage:
    endpoint: "http://ozone-s3g-rest:9878"
    bucket: hive
    accessKey: "ozone"
    secretKey: "ozone"
```

The Ozone Helm chart exposes the S3 Gateway as a Kubernetes Service named
`ozone-s3g-rest` on port `9878`. Default Ozone credentials are `ozone`/`ozone`.

#### Teardown

```bash
helm uninstall ozone
```

### Using MinIO

```yaml
  storage:
    endpoint: "http://minio.minio-ns.svc:9000"
    accessKeySecretRef:
      name: minio-creds
      key: accessKey
    secretKeySecretRef:
      name: minio-creds
      key: secretKey
```

### Using AWS S3

```yaml
  storage:
    endpoint: "https://s3.amazonaws.com"
    pathStyleAccess: false
    accessKeySecretRef:
      name: aws-creds
      key: accessKey
    secretKeySecretRef:
      name: aws-creds
      key: secretKey
```

---

## Deployment Scenarios

### Scenario 1: Minimal Cluster (Metastore + HiveServer2)

**Use this when:** you want a basic Hive cluster backed by external
S3-compatible storage and PostgreSQL.

#### Step 1: Deploy PostgreSQL

```bash
kubectl create secret generic hive-db-secret \
  --from-literal=password=hive123

kubectl apply -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:15
        env:
        - name: POSTGRES_DB
          value: metastore
        - name: POSTGRES_USER
          value: hive
        - name: POSTGRES_PASSWORD
          value: hive123
        ports:
        - containerPort: 5432
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  selector:
    app: postgres
  ports:
  - port: 5432
EOF
```

#### Step 2: Set up storage

Follow the [Storage Setup](#storage-setup) section above to deploy your
S3-compatible storage (e.g., Ozone via Helm).

#### Step 3: Create the HiveCluster

```bash
kubectl apply -f config/samples/hivecluster-minimal.yaml
```

Or inline:

```bash
kubectl apply -f - <<'EOF'
apiVersion: hive.apache.org/v1alpha1
kind: HiveCluster
metadata:
  name: my-hive
spec:
  image: apache/hive:4.3.0-SNAPSHOT
  imagePullPolicy: IfNotPresent

  metastore:
    replicas: 1
    database:
      type: postgres
      url: "jdbc:postgresql://postgres:5432/metastore"
      driver: "org.postgresql.Driver"
      username: hive
      passwordSecretRef:
        name: hive-db-secret
        key: password
      driverJarUrl: "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar"
    warehouseDir: "s3a://hive/warehouse"

  hiveServer2:
    replicas: 1
    serviceType: ClusterIP

  zookeeper:
    quorum: "zookeeper:2181"

  storage:
    endpoint: "http://ozone-s3g-rest:9878"
    bucket: hive
    accessKey: "ozone"
    secretKey: "ozone"
EOF
```

#### What happens

The operator creates:

| Resource | Purpose |
|----------|---------|
| `my-hive-hadoop-config` ConfigMap | `core-site.xml` with S3A endpoint auto-configured |
| `my-hive-metastore-config` ConfigMap | `metastore-site.xml` with warehouse dir and DB settings |
| `my-hive-hiveserver2-config` ConfigMap | `hive-site.xml` + `tez-site.xml` |
| `my-hive-schema-init` | Job that runs `schematool -initOrUpgradeSchema` |
| `my-hive-metastore` | Metastore Deployment + Service (port 9083) |
| `my-hive-hiveserver2` | HiveServer2 Deployment + Service (port 10000) |

---

### Scenario 2: External PostgreSQL

**Use this when:** you have an existing PostgreSQL instance (e.g. Amazon RDS,
Cloud SQL, or a corporate database).

**What you provide:** the JDBC URL, credentials, and driver jar URL for your
existing database.

#### Step 1: Create the database password Secret

```bash
kubectl create secret generic hive-db-secret \
  --from-literal=password=<your-actual-db-password>
```

#### Step 2: Create the HiveCluster

Point the `database` section at your external PostgreSQL:

```yaml
apiVersion: hive.apache.org/v1alpha1
kind: HiveCluster
metadata:
  name: my-hive
spec:
  image: apache/hive:4.3.0-SNAPSHOT

  metastore:
    replicas: 1
    database:
      type: postgres
      url: "jdbc:postgresql://my-rds-host.us-east-1.rds.amazonaws.com:5432/metastore"
      driver: "org.postgresql.Driver"
      username: hive_admin
      passwordSecretRef:
        name: hive-db-secret
        key: password
      driverJarUrl: "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar"
    warehouseDir: "s3a://hive/warehouse"

  hiveServer2:
    replicas: 1
    serviceType: ClusterIP

  zookeeper:
    quorum: "zookeeper:2181"

  storage:
    endpoint: "https://s3.amazonaws.com"
    bucket: hive
    pathStyleAccess: false
    accessKeySecretRef:
      name: s3-creds
      key: accessKey
    secretKeySecretRef:
      name: s3-creds
      key: secretKey
```

The `driverJarUrl` field tells the operator to add an init container that
downloads the JDBC driver JAR at pod startup. This works for any URL
(Maven Central, internal artifact repo, etc.).

#### Supported databases

| Database | `type` | Example `url` | Example `driver` |
|----------|--------|---------------|------------------|
| PostgreSQL | `postgres` | `jdbc:postgresql://host:5432/metastore` | `org.postgresql.Driver` |
| MySQL | `mysql` | `jdbc:mysql://host:3306/metastore` | `com.mysql.cj.jdbc.Driver` |
| Derby | `derby` | *(embedded, no URL needed)* | *(auto-detected)* |

---

### Scenario 3: Full Cluster (LLAP + Tez AM)

**Use this when:** you want all four Hive services running - Metastore,
HiveServer2, LLAP daemons, and a standalone Tez Application Master.

When `tezAm.enabled` is `true`, HiveServer2 is configured to use external Tez
sessions via ZooKeeper (`hive.server2.tez.use.external.sessions=true`). The
standalone Tez AM registers itself in ZooKeeper and HiveServer2 discovers it
through the registry.

When `tezAm.enabled` is `false` (the default), HiveServer2 runs Tez in local
mode (`tez.local.mode=true`), where the Tez DAG executes inside the HiveServer2
JVM itself.

#### Step 1: Deploy PostgreSQL and Storage

See [Scenario 1 Step 1](#step-1-deploy-postgresql) for PostgreSQL and
[Storage Setup](#storage-setup) for S3-compatible storage.

#### Step 2: Create the HiveCluster

For Non HA
```bash
kubectl apply -f config/samples/hivecluster-full.yaml
```

For HA
```bash
kubectl apply -f config/samples/hivecluster-full-ha.yaml
```

#### What this creates

With all components enabled, the operator creates approximately 12 resources on Non HA mode:

| Category | Resources |
|----------|-----------|
| Hive | Schema-init Job (1), Metastore (1), HiveServer2 (2), LLAP (2), Tez AM (1), scratch PVC (1) |
| You deployed | PostgreSQL (1), ZooKeeper (1), S3-compatible storage |

---

## Monitor

```bash
# Watch pods come up in order
kubectl get pods -w

# Check HiveCluster status and conditions
kubectl get hiveclusters
kubectl describe hivecluster my-hive

# Operator logs
kubectl -n hive-operator logs -f deployment/hive-operator
```

## Connect to HiveServer2

```bash
# Port-forward the thrift port
kubectl port-forward svc/my-hive-hiveserver2 10000:10000

# Connect with Beeline
beeline -u "jdbc:hive2://my-hive-hiveserver2:10000/"
```

Or exec into the HiveServer2 pod directly:

```bash
HS2_POD=$(kubectl get pods -l app.kubernetes.io/component=hiveserver2 \
  -o jsonpath='{.items[0].metadata.name}')

kubectl exec $HS2_POD -- env TERM=dumb /opt/hive/bin/beeline \
  -u "jdbc:hive2://localhost:10000/" -n hive \
  -e "CREATE TABLE test (id INT, name STRING) STORED AS ORC
      LOCATION 's3a://hive/warehouse/test';
      INSERT INTO test VALUES (1, 'hello'), (2, 'world');
      SELECT * FROM test;"
```

If the HiveServer2 Service type is `LoadBalancer` or `NodePort`, use the
external address directly instead of port-forwarding.

## CRD Reference

### Top-level spec

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `spec.image` | string | | Hive Docker image |
| `spec.imagePullPolicy` | string | `IfNotPresent` | Image pull policy |

### Metastore (`spec.metastore`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `replicas` | int | `1` | Number of Metastore replicas |
| `warehouseDir` | string | | Warehouse directory (e.g. `s3a://hive/warehouse`) |
| `configOverrides` | map | | Extra `metastore-site.xml` properties |
| `resources.*` | object | | CPU/memory requests and limits |

### Metastore Database (`spec.metastore.database`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `type` | string | `derby` | DB type: `postgres`, `mysql`, `derby` |
| `url` | string | | JDBC connection URL |
| `driver` | string | | JDBC driver class name |
| `username` | string | | Database username |
| `passwordSecretRef.name` | string | | Kubernetes Secret name containing the password |
| `passwordSecretRef.key` | string | | Key within the Secret |
| `driverJarUrl` | string | | URL to download the JDBC driver JAR at pod startup |

### HiveServer2 (`spec.hiveServer2`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `replicas` | int | `1` | Number of HiveServer2 replicas |
| `serviceType` | string | `ClusterIP` | Kubernetes Service type (`ClusterIP`, `NodePort`, `LoadBalancer`) |
| `thriftPort` | int | `10000` | Thrift port |
| `webUiPort` | int | `10002` | Web UI port |
| `configOverrides` | map | | Extra `hive-site.xml` properties |
| `resources.*` | object | | CPU/memory requests and limits |

### LLAP (`spec.llap`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable LLAP daemons |
| `replicas` | int | `1` | Number of LLAP daemon replicas |
| `executors` | int | `1` | Executors per daemon |
| `memoryMb` | int | `2048` | Memory per daemon (MB) |
| `serviceHosts` | string | | LLAP service hosts identifier (e.g. `@llap0`) |
| `resources.*` | object | | CPU/memory requests and limits |

### Tez AM (`spec.tezAm`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable standalone Tez Application Master |
| `replicas` | int | `1` | Number of Tez AM replicas |
| `scratchStorageSize` | string | `1Gi` | Storage size for the shared scratch PVC (ReadWriteMany) mounted on HS2 and TezAM at `/opt/hive/scratch` |
| `scratchStorageClassName` | string | | StorageClass for the shared scratch PVC. Must support ReadWriteMany. If empty, uses cluster default. |
| `resources.*` | object | | CPU/memory requests and limits |

### ZooKeeper (`spec.zookeeper`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `quorum` | string | `zookeeper:2181` | ZooKeeper connection string |

### Storage (`spec.storage`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `endpoint` | string | | S3-compatible endpoint URL (e.g. `http://ozone-s3g-rest:9878`, `https://s3.amazonaws.com`) |
| `bucket` | string | `hive` | Bucket name for scratch directory |
| `pathStyleAccess` | boolean | `true` | Use path-style S3 access (set `false` for AWS S3 virtual-hosted style) |
| `accessKey` | string | | S3 access key as plain text (dev/test only) |
| `secretKey` | string | | S3 secret key as plain text (dev/test only) |
| `accessKeySecretRef.name` | string | | Kubernetes Secret name containing the S3 access key (recommended for production) |
| `accessKeySecretRef.key` | string | | Key within the Secret |
| `secretKeySecretRef.name` | string | | Kubernetes Secret name containing the S3 secret key (recommended for production) |
| `secretKeySecretRef.key` | string | | Key within the Secret |

### Hadoop (`spec.hadoop`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `coreSiteOverrides` | map | | Extra `core-site.xml` properties (applied after auto-injected config) |

### Resource requirements

All components support a `resources` object with these fields:

| Field | Example |
|-------|---------|
| `resources.requestsCpu` | `"500m"` |
| `resources.requestsMemory` | `"1Gi"` |
| `resources.limitsCpu` | `"2"` |
| `resources.limitsMemory` | `"4Gi"` |

## How Storage Configuration Works

When an S3-compatible endpoint is configured via `spec.storage`, the operator:

1. Injects `fs.s3a.endpoint`, `fs.s3a.path.style.access`, and `fs.s3a.impl`
   into `core-site.xml` and `hive-site.xml`
2. Injects S3 credentials as `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
   environment variables on all Hive containers (Metastore, HiveServer2,
   LLAP, Tez AM) — credentials never appear in ConfigMaps

Credential sources (in priority order):

| Source | How it works |
|--------|-------------|
| `accessKeySecretRef` / `secretKeySecretRef` | Injected via `valueFrom.secretKeyRef` — recommended for production |
| `accessKey` / `secretKey` | Injected as literal env var values — for dev/test only |

S3A credentials are provided via environment variables rather than Hadoop
Configuration properties. This ensures credentials are available to Tez tasks,
which deserialize their Configuration from the DAG payload and may not inherit
classpath XML resources.

### Overrides

Properties in `spec.hadoop.coreSiteOverrides` are applied last and override
any auto-injected values. Properties in `spec.metastore.configOverrides` and
`spec.hiveServer2.configOverrides` override values in `metastore-site.xml` and
`hive-site.xml` respectively.

## Cleanup

```bash
# Delete the HiveCluster (removes all managed pods, services, etc.)
kubectl delete hivecluster my-hive

# Remove the operator
kubectl delete -f config/operator/deployment.yaml
kubectl delete -f config/rbac/cluster-role-binding.yaml
kubectl delete -f config/rbac/cluster-role.yaml
kubectl delete -f config/rbac/service-account.yaml
kubectl delete namespace hive-operator

# Remove the CRD
kubectl delete crd hiveclusters.hive.apache.org
```
Remove Everything

```bash
kubectl delete hivecluster my-hive --ignore-not-found
kubectl delete -f config/operator/deployment.yaml --ignore-not-found
kubectl delete -f config/rbac/ --ignore-not-found
kubectl delete namespace hive-operator --ignore-not-found
kubectl delete crd hiveclusters.hive.apache.org --ignore-not-found
kubectl delete deployment postgres zookeeper --ignore-not-found
kubectl delete svc postgres zookeeper --ignore-not-found
kubectl delete secret hive-db-secret --ignore-not-found
helm uninstall ozone --ignore-not-found 2>/dev/null || true
```
