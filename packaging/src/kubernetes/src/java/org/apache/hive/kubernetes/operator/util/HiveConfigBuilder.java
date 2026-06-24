/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.kubernetes.operator.util;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.DatabaseConfig;
import org.apache.hive.kubernetes.operator.model.spec.HadoopSpec;
import org.apache.hive.kubernetes.operator.model.spec.LlapSpec;
import org.apache.hive.kubernetes.operator.model.spec.MetastoreSpec;

/**
 * Single source of truth for all Hive component configuration properties.
 * Both ConfigMap dependents and Deployment/StatefulSet dependents call these
 * methods, ensuring the config hash always matches the actual ConfigMap content.
 */
public final class HiveConfigBuilder {

  private HiveConfigBuilder() {
  }

  /** Builds hive-site.xml properties for HiveServer2 and TezAM. */
  public static Map<String, String> getHiveServer2HiveSite(
      HiveCluster hiveCluster, HiveClusterSpec spec) {
    Map<String, String> props = new LinkedHashMap<>();
    boolean tezAmEnabled = spec.tezAm().isEnabled();
    String zkQuorum = spec.zookeeper().quorum();

    int metastorePort = ConfigUtils.getInt(
        spec.metastore().configOverrides(),
        ConfigUtils.METASTORE_THRIFT_PORT_KEY,
        ConfigUtils.METASTORE_THRIFT_PORT_HIVE_KEY,
        ConfigUtils.METASTORE_THRIFT_PORT_DEFAULT);
    String metastoreUri = spec.metastore().isEnabled()
        ? "thrift://" + hiveCluster.getMetadata().getName()
            + "-metastore:" + metastorePort
        : spec.metastore().externalUri();
    if (metastoreUri != null && !metastoreUri.isEmpty()) {
      props.put(ConfigUtils.METASTORE_URIS_KEY, metastoreUri);
    }
    // Client-side HTTP transport mode to match metastore server config.
    props.put(ConfigUtils.METASTORE_CLIENT_TRANSPORT_MODE_KEY,
        ConfigUtils.METASTORE_CLIENT_TRANSPORT_MODE_DEFAULT);
    props.put(ConfigUtils.METASTORE_CLIENT_HTTP_PATH_KEY,
        ConfigUtils.METASTORE_CLIENT_HTTP_PATH_DEFAULT);
    props.put(ConfigUtils.HIVE_METASTORE_WAREHOUSE_KEY,
        spec.metastore().warehouseDir());
    props.put(ConfigUtils.HIVE_SERVER2_ENABLE_DOAS_KEY, "false");
    props.put(ConfigUtils.HIVE_SERVER2_TRANSPORT_MODE_KEY,
        ConfigUtils.HIVE_SERVER2_TRANSPORT_MODE_DEFAULT);
    props.put(ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PORT_KEY,
        String.valueOf(ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PORT_DEFAULT));
    props.put(ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PATH_KEY,
        ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PATH_DEFAULT);
    props.put(ConfigUtils.HIVE_TEZ_EXEC_INPLACE_PROGRESS_KEY, "false");
    props.put(ConfigUtils.HIVE_TEZ_EXEC_SUMMARY_KEY, "true");
    props.put(ConfigUtils.HIVE_JAR_DIRECTORY_KEY, "/tmp");
    props.put(ConfigUtils.HIVE_USER_INSTALL_DIR_KEY, "/tmp");
    if (tezAmEnabled) {
      props.put(ConfigUtils.HIVE_LOCAL_SCRATCH_DIR_KEY,
          "/opt/hive/scratch");
    }

    if (tezAmEnabled) {
      props.put(ConfigUtils.HIVE_SERVER2_TEZ_USE_EXTERNAL_SESSIONS_KEY, "true");
      // Default external sessions namespace points to first LLAP cluster's TezAM.
      // Client routes to other clusters by overriding both properties in JDBC URL:
      //   hive.server2.tez.external.sessions.namespace=<prefix>/<llapName>
      //   tez.am.registry.namespace=/<llapName>
      //
      // Path relationship (matches Docker template convention):
      //   tez.am.registry.namespace = /<llapName>
      //   Tez registers session node at: <TEZ_EXTERNAL_SESSIONS_ZK_PREFIX>/<llapName>/<appId>
      //   hive.server2.tez.external.sessions.namespace = <TEZ_EXTERNAL_SESSIONS_ZK_PREFIX>/<llapName>
      String defaultRegistryNs = defaultLlapCluster(spec)
          .map(llap -> "/" + llap.name()).orElse("/default");
      String defaultNamespace = ConfigUtils.TEZ_EXTERNAL_SESSIONS_ZK_PREFIX + defaultRegistryNs;
      props.put(ConfigUtils.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE_KEY,
          defaultNamespace);
      props.put(ConfigUtils.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_REGISTRY_CLASS_KEY,
          "org.apache.hadoop.hive.ql.exec.tez.ZookeeperExternalSessionsRegistryClient");
      props.put(ConfigUtils.HIVE_ZOOKEEPER_QUORUM_KEY, zkQuorum);
      props.put(ConfigUtils.TEZ_AM_FRAMEWORK_MODE_KEY, "STANDALONE_ZOOKEEPER");
      props.put(ConfigUtils.TEZ_AM_REGISTRY_NAMESPACE_KEY, defaultRegistryNs);
      props.put(ConfigUtils.TEZ_AM_ZOOKEEPER_QUORUM_KEY, zkQuorum);
      defaultLlapCluster(spec).ifPresent(llap -> {
        props.put(ConfigUtils.HIVE_EXECUTION_MODE_KEY, "llap");
        props.put(ConfigUtils.HIVE_LLAP_EXECUTION_MODE_KEY, "all");
        props.put(ConfigUtils.HIVE_LLAP_DAEMON_SERVICE_HOSTS_KEY,
            llap.serviceHosts());
      });
    } else {
      props.put(ConfigUtils.HIVE_SERVER2_TEZ_USE_EXTERNAL_SESSIONS_KEY, "false");
      props.put(ConfigUtils.TEZ_LOCAL_MODE_KEY, "true");
      props.put(ConfigUtils.TEZ_AM_FRAMEWORK_MODE_KEY, "LOCAL");
      props.put("mapreduce.framework.name", "local");
    }

    // Server-side LLAP cluster routing: emit per-cluster definitions and routing rules.
    if (spec.llapClusterRouting() != null && !spec.llapClusterRouting().isEmpty()) {
      props.put(ConfigUtils.HIVE_LLAP_CLUSTER_ROUTING_RULES_KEY, spec.llapClusterRouting());
      for (LlapSpec llap : spec.llapClusters()) {
        if (!llap.isEnabled()) {
          continue;
        }
        String sessionsNs = ConfigUtils.TEZ_EXTERNAL_SESSIONS_ZK_PREFIX + "/" + llap.name();
        String registryNs = "/" + llap.name();
        props.put(ConfigUtils.HIVE_LLAP_CLUSTER_PREFIX + llap.name()
            + ConfigUtils.HIVE_LLAP_CLUSTER_SESSIONS_NS_SUFFIX, sessionsNs);
        props.put(ConfigUtils.HIVE_LLAP_CLUSTER_PREFIX + llap.name()
            + ConfigUtils.HIVE_LLAP_CLUSTER_REGISTRY_NS_SUFFIX, registryNs);
        props.put(ConfigUtils.HIVE_LLAP_CLUSTER_PREFIX + llap.name()
            + ConfigUtils.HIVE_LLAP_CLUSTER_SERVICE_HOSTS_SUFFIX, llap.serviceHosts());
      }
    }

    // Enable JMX metrics when autoscaling is active for HS2, OR if LLAP/TezAM rely on them.
    boolean llapOrTezAmAutoscales = spec.llapClusters().stream().anyMatch(
        l -> l.isEnabled() && (l.autoscaling().isEnabled()
            || (spec.tezAm().isEnabled() && l.tezAm().autoscaling().isEnabled())));
    if (spec.hiveServer2().autoscaling().isEnabled() || llapOrTezAmAutoscales) {
      props.put("hive.server2.metrics.enabled", "true");
      props.put("hive.server2.metrics.reporter", "JMX");
    }

    if (spec.hiveServer2().configOverrides() != null) {
      props.putAll(spec.hiveServer2().configOverrides());
    }
    return props;
  }

  /** Builds tez-site.xml properties for HiveServer2 (uses first LLAP cluster as default). */
  public static Map<String, String> getTezSite(HiveClusterSpec spec) {
    return getTezSite(spec, defaultLlapCluster(spec).orElse(null));
  }

  /** Builds tez-site.xml properties for a specific LLAP cluster's TezAM. */
  public static Map<String, String> getTezSite(HiveClusterSpec spec, LlapSpec llap) {
    boolean tezAmEnabled = spec.tezAm().isEnabled();
    String zkQuorum = spec.zookeeper().quorum();

    Map<String, String> tezProps = new LinkedHashMap<>();
    tezProps.put(ConfigUtils.TEZ_AM_SESSION_MODE_KEY, "true");
    tezProps.put(ConfigUtils.TEZ_IGNORE_LIB_URIS_KEY, "true");
    tezProps.put(ConfigUtils.TEZ_AM_WEBSERVICE_ENABLE_KEY, "false");
    tezProps.put(ConfigUtils.TEZ_AM_DISABLE_CLIENT_VERSION_CHECK_KEY, "true");
    tezProps.put(ConfigUtils.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS_KEY, "-1");
    tezProps.put(ConfigUtils.TEZ_AM_ZOOKEEPER_QUORUM_KEY, zkQuorum);
    tezProps.put(ConfigUtils.HIVE_ZOOKEEPER_QUORUM_KEY, zkQuorum);
    if (tezAmEnabled) {
      tezProps.put(ConfigUtils.TEZ_LOCAL_MODE_KEY, "false");
      tezProps.put(ConfigUtils.TEZ_AM_FRAMEWORK_MODE_KEY, "STANDALONE_ZOOKEEPER");
      // Per-LLAP-cluster TezAM: each registers under its own ZK namespace.
      // tez.am.registry.namespace is the path WITHIN Tez's "tez-external-sessions"
      // Curator namespace (chroot). The absolute ZK path becomes:
      //   /tez-external-sessions/<registryNamespace>/<applicationId>
      String registryNamespace = llap != null
          ? "/" + llap.name() : "/default";
      tezProps.put(ConfigUtils.TEZ_AM_REGISTRY_NAMESPACE_KEY, registryNamespace);
    } else {
      tezProps.put(ConfigUtils.TEZ_LOCAL_MODE_KEY, "true");
    }

    if (llap != null) {
      tezProps.put(ConfigUtils.HIVE_LLAP_DAEMON_SERVICE_HOSTS_KEY,
          llap.serviceHosts());
    }

    // Required by LlapTaskCommunicator — Tez's Configuration doesn't get HiveConf defaults
    tezProps.put(ConfigUtils.HIVE_LLAP_DAEMON_UMBILICAL_PORT_KEY,
        ConfigUtils.HIVE_LLAP_DAEMON_UMBILICAL_PORT_DEFAULT);

    if (spec.tezAm().configOverrides() != null) {
      tezProps.putAll(spec.tezAm().configOverrides());
    }
    return tezProps;
  }

  /** Builds core-site.xml properties from hadoop.coreSiteOverrides. */
  public static Map<String, String> getHadoopCoreSite(HiveClusterSpec spec) {
    Map<String, String> props = new LinkedHashMap<>();
    HadoopSpec hadoop = spec.hadoop();
    if (hadoop != null && hadoop.coreSiteOverrides() != null) {
      props.putAll(hadoop.coreSiteOverrides());
    }
    return props;
  }

  /** Builds metastore-site.xml properties. */
  public static Map<String, String> getMetastoreSite(HiveClusterSpec spec) {
    MetastoreSpec metastore = spec.metastore();
    Map<String, String> props = new LinkedHashMap<>();

    // HTTP transport mode: stateless connections allow safe scale-down
    // without breaking active client connections.
    props.put(ConfigUtils.METASTORE_SERVER_TRANSPORT_MODE_KEY,
        ConfigUtils.METASTORE_SERVER_TRANSPORT_MODE_DEFAULT);
    props.put(ConfigUtils.METASTORE_SERVER_HTTP_PATH_KEY,
        ConfigUtils.METASTORE_SERVER_HTTP_PATH_DEFAULT);

    props.put(ConfigUtils.METASTORE_WAREHOUSE_KEY,
        metastore.warehouseDir());

    DatabaseConfig db = metastore.database();
    if (db != null) {
      if (db.url() != null) {
        props.put(ConfigUtils.METASTORE_CONNECTION_URL_KEY, db.url());
      }
      if (db.driver() != null) {
        props.put(ConfigUtils.METASTORE_CONNECTION_DRIVER_KEY, db.driver());
      }
      if (db.username() != null) {
        props.put(ConfigUtils.METASTORE_CONNECTION_USER_KEY, db.username());
      }
    }

    // Enable JMX metrics when autoscaling is active.
    // The Prometheus JMX Exporter agent reads JMX MBeans and exposes them
    // in Prometheus text format at /metrics on the metrics port.
    if (metastore.autoscaling().isEnabled()) {
      props.put("metastore.metrics.enabled", "true");
      props.put("metastore.metrics.reporter", "JMX");
    }

    if (metastore.configOverrides() != null) {
      props.putAll(metastore.configOverrides());
    }
    return props;
  }

  /** Builds llap-daemon-site.xml properties for a specific LLAP cluster. */
  public static Map<String, String> getLlapDaemonSite(HiveClusterSpec spec, LlapSpec llap) {
    Map<String, String> props = new LinkedHashMap<>();

    props.put(ConfigUtils.HIVE_LLAP_DAEMON_MEMORY_MB_KEY,
        String.valueOf(llap.memoryMb()));
    props.put(ConfigUtils.HIVE_LLAP_DAEMON_NUM_EXECUTORS_KEY,
        String.valueOf(llap.executors()));
    props.put(ConfigUtils.HIVE_LLAP_DAEMON_SERVICE_HOSTS_KEY,
        llap.serviceHosts());
    props.put(ConfigUtils.HIVE_ZOOKEEPER_QUORUM_KEY,
        spec.zookeeper().quorum());

    if (llap.configOverrides() != null) {
      props.putAll(llap.configOverrides());
    }
    return props;
  }

  /** Returns the first enabled LLAP cluster (used as the default for HS2/TezAM config). */
  private static Optional<LlapSpec> defaultLlapCluster(HiveClusterSpec spec) {
    return spec.llapClusters().stream()
        .filter(LlapSpec::isEnabled)
        .findFirst();
  }
}
