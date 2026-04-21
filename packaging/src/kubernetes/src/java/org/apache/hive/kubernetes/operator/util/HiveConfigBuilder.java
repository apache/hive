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

    String metastoreUri = spec.metastore().isEnabled()
        ? "thrift://" + hiveCluster.getMetadata().getName() + "-metastore:9083"
        : spec.metastore().externalUri();
    if (metastoreUri != null && !metastoreUri.isEmpty()) {
      props.put("hive.metastore.uris", metastoreUri);
    }
    props.put("hive.metastore.warehouse.dir", spec.metastore().warehouseDir());
    props.put("hive.server2.enable.doAs", "false");
    props.put("hive.tez.exec.inplace.progress", "false");
    props.put("hive.tez.exec.print.summary", "true");
    props.put("hive.jar.directory", "/tmp");
    props.put("hive.user.install.directory", "/tmp");
    if (tezAmEnabled) {
      props.put("hive.exec.local.scratchdir", "/opt/hive/scratch");
    }

    if (tezAmEnabled) {
      props.put("hive.server2.tez.use.external.sessions", "true");
      props.put("hive.server2.tez.external.sessions.namespace",
          "/tez-external-sessions/tez_am/server");
      props.put("hive.server2.tez.external.sessions.registry.class",
          "org.apache.hadoop.hive.ql.exec.tez."
              + "ZookeeperExternalSessionsRegistryClient");
      props.put("hive.zookeeper.quorum", zkQuorum);
      props.put("tez.am.framework.mode", "STANDALONE_ZOOKEEPER");
      props.put("tez.am.registry.namespace", "/tez_am/server");
      props.put("tez.am.zookeeper.quorum", zkQuorum);
      LlapSpec llap = spec.llap();
      if (llap.isEnabled()) {
        props.put("hive.execution.mode", "llap");
        props.put("hive.llap.execution.mode", "all");
        props.put("hive.llap.daemon.service.hosts", llap.serviceHosts());
      }
    } else {
      props.put("hive.server2.tez.use.external.sessions", "false");
      props.put("tez.local.mode", "true");
      props.put("tez.am.framework.mode", "LOCAL");
      props.put("mapreduce.framework.name", "local");
    }

    if (spec.hiveServer2().configOverrides() != null) {
      props.putAll(spec.hiveServer2().configOverrides());
    }
    return props;
  }

  /** Builds tez-site.xml properties for HiveServer2 and TezAM. */
  public static Map<String, String> getTezSite(HiveClusterSpec spec) {
    boolean tezAmEnabled = spec.tezAm().isEnabled();
    String zkQuorum = spec.zookeeper().quorum();

    Map<String, String> tezProps = new LinkedHashMap<>();
    tezProps.put("tez.am.mode.session", "true");
    tezProps.put("tez.ignore.lib.uris", "true");
    tezProps.put("tez.am.tez-ui.webservice.enable", "false");
    tezProps.put("tez.am.disable.client-version-check", "true");
    tezProps.put("tez.session.am.dag.submit.timeout.secs", "-1");
    tezProps.put("tez.am.zookeeper.quorum", zkQuorum);
    tezProps.put("hive.zookeeper.quorum", zkQuorum);
    if (tezAmEnabled) {
      tezProps.put("tez.local.mode", "false");
      tezProps.put("tez.am.framework.mode", "STANDALONE_ZOOKEEPER");
      tezProps.put("tez.am.registry.namespace", "/tez_am/server");
    } else {
      tezProps.put("tez.local.mode", "true");
    }

    LlapSpec llap = spec.llap();
    if (llap.isEnabled()) {
      tezProps.put("hive.llap.daemon.service.hosts", llap.serviceHosts());
    }

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

    props.put("metastore.warehouse.dir", metastore.warehouseDir());

    DatabaseConfig db = metastore.database();
    if (db != null) {
      if (db.url() != null) {
        props.put("javax.jdo.option.ConnectionURL", db.url());
      }
      if (db.driver() != null) {
        props.put("javax.jdo.option.ConnectionDriverName", db.driver());
      }
      if (db.username() != null) {
        props.put("javax.jdo.option.ConnectionUserName", db.username());
      }
    }

    if (metastore.configOverrides() != null) {
      props.putAll(metastore.configOverrides());
    }
    return props;
  }

  /** Builds llap-daemon-site.xml properties. */
  public static Map<String, String> getLlapDaemonSite(HiveClusterSpec spec) {
    LlapSpec llap = spec.llap();
    Map<String, String> props = new LinkedHashMap<>();

    props.put("hive.llap.daemon.memory.per.instance.mb",
        String.valueOf(llap.memoryMb()));
    props.put("hive.llap.daemon.num.executors",
        String.valueOf(llap.executors()));
    props.put("hive.llap.daemon.service.hosts", llap.serviceHosts());
    props.put("hive.zookeeper.quorum", spec.zookeeper().quorum());

    if (llap.configOverrides() != null) {
      props.putAll(llap.configOverrides());
    }
    return props;
  }
}
