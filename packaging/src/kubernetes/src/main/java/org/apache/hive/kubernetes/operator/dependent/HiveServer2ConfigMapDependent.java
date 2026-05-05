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

package org.apache.hive.kubernetes.operator.dependent;

import java.util.LinkedHashMap;
import java.util.Map;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.LlapSpec;
import org.apache.hive.kubernetes.operator.model.spec.StorageSpec;
import org.apache.hive.kubernetes.operator.util.HadoopXmlBuilder;
import org.apache.hive.kubernetes.operator.util.Labels;

/** Manages the hive-site.xml ConfigMap for HiveServer2. */
@KubernetesDependent(
    labelSelector = "app.kubernetes.io/component=hiveserver2,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator"
)
public class HiveServer2ConfigMapDependent
    extends HiveDependentResource<ConfigMap, HiveCluster> {

  public static final String COMPONENT = "hiveserver2";

  public HiveServer2ConfigMapDependent() {
    super(ConfigMap.class);
  }

  @Override
  protected ConfigMap desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    boolean tezAmEnabled = spec.getTezAm().isEnabled();
    String zkQuorum = spec.getZookeeper().getQuorum();

    // hive-site.xml properties
    Map<String, String> props = new LinkedHashMap<>();
    String metastoreUri = "thrift://"
        + hiveCluster.getMetadata().getName() + "-metastore:9083";
    props.put("hive.metastore.uris", metastoreUri);
    props.put("hive.server2.enable.doAs", "false");
    props.put("hive.tez.exec.inplace.progress", "false");
    props.put("hive.tez.exec.print.summary", "true");
    props.put("hive.jar.directory", "/tmp");
    props.put("hive.user.install.directory", "/tmp");
    props.put("hive.exec.local.scratchdir", "/opt/hive/scratch");
    if (tezAmEnabled && spec.getLlap().isEnabled()
        && spec.getStorage() != null) {
      props.put("hive.exec.scratchdir",
          "s3a://" + spec.getStorage().getBucket() + "/scratch");
    } else {
      props.put("hive.exec.scratchdir", "/opt/hive/scratch");
    }
    props.put("hive.exec.submit.local.task.via.child", "false");
    props.put("tez.runtime.optimize.local.fetch", "true");

    // S3A endpoint config in hive-site.xml so it is serialized
    // into the Tez DAG configuration for task execution.
    // Credentials are provided via AWS_ACCESS_KEY_ID /
    // AWS_SECRET_ACCESS_KEY env vars on the container.
    StorageSpec storage = spec.getStorage();
    if (storage != null && storage.getEndpoint() != null) {
      props.put("fs.s3a.endpoint", storage.getEndpoint());
      props.put("fs.s3a.path.style.access",
          String.valueOf(storage.isPathStyleAccess()));
      props.put("fs.s3a.impl",
          "org.apache.hadoop.fs.s3a.S3AFileSystem");
    }

    if (tezAmEnabled) {
      props.put("hive.server2.tez.use.external.sessions", "true");
      props.put("hive.server2.tez.external.sessions.namespace",
          "/tez-external-sessions/tez_am/server");
      props.put("hive.server2.tez.external.sessions.registry.class",
          "org.apache.hadoop.hive.ql.exec.tez."
          + "ZookeeperExternalSessionsRegistryClient");
      props.put("hive.zookeeper.quorum", zkQuorum);
      if (spec.getLlap().isEnabled()) {
        props.put("hive.execution.mode", "llap");
        props.put("hive.llap.execution.mode", "all");
        props.put("hive.llap.daemon.service.hosts",
            spec.getLlap().getServiceHosts());
      }
    } else {
      props.put("hive.server2.tez.use.external.sessions", "false");
      props.put("tez.local.mode", "true");
      props.put("mapreduce.framework.name", "local");
    }

    if (spec.getHiveServer2().getConfigOverrides() != null) {
      props.putAll(spec.getHiveServer2().getConfigOverrides());
    }

    // tez-site.xml properties
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

    // LLAP properties required by the Tez AM's
    // service_plugins_descriptor.json (LlapTaskCommunicator,
    // LlapTaskSchedulerService).
    LlapSpec llap = spec.getLlap();
    if (llap.isEnabled()) {
      tezProps.put("hive.llap.daemon.service.hosts",
          llap.getServiceHosts());
      tezProps.put("hive.llap.daemon.umbilical.port", "33333");
    }

    return new ConfigMapBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .addToData("hive-site.xml", HadoopXmlBuilder.buildXml(props))
        .addToData("tez-site.xml", HadoopXmlBuilder.buildXml(tezProps))
        .build();
  }

  /** Returns the ConfigMap resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-hiveserver2-config";
  }
}
