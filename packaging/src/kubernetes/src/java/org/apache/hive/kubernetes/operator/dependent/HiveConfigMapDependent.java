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

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;

import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.util.HadoopXmlBuilder;
import org.apache.hive.kubernetes.operator.util.HiveConfigBuilder;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Unified ConfigMap dependent resource for all Hive component configurations.
 * Subclassed per component to define the specific XML data and label selector.
 */
public abstract class HiveConfigMapDependent extends HiveDependentResource<ConfigMap, HiveCluster> {

  private final String component;
  private final String suffix;

  protected HiveConfigMapDependent(String component, String suffix) {
    super(ConfigMap.class);
    this.component = component;
    this.suffix = suffix;
  }

  @Override
  protected String getSecondaryResourceName(HiveCluster primary, Context<HiveCluster> context) {
    return primary.getMetadata().getName() + "-" + suffix;
  }

  @Override
  protected ConfigMap desired(HiveCluster hiveCluster, Context<HiveCluster> context) {
    ConfigMapBuilder builder =
        new ConfigMapBuilder().withNewMetadata().withName(hiveCluster.getMetadata().getName() + "-" + suffix)
            .withNamespace(hiveCluster.getMetadata().getNamespace())
            .withLabels(Labels.forComponent(hiveCluster, component)).endMetadata();
    addData(builder, hiveCluster);
    return builder.build();
  }

  /**
   * Subclasses add their specific XML data entries.
   */
  protected abstract void addData(ConfigMapBuilder builder, HiveCluster hiveCluster);

  /**
   * Hadoop core-site.xml ConfigMap for filesystem configuration.
   */
  @KubernetesDependent(informer = @Informer(labelSelector = "app.kubernetes.io/component=hadoop-config,"
      + "app.kubernetes.io/managed-by=hive-kubernetes-operator"))
  public static class Hadoop extends HiveConfigMapDependent {
    public Hadoop() {
      super("hadoop-config", "hadoop-config");
    }

    @Override
    protected void addData(ConfigMapBuilder builder, HiveCluster hiveCluster) {
      builder.addToData("core-site.xml",
          HadoopXmlBuilder.buildXml(HiveConfigBuilder.getHadoopCoreSite(hiveCluster.getSpec())));
    }

    public static String resourceName(HiveCluster hiveCluster) {
      return hiveCluster.getMetadata().getName() + "-hadoop-config";
    }
  }

  /**
   * Metastore metastore-site.xml ConfigMap.
   */
  @KubernetesDependent(informer = @Informer(labelSelector = "app.kubernetes.io/component=metastore,"
      + "app.kubernetes.io/managed-by=hive-kubernetes-operator"))
  public static class Metastore extends HiveConfigMapDependent {
    public Metastore() {
      super("metastore", "metastore-config");
    }

    @Override
    protected void addData(ConfigMapBuilder builder, HiveCluster hiveCluster) {
      builder.addToData("metastore-site.xml",
          HadoopXmlBuilder.buildXml(HiveConfigBuilder.getMetastoreSite(hiveCluster.getSpec())));
    }

    public static String resourceName(HiveCluster hiveCluster) {
      return hiveCluster.getMetadata().getName() + "-metastore-config";
    }
  }

  /**
   * HiveServer2 hive-site.xml + tez-site.xml ConfigMap.
   */
  @KubernetesDependent(informer = @Informer(labelSelector = "app.kubernetes.io/component=hiveserver2,"
      + "app.kubernetes.io/managed-by=hive-kubernetes-operator"))
  public static class HiveServer2 extends HiveConfigMapDependent {
    public HiveServer2() {
      super("hiveserver2", "hiveserver2-config");
    }

    @Override
    protected void addData(ConfigMapBuilder builder, HiveCluster hiveCluster) {
      HiveClusterSpec spec = hiveCluster.getSpec();
      builder.addToData("hive-site.xml",
          HadoopXmlBuilder.buildXml(HiveConfigBuilder.getHiveServer2HiveSite(hiveCluster, spec)));
      builder.addToData("tez-site.xml", HadoopXmlBuilder.buildXml(HiveConfigBuilder.getTezSite(spec)));
    }

    public static String resourceName(HiveCluster hiveCluster) {
      return hiveCluster.getMetadata().getName() + "-hiveserver2-config";
    }
  }

  /**
   * LLAP llap-daemon-site.xml ConfigMap.
   */
  @KubernetesDependent(informer = @Informer(labelSelector = "app.kubernetes.io/component=llap,"
      + "app.kubernetes.io/managed-by=hive-kubernetes-operator"))
  public static class Llap extends HiveConfigMapDependent {
    public Llap() {
      super("llap", "llap-config");
    }

    @Override
    protected void addData(ConfigMapBuilder builder, HiveCluster hiveCluster) {
      builder.addToData("llap-daemon-site.xml",
          HadoopXmlBuilder.buildXml(HiveConfigBuilder.getLlapDaemonSite(hiveCluster.getSpec())));
    }

    public static String resourceName(HiveCluster hiveCluster) {
      return hiveCluster.getMetadata().getName() + "-llap-config";
    }
  }
}
