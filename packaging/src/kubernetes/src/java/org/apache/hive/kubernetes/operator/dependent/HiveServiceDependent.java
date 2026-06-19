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

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.config.informer.Informer;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Unified Kubernetes Service dependent for all Hive components.
 * Subclassed per component to define component-specific service type and ports.
 */
public abstract class HiveServiceDependent
    extends HiveDependentResource<Service, HiveCluster> {

  private final String component;

  protected HiveServiceDependent(String component) {
    super(Service.class);
    this.component = component;
  }

  @Override
  protected String getSecondaryResourceName(HiveCluster primary,
      Context<HiveCluster> context) {
    return primary.getMetadata().getName() + "-" + component;
  }

  @Override
  protected Service desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    ServiceBuilder builder = new ServiceBuilder()
        .withNewMetadata()
          .withName(hiveCluster.getMetadata().getName() + "-" + component)
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, component))
        .endMetadata()
        .withNewSpec()
          .withSelector(Labels.selectorForComponent(hiveCluster, component))
        .endSpec();
    customizeSpec(builder, hiveCluster);
    return builder.build();
  }

  /** Subclasses override to set service type and add ports. */
  protected abstract void customizeSpec(ServiceBuilder builder, HiveCluster hiveCluster);

  /** HiveServer2 Service: configurable type, thrift + http + webui ports. */
  @KubernetesDependent(
      informer = @Informer(labelSelector = "app.kubernetes.io/component=hiveserver2,"
          + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
  )
  public static class HiveServer2 extends HiveServiceDependent {
    public HiveServer2() {
      super(ConfigUtils.COMPONENT_HIVESERVER2);
    }

    @Override
    protected void customizeSpec(ServiceBuilder builder, HiveCluster hiveCluster) {
      var hs2 = hiveCluster.getSpec().hiveServer2();
      int thriftPort = ConfigUtils.getInt(hs2.configOverrides(),
          ConfigUtils.HIVE_SERVER2_THRIFT_PORT_KEY,
          null, ConfigUtils.HIVE_SERVER2_THRIFT_PORT_DEFAULT);
      int httpPort = ConfigUtils.getInt(hs2.configOverrides(),
          ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PORT_KEY,
          null, ConfigUtils.HIVE_SERVER2_THRIFT_HTTP_PORT_DEFAULT);
      int webUiPort = ConfigUtils.getInt(hs2.configOverrides(),
          ConfigUtils.HIVE_SERVER2_WEBUI_PORT_KEY,
          null, ConfigUtils.HIVE_SERVER2_WEBUI_PORT_DEFAULT);
      builder.editSpec()
          .withType(hs2.serviceType())
          .withSessionAffinity("ClientIP")
          .addNewPort().withName("thrift").withProtocol("TCP")
            .withPort(thriftPort).withTargetPort(new IntOrString(thriftPort)).endPort()
          .addNewPort().withName("http").withProtocol("TCP")
            .withPort(httpPort).withTargetPort(new IntOrString(httpPort)).endPort()
          .addNewPort().withName("webui").withProtocol("TCP")
            .withPort(webUiPort).withTargetPort(new IntOrString(webUiPort)).endPort()
          .endSpec();
    }
  }

  /** Metastore Service: ClusterIP, thrift + rest ports. */
  @KubernetesDependent(
      informer = @Informer(labelSelector = "app.kubernetes.io/component=metastore,"
          + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
  )
  public static class Metastore extends HiveServiceDependent {
    public Metastore() {
      super(ConfigUtils.COMPONENT_METASTORE);
    }

    @Override
    protected void customizeSpec(ServiceBuilder builder, HiveCluster hiveCluster) {
      var overrides = hiveCluster.getSpec().metastore().configOverrides();
      int thriftPort = ConfigUtils.getInt(overrides,
          ConfigUtils.METASTORE_THRIFT_PORT_KEY,
          ConfigUtils.METASTORE_THRIFT_PORT_HIVE_KEY,
          ConfigUtils.METASTORE_THRIFT_PORT_DEFAULT);
      int restPort = ConfigUtils.getInt(overrides,
          ConfigUtils.METASTORE_REST_HTTP_PORT_KEY,
          null, ConfigUtils.METASTORE_REST_HTTP_PORT_DEFAULT);
      builder.editSpec()
          .withType("ClusterIP")
          .addNewPort().withName("thrift").withProtocol("TCP")
            .withPort(thriftPort).withTargetPort(new IntOrString(thriftPort)).endPort()
          .addNewPort().withName("rest").withProtocol("TCP")
            .withPort(restPort).withTargetPort(new IntOrString(restPort)).endPort()
          .endSpec();
    }
  }

  /** LLAP headless Service: required by StatefulSet for stable DNS. */
  @KubernetesDependent(
      informer = @Informer(labelSelector = "app.kubernetes.io/component=llap,"
          + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
  )
  public static class Llap extends HiveServiceDependent {
    public Llap() {
      super(ConfigUtils.COMPONENT_LLAP);
    }

    @Override
    protected void customizeSpec(ServiceBuilder builder, HiveCluster hiveCluster) {
      var overrides = hiveCluster.getSpec().llap().configOverrides();
      int managementPort = ConfigUtils.getInt(overrides,
          ConfigUtils.HIVE_LLAP_MANAGEMENT_RPC_PORT_KEY, null,
          ConfigUtils.HIVE_LLAP_MANAGEMENT_RPC_PORT_DEFAULT);
      int shufflePort = ConfigUtils.getInt(overrides,
          ConfigUtils.HIVE_LLAP_DAEMON_SHUFFLE_PORT_KEY, null,
          ConfigUtils.HIVE_LLAP_DAEMON_SHUFFLE_PORT_DEFAULT);
      int webPort = ConfigUtils.getInt(overrides,
          ConfigUtils.HIVE_LLAP_DAEMON_WEB_PORT_KEY, null,
          ConfigUtils.HIVE_LLAP_DAEMON_WEB_PORT_DEFAULT);
      builder.editSpec()
          .withClusterIP("None")
          .addNewPort().withName("management").withProtocol("TCP")
            .withPort(managementPort).withTargetPort(new IntOrString(managementPort)).endPort()
          .addNewPort().withName("shuffle").withProtocol("TCP")
            .withPort(shufflePort).withTargetPort(new IntOrString(shufflePort)).endPort()
          .addNewPort().withName("web").withProtocol("TCP")
            .withPort(webPort).withTargetPort(new IntOrString(webPort)).endPort()
          .endSpec();
    }
  }

  /** TezAM headless Service: required by StatefulSet for stable DNS. */
  @KubernetesDependent(
      informer = @Informer(labelSelector = "app.kubernetes.io/component=tezam,"
          + "app.kubernetes.io/managed-by=hive-kubernetes-operator")
  )
  public static class TezAm extends HiveServiceDependent {
    public TezAm() {
      super(ConfigUtils.COMPONENT_TEZAM);
    }

    @Override
    protected void customizeSpec(ServiceBuilder builder, HiveCluster hiveCluster) {
      builder.editSpec()
          .withClusterIP("None")
          .endSpec();
    }
  }
}
