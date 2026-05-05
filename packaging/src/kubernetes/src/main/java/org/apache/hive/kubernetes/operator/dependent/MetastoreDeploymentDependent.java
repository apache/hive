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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Probe;
import io.fabric8.kubernetes.api.model.ProbeBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.DatabaseConfig;
import org.apache.hive.kubernetes.operator.model.spec.StorageSpec;
import org.apache.hive.kubernetes.operator.model.spec.SecretKeyRef;
import org.apache.hive.kubernetes.operator.util.Labels;

/** Manages the Kubernetes Deployment for the Hive Metastore. */
@KubernetesDependent(
    labelSelector = "app.kubernetes.io/component=metastore,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator"
)
public class MetastoreDeploymentDependent
    extends HiveDependentResource<Deployment, HiveCluster> {

  public static final String COMPONENT = "metastore";
  private static final String CONF_MOUNT_PATH = "/etc/hive/conf";
  private static final String HADOOP_CONF_MOUNT_PATH = "/etc/hadoop/conf";
  private static final String EXT_JARS_PATH = "/tmp/ext-jars";

  public MetastoreDeploymentDependent() {
    super(Deployment.class);
  }

  @Override
  protected Deployment desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    DatabaseConfig db = spec.getMetastore().getDatabase();
    Map<String, String> selectorLabels =
        Labels.selectorForComponent(hiveCluster, COMPONENT);

    List<EnvVar> envVars = buildEnvVars(db, spec.getStorage());
    boolean hasDriverJar = db.getDriverJarUrl() != null;

    List<ContainerPort> ports = List.of(
        new ContainerPortBuilder()
            .withName("thrift").withContainerPort(9083).build(),
        new ContainerPortBuilder()
            .withName("rest").withContainerPort(9001).build()
    );

    Probe readinessProbe = new ProbeBuilder()
        .withNewTcpSocket()
          .withPort(new IntOrString(9083))
        .endTcpSocket()
        .withInitialDelaySeconds(15)
        .withPeriodSeconds(10)
        .withFailureThreshold(3)
        .build();

    Probe livenessProbe = new ProbeBuilder()
        .withNewTcpSocket()
          .withPort(new IntOrString(9083))
        .endTcpSocket()
        .withInitialDelaySeconds(60)
        .withPeriodSeconds(30)
        .withFailureThreshold(5)
        .build();

    // Init container to download JDBC driver if URL is provided
    List<Container> initContainers = new ArrayList<>();
    if (hasDriverJar) {
      initContainers.add(new ContainerBuilder()
          .withName("download-jdbc-driver")
          .withImage(spec.getImage())
          .withCommand("/bin/bash", "-c",
              "wget -q -O " + EXT_JARS_PATH
              + "/jdbc-driver.jar '"
              + db.getDriverJarUrl() + "'")
          .withVolumeMounts(new VolumeMountBuilder()
              .withName("ext-jars")
              .withMountPath(EXT_JARS_PATH)
              .build())
          .build());
    }

    // Volume mounts for the main container
    List<VolumeMount> volumeMounts = new ArrayList<>();
    volumeMounts.add(new VolumeMountBuilder()
        .withName("hive-config")
        .withMountPath(CONF_MOUNT_PATH).build());
    if (hasDriverJar) {
      // Entrypoint copies jars from /tmp/ext-jars to $HIVE_HOME/lib
      volumeMounts.add(new VolumeMountBuilder()
          .withName("ext-jars")
          .withMountPath(EXT_JARS_PATH).build());
    }

    // Projected volume merges metastore + hadoop ConfigMaps into
    // one mount so the entrypoint's HIVE_CUSTOM_CONF_DIR symlink
    // picks up both hive-site.xml and core-site.xml.
    List<Volume> volumes = new ArrayList<>();
    volumes.add(new VolumeBuilder()
        .withName("hive-config")
        .withNewProjected()
          .addNewSource()
            .withNewConfigMap()
              .withName(MetastoreConfigMapDependent
                  .resourceName(hiveCluster))
            .endConfigMap()
          .endSource()
          .addNewSource()
            .withNewConfigMap()
              .withName(HadoopConfigMapDependent
                  .resourceName(hiveCluster))
            .endConfigMap()
          .endSource()
        .endProjected().build());
    if (hasDriverJar) {
      volumes.add(new VolumeBuilder()
          .withName("ext-jars")
          .withNewEmptyDir().endEmptyDir().build());
    }

    return new DeploymentBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .withNewSpec()
          .withReplicas(spec.getMetastore().getReplicas())
          .withNewSelector()
            .withMatchLabels(selectorLabels)
          .endSelector()
          .withNewTemplate()
            .withNewMetadata()
              .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
            .endMetadata()
            .withNewSpec()
              .withInitContainers(initContainers)
              .addNewContainer()
                .withName("metastore")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withEnv(envVars)
                .withPorts(ports)
                .withReadinessProbe(readinessProbe)
                .withLivenessProbe(livenessProbe)
                .withResources(buildResources(
                    spec.getMetastore().getResources()))
                .withVolumeMounts(volumeMounts)
              .endContainer()
              .withVolumes(volumes)
            .endSpec()
          .endTemplate()
        .endSpec()
        .build();
  }

  private List<EnvVar> buildEnvVars(DatabaseConfig db,
      StorageSpec storage) {
    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", "metastore", null));
    envVars.add(new EnvVar("IS_RESUME", "true", null));
    envVars.add(new EnvVar("DB_DRIVER", db.getType(), null));
    envVars.add(new EnvVar("HIVE_CUSTOM_CONF_DIR",
        CONF_MOUNT_PATH, null));
    envVars.add(new EnvVar("HADOOP_CLASSPATH",
        "/opt/hadoop/share/hadoop/tools/lib/*", null));

    // S3A credentials as env vars — supports SecretKeyRef or plain-text.
    envVars.addAll(buildS3CredentialEnvVars(storage));

    // DBPASSWORD must be defined before SERVICE_OPTS so that
    // Kubernetes $(DBPASSWORD) interpolation resolves correctly.
    SecretKeyRef passwordRef = db.getPasswordSecretRef();
    if (passwordRef != null) {
      envVars.add(new EnvVarBuilder()
          .withName("DBPASSWORD")
          .withNewValueFrom()
            .withNewSecretKeyRef()
              .withName(passwordRef.getName())
              .withKey(passwordRef.getKey())
            .endSecretKeyRef()
          .endValueFrom()
          .build());
    }

    StringBuilder serviceOpts = new StringBuilder();
    if (db.getUrl() != null) {
      serviceOpts.append("-Djavax.jdo.option.ConnectionURL=")
          .append(db.getUrl());
    }
    if (db.getDriver() != null) {
      serviceOpts.append(" -Djavax.jdo.option.ConnectionDriverName=")
          .append(db.getDriver());
    }
    if (db.getUsername() != null) {
      serviceOpts.append(" -Djavax.jdo.option.ConnectionUserName=")
          .append(db.getUsername());
    }
    if (passwordRef != null) {
      // $(DBPASSWORD) is resolved by Kubernetes from the env var above
      serviceOpts.append(
          " -Djavax.jdo.option.ConnectionPassword=$(DBPASSWORD)");
    }
    if (serviceOpts.length() > 0) {
      envVars.add(new EnvVar("SERVICE_OPTS",
          serviceOpts.toString().trim(), null));
    }

    return envVars;
  }

  /** Returns the Deployment resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-metastore";
  }
}
