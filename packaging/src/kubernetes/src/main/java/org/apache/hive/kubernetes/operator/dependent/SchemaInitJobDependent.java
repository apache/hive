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

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.api.model.batch.v1.JobBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.processing.dependent.kubernetes.KubernetesDependent;
import org.apache.hive.kubernetes.operator.model.HiveCluster;
import org.apache.hive.kubernetes.operator.model.HiveClusterSpec;
import org.apache.hive.kubernetes.operator.model.spec.DatabaseConfig;
import org.apache.hive.kubernetes.operator.model.spec.SecretKeyRef;
import org.apache.hive.kubernetes.operator.util.Labels;

/**
 * Manages the Kubernetes Job that initializes or upgrades the Hive Metastore
 * database schema using schematool.
 */
@KubernetesDependent(
    labelSelector = "app.kubernetes.io/component=schema-init,"
        + "app.kubernetes.io/managed-by=hive-kubernetes-operator"
)
public class SchemaInitJobDependent
    extends HiveDependentResource<Job, HiveCluster> {

  public static final String COMPONENT = "schema-init";
  private static final String CONF_MOUNT_PATH = "/etc/hive/conf";
  private static final String EXT_JARS_PATH = "/tmp/ext-jars";

  public SchemaInitJobDependent() {
    super(Job.class);
  }

  @Override
  protected Job desired(HiveCluster hiveCluster,
      Context<HiveCluster> context) {
    HiveClusterSpec spec = hiveCluster.getSpec();
    DatabaseConfig db = spec.getMetastore().getDatabase();

    List<EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVar("SERVICE_NAME", "metastore", null));
    envVars.add(new EnvVar("IS_RESUME", "false", null));
    envVars.add(new EnvVar("DB_DRIVER", db.getType(), null));
    envVars.add(new EnvVar("HIVE_CUSTOM_CONF_DIR",
        CONF_MOUNT_PATH, null));

    if (db.getUrl() != null) {
      envVars.add(new EnvVar("SERVICE_OPTS",
          "-Djavax.jdo.option.ConnectionURL=" + db.getUrl()
          + (db.getUsername() != null
              ? " -Djavax.jdo.option.ConnectionUserName="
                  + db.getUsername()
              : "")
          + (db.getDriver() != null
              ? " -Djavax.jdo.option.ConnectionDriverName="
                  + db.getDriver()
              : ""),
          null));
    }

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

    boolean hasDriverJar = db.getDriverJarUrl() != null;

    // This Job runs schematool directly (not via the entrypoint),
    // so we must replicate the entrypoint's config setup:
    // 1. Symlink custom config files into HIVE_CONF_DIR
    // 2. Set HADOOP_CLIENT_OPTS to pass SERVICE_OPTS as JVM args
    // 3. Copy JDBC driver jar if downloaded by init container
    StringBuilder cmd = new StringBuilder();
    cmd.append("export HIVE_CONF_DIR=$HIVE_HOME/conf && ");
    cmd.append("if [ -d \"${HIVE_CUSTOM_CONF_DIR:-}\" ]; then ");
    cmd.append("find \"${HIVE_CUSTOM_CONF_DIR}\" -type f -exec ");
    cmd.append("ln -sfn {} \"${HIVE_CONF_DIR}\"/ \\; ; ");
    cmd.append("export HADOOP_CONF_DIR=$HIVE_CONF_DIR; fi && ");
    cmd.append("export HADOOP_CLIENT_OPTS="
        + "\"${HADOOP_CLIENT_OPTS:-} -Xmx1G ${SERVICE_OPTS:-}\" && ");
    if (hasDriverJar) {
      cmd.append("cp ").append(EXT_JARS_PATH)
          .append("/*.jar $HIVE_HOME/lib/ && ");
    }
    cmd.append("$HIVE_HOME/bin/schematool -dbType ")
        .append(db.getType())
        .append(" -initOrUpgradeSchema");
    if (passwordRef != null) {
      cmd.append(" -passWord \"$DBPASSWORD\"");
    }
    String schemaCommand = cmd.toString();

    // Build init containers
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

    // Build volume mounts for the main container
    List<VolumeMount> volumeMounts = new ArrayList<>();
    volumeMounts.add(new VolumeMountBuilder()
        .withName("metastore-config")
        .withMountPath(CONF_MOUNT_PATH).build());
    volumeMounts.add(new VolumeMountBuilder()
        .withName("hadoop-config")
        .withMountPath("/etc/hadoop/conf").build());
    if (hasDriverJar) {
      volumeMounts.add(new VolumeMountBuilder()
          .withName("ext-jars")
          .withMountPath(EXT_JARS_PATH).build());
    }

    // Build volumes
    List<Volume> volumes = new ArrayList<>();
    volumes.add(new VolumeBuilder()
        .withName("metastore-config")
        .withNewConfigMap()
          .withName(MetastoreConfigMapDependent
              .resourceName(hiveCluster))
        .endConfigMap().build());
    volumes.add(new VolumeBuilder()
        .withName("hadoop-config")
        .withNewConfigMap()
          .withName(HadoopConfigMapDependent
              .resourceName(hiveCluster))
        .endConfigMap().build());
    if (hasDriverJar) {
      volumes.add(new VolumeBuilder()
          .withName("ext-jars")
          .withNewEmptyDir().endEmptyDir().build());
    }

    return new JobBuilder()
        .withNewMetadata()
          .withName(resourceName(hiveCluster))
          .withNamespace(hiveCluster.getMetadata().getNamespace())
          .withLabels(Labels.forComponent(hiveCluster, COMPONENT))
        .endMetadata()
        .withNewSpec()
          .withBackoffLimit(3)
          .withNewTemplate()
            .withNewMetadata()
              .withLabels(Labels.forComponent(
                  hiveCluster, COMPONENT))
            .endMetadata()
            .withNewSpec()
              .withRestartPolicy("OnFailure")
              .withInitContainers(initContainers)
              .addNewContainer()
                .withName("schema-init")
                .withImage(spec.getImage())
                .withImagePullPolicy(spec.getImagePullPolicy())
                .withCommand("/bin/bash", "-c")
                .withArgs(schemaCommand)
                .withEnv(envVars)
                .withVolumeMounts(volumeMounts)
              .endContainer()
              .withVolumes(volumes)
            .endSpec()
          .endTemplate()
        .endSpec()
        .build();
  }

  /** Returns the Job resource name for this HiveCluster. */
  public static String resourceName(HiveCluster hiveCluster) {
    return hiveCluster.getMetadata().getName() + "-schema-init";
  }
}
