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

package org.apache.hive.kubernetes.operator.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import io.fabric8.crd.generator.annotation.PreserveUnknownFields;
import io.fabric8.crd.generator.annotation.SchemaFrom;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import org.apache.hive.kubernetes.operator.model.spec.HadoopSpec;
import org.apache.hive.kubernetes.operator.model.spec.HiveServer2Spec;
import org.apache.hive.kubernetes.operator.model.spec.LlapSpec;
import org.apache.hive.kubernetes.operator.model.spec.MetastoreSpec;
import org.apache.hive.kubernetes.operator.model.spec.TezAmSpec;
import org.apache.hive.kubernetes.operator.model.spec.ZookeeperSpec;

/** Full specification for a HiveCluster custom resource. */
public record HiveClusterSpec(
    @JsonPropertyDescription("Docker image to use for all Hive components")
    String image,
    @JsonPropertyDescription("Image pull policy: Always, Never, or IfNotPresent")
    String imagePullPolicy,
    @JsonPropertyDescription("Metastore component configuration")
    MetastoreSpec metastore,
    @JsonPropertyDescription("HiveServer2 component configuration")
    HiveServer2Spec hiveServer2,
    @JsonPropertyDescription("LLAP daemon configuration. Disabled by default.")
    LlapSpec llap,
    @JsonPropertyDescription("Tez Application Master configuration. Disabled by default.")
    TezAmSpec tezAm,
    @JsonPropertyDescription(
        "External ZooKeeper connection details (not managed by this operator)")
    ZookeeperSpec zookeeper,
    @JsonPropertyDescription("Hadoop/core-site.xml configuration overrides")
    HadoopSpec hadoop,
    @JsonPropertyDescription(
        "Environment variables injected into all component pods "
        + "(e.g., storage credentials, custom JVM options)")
    @SchemaFrom(type = Object[].class) @PreserveUnknownFields
    List<EnvVar> envVars,
    @JsonPropertyDescription(
        "External JARs (URLs) downloaded into all component pods and added to "
        + "HADOOP_CLASSPATH (e.g., GCS connector, ABFS connector)")
    List<String> externalJars,
    @JsonPropertyDescription(
        "Volumes added to all component pods "
        + "(e.g., Secrets containing keytabs or service account keys)")
    @SchemaFrom(type = Object[].class) @PreserveUnknownFields
    List<Volume> volumes,
    @JsonPropertyDescription(
        "Volume mounts added to all component containers "
        + "(e.g., mounting a GCS key file at /etc/gcs/key.json)")
    @SchemaFrom(type = Object[].class) @PreserveUnknownFields
    List<VolumeMount> volumeMounts) {

  public HiveClusterSpec {
    image = image != null ? image : "apache/hive:4.3.0-SNAPSHOT";
    imagePullPolicy = imagePullPolicy != null ? imagePullPolicy : "IfNotPresent";
    metastore = metastore != null ?
        metastore :
        new MetastoreSpec(null, null, null, null, null, null, null, null, null, null, null);
    hiveServer2 = hiveServer2 != null ?
        hiveServer2 :
        new HiveServer2Spec(null, null, null, null, null, null, null, null, null, null, null);
    llap = llap != null ? llap : new LlapSpec(null, null, null, null, null, null, null, null, null, null);
    tezAm = tezAm != null ? tezAm : new TezAmSpec(null, null, null, null, null, null, null, null);
    zookeeper = zookeeper != null ? zookeeper : new ZookeeperSpec(null);
    hadoop = hadoop != null ? hadoop : new HadoopSpec(null);
    envVars = envVars != null ? envVars : List.of();
    externalJars = externalJars != null ? externalJars : List.of();
    volumes = volumes != null ? volumes : List.of();
    volumeMounts = volumeMounts != null ? volumeMounts : List.of();
  }
}
