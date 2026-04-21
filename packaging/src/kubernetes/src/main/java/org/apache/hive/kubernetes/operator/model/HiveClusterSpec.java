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

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.apache.hive.kubernetes.operator.model.spec.HadoopSpec;
import org.apache.hive.kubernetes.operator.model.spec.HiveServer2Spec;
import org.apache.hive.kubernetes.operator.model.spec.LlapSpec;
import org.apache.hive.kubernetes.operator.model.spec.MetastoreSpec;
import org.apache.hive.kubernetes.operator.model.spec.StorageSpec;
import org.apache.hive.kubernetes.operator.model.spec.TezAmSpec;
import org.apache.hive.kubernetes.operator.model.spec.ZookeeperSpec;

/** Full specification for a HiveCluster custom resource. */
public class HiveClusterSpec {

  @JsonPropertyDescription("Docker image to use for all Hive components")
  private String image = "apache/hive:4.3.0-SNAPSHOT";

  @JsonPropertyDescription("Image pull policy: Always, Never, or IfNotPresent")
  private String imagePullPolicy = "IfNotPresent";

  @JsonPropertyDescription("Metastore component configuration")
  private MetastoreSpec metastore = new MetastoreSpec();

  @JsonPropertyDescription("HiveServer2 component configuration")
  private HiveServer2Spec hiveServer2 = new HiveServer2Spec();

  @JsonPropertyDescription("LLAP daemon configuration. Disabled by default.")
  private LlapSpec llap = new LlapSpec();

  @JsonPropertyDescription("Tez Application Master configuration. Disabled by default.")
  private TezAmSpec tezAm = new TezAmSpec();

  @JsonPropertyDescription(
      "External ZooKeeper connection details (not managed by this operator)")
  private ZookeeperSpec zookeeper = new ZookeeperSpec();

  @JsonPropertyDescription(
      "S3-compatible storage backend configuration. Supports any "
      + "S3-compatible endpoint (Apache Ozone, MinIO, AWS S3, etc.).")
  private StorageSpec storage;

  @JsonPropertyDescription(
      "Hadoop/core-site.xml configuration overrides (e.g. S3A settings)")
  private HadoopSpec hadoop = new HadoopSpec();

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public String getImagePullPolicy() {
    return imagePullPolicy;
  }

  public void setImagePullPolicy(String imagePullPolicy) {
    this.imagePullPolicy = imagePullPolicy;
  }

  public MetastoreSpec getMetastore() {
    return metastore;
  }

  public void setMetastore(MetastoreSpec metastore) {
    this.metastore = metastore;
  }

  public HiveServer2Spec getHiveServer2() {
    return hiveServer2;
  }

  public void setHiveServer2(HiveServer2Spec hiveServer2) {
    this.hiveServer2 = hiveServer2;
  }

  public LlapSpec getLlap() {
    return llap;
  }

  public void setLlap(LlapSpec llap) {
    this.llap = llap;
  }

  public TezAmSpec getTezAm() {
    return tezAm;
  }

  public void setTezAm(TezAmSpec tezAm) {
    this.tezAm = tezAm;
  }

  public ZookeeperSpec getZookeeper() {
    return zookeeper;
  }

  public void setZookeeper(ZookeeperSpec zookeeper) {
    this.zookeeper = zookeeper;
  }

  public StorageSpec getStorage() {
    return storage;
  }

  public void setStorage(StorageSpec storage) {
    this.storage = storage;
  }

  public HadoopSpec getHadoop() {
    return hadoop;
  }

  public void setHadoop(HadoopSpec hadoop) {
    this.hadoop = hadoop;
  }
}
