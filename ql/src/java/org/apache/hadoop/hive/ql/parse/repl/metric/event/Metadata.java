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
package org.apache.hadoop.hive.ql.parse.repl.metric.event;

/**
 * Class for defining the metadata info for replication metrics.
 */
public class Metadata {
  /**
   * Type of replication.
   */
  public enum ReplicationType {
    BOOTSTRAP,
    INCREMENTAL,
    PRE_OPTIMIZED_BOOTSTRAP,
    OPTIMIZED_BOOTSTRAP
  }

  private String dbName;
  private ReplicationType replicationType;
  private double replicatedDBSizeInKB;
  private String stagingDir;
  private long lastReplId;
  private String failoverMetadataLoc;
  private long failoverEventId;
  private String failoverEndPoint;
  private String failoverType;

  public Metadata() {

  }

  public Metadata(Metadata metadata) {
    this.dbName = metadata.dbName;
    this.replicationType = metadata.replicationType;
    this.replicatedDBSizeInKB = metadata.replicatedDBSizeInKB;
    this.stagingDir = metadata.stagingDir;
    this.lastReplId = metadata.lastReplId;
    this.failoverMetadataLoc = metadata.failoverMetadataLoc;
    this.failoverEventId = metadata.failoverEventId;
    this.failoverEndPoint = metadata.failoverEndPoint;
    this.failoverType = metadata.failoverType;
  }

  public Metadata(String dbName, ReplicationType replicationType, String stagingDir) {
    this.dbName = dbName;
    this.replicationType = replicationType;
    this.stagingDir = stagingDir;
  }

  public long getLastReplId() {
    return lastReplId;
  }

  public String getDbName() {
    return dbName;
  }

  public ReplicationType getReplicationType() {
    return replicationType;
  }

  public String getStagingDir() {
    return stagingDir;
  }

  public void setLastReplId(long lastReplId) {
    this.lastReplId = lastReplId;
  }

  public double getReplicatedDBSizeInKB() {
    return replicatedDBSizeInKB;
  }

  public void setReplicatedDBSizeInKB(double size) {
    this.replicatedDBSizeInKB = size;
  }

  public String getFailoverMetadataLoc() {
    return failoverMetadataLoc;
  }

  public void setFailoverMetadataLoc(String failoverMetadataLoc) {
    this.failoverMetadataLoc = failoverMetadataLoc;
  }

  public long getFailoverEventId() {
    return failoverEventId;
  }

  public void setFailoverEventId(long failoverEventId) {
    this.failoverEventId = failoverEventId;
  }

  public String getFailoverEndPoint() { return failoverEndPoint; }

  public void setFailoverEndPoint(String endpoint) { this.failoverEndPoint = endpoint; }

  public String getFailoverType() { return failoverType; }

  public void setFailoverType(String type) { this.failoverType = type; }
}
