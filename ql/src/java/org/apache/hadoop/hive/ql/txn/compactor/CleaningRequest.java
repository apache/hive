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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * A class which specifies the required information for cleanup.
 * Objects from this class are passed to FSRemover for cleanup.
 */
public class CleaningRequest {
  public enum RequestType {
    COMPACTION,
  }
  private final RequestType type;
  private final String location;
  private final List<Path> obsoleteDirs;
  private final boolean purge;
  private final FileSystem fs;
  protected String runAs;
  protected String cleanerMetric;
  protected String dbName;
  protected String tableName;
  protected String partitionName;
  protected boolean dropPartition;
  protected String fullPartitionName;

  public CleaningRequest(RequestType type, String location, List<Path> obsoleteDirs, boolean purge, FileSystem fs) {
    this.type = type;
    this.location = location;
    this.obsoleteDirs = obsoleteDirs;
    this.purge = purge;
    this.fs = fs;
  }

  public RequestType getType() {
    return type;
  }

  public String getLocation() {
    return location;
  }

  public List<Path> getObsoleteDirs() {
    return obsoleteDirs;
  }

  public boolean isPurge() {
    return purge;
  }

  public FileSystem getFs() {
    return fs;
  }

  public String getCleanerMetric() {
    return cleanerMetric;
  }

  public String runAs() {
    return runAs;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public boolean isDropPartition() {
    return dropPartition;
  }

  public String getFullPartitionName() {
    return fullPartitionName;
  }
}
