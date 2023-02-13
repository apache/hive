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
  private final String runAs;
  private final String cleanerMetric;
  private final String dbName;
  private final String tableName;
  private final String partitionName;
  private final boolean dropPartition;
  private final String fullPartitionName;

  public CleaningRequest(CleaningRequestBuilder<? extends CleaningRequestBuilder<?>> builder) {
    this.type = builder.type;
    this.location = builder.location;
    this.obsoleteDirs = builder.obsoleteDirs;
    this.purge = builder.purge;
    this.fs = builder.fs;
    this.runAs = builder.runAs;
    this.cleanerMetric = builder.cleanerMetric;
    this.dbName = builder.dbName;
    this.tableName = builder.tableName;
    this.partitionName = builder.partitionName;
    this.dropPartition = builder.dropPartition;
    this.fullPartitionName = builder.fullPartitionName;
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

  /**
   * This builder supports explicit type-casting of sub-class builders by using recursion and generics.
   * @param <T> Sub-class that is going to extend this cleaning request builder
   */
  public static class CleaningRequestBuilder<T extends CleaningRequestBuilder<T>> {
    private RequestType type;
    private String location;
    private List<Path> obsoleteDirs;
    private boolean purge;
    private FileSystem fs;
    private String runAs;
    private String cleanerMetric;
    private String dbName;
    private String tableName;
    private String partitionName;
    private boolean dropPartition;
    private String fullPartitionName;

    public T setType(RequestType type) {
      this.type = type;
      return self();
    }

    public T setLocation(String location) {
      this.location = location;
      return self();
    }

    public T setObsoleteDirs(List<Path> obsoleteDirs) {
      this.obsoleteDirs = obsoleteDirs;
      return self();
    }

    public T setPurge(boolean purge) {
      this.purge = purge;
      return self();
    }

    public T setFs(FileSystem fs) {
      this.fs = fs;
      return self();
    }

    public T setCleanerMetric(String cleanerMetric) {
      this.cleanerMetric = cleanerMetric;
      return self();
    }

    public T setDbName(String dbName) {
      this.dbName = dbName;
      return self();
    }

    public T setTableName(String tableName) {
      this.tableName = tableName;
      return self();
    }

    public T setPartitionName(String partitionName) {
      this.partitionName = partitionName;
      return self();
    }

    public T setRunAs(String runAs) {
      this.runAs = runAs;
      return self();
    }

    public T setFullPartitionName(String fullPartitionName) {
      this.fullPartitionName = fullPartitionName;
      return self();
    }

    public T setDropPartition(boolean dropPartition) {
      this.dropPartition = dropPartition;
      return self();
    }

    @SuppressWarnings("unchecked")
    final T self() {
      return (T) this;
    }

    public CleaningRequest build() {
      return new CleaningRequest(this);
    }
  }
}
