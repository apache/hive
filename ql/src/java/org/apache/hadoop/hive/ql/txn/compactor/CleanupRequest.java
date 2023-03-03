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

import org.apache.hadoop.fs.Path;

import java.util.List;

/**
 * A class which specifies the required information for cleanup.
 * Objects of this class are created by request handlers.
 * Objects from this class are passed to FSRemover for cleanup.
 */
public class CleanupRequest {
  private final String location;
  private final List<Path> obsoleteDirs;
  private final boolean purge;
  private final String runAs;
  private final String dbName;
  private final String fullPartitionName;

  public CleanupRequest(CleanupRequestBuilder builder) {
    this.location = builder.location;
    this.obsoleteDirs = builder.obsoleteDirs;
    this.purge = builder.purge;
    this.runAs = builder.runAs;
    this.dbName = builder.dbName;
    this.fullPartitionName = builder.fullPartitionName;
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

  public String runAs() {
    return runAs;
  }

  public String getDbName() {
    return dbName;
  }

  public String getFullPartitionName() {
    return fullPartitionName;
  }

  /**
   * A builder for generating objects of CleaningRequest.
   */
  public static class CleanupRequestBuilder {
    private String location;
    private List<Path> obsoleteDirs;
    private boolean purge;
    private String runAs;
    private String dbName;
    private String fullPartitionName;

    public CleanupRequestBuilder setLocation(String location) {
      this.location = location;
      return this;
    }

    public CleanupRequestBuilder setObsoleteDirs(List<Path> obsoleteDirs) {
      this.obsoleteDirs = obsoleteDirs;
      return this;
    }

    public CleanupRequestBuilder setPurge(boolean purge) {
      this.purge = purge;
      return this;
    }

    public CleanupRequestBuilder setDbName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public CleanupRequestBuilder setRunAs(String runAs) {
      this.runAs = runAs;
      return this;
    }

    public CleanupRequestBuilder setFullPartitionName(String fullPartitionName) {
      this.fullPartitionName = fullPartitionName;
      return this;
    }

    public CleanupRequest build() {
      return new CleanupRequest(this);
    }
  }
}
