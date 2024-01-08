/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.txn.entities;

public class CompactionMetricsData {

  private final String dbName;
  private final String tblName;
  private final String partitionName;
  private final MetricType metricType;
  private final int metricValue;
  private final int version;
  private final int threshold;

  public enum MetricType {
    NUM_OBSOLETE_DELTAS("HIVE_ACID_NUM_OBSOLETE_DELTAS"),
    NUM_DELTAS("HIVE_ACID_NUM_DELTAS"),
    NUM_SMALL_DELTAS("HIVE_ACID_NUM_SMALL_DELTAS");

    private final String value;

    MetricType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  private CompactionMetricsData(Builder builder) {
    this.dbName = builder.dbName;
    this.tblName = builder.tblName;
    this.partitionName = builder.partitionName;
    this.metricType = builder.metricType;
    this.metricValue = builder.metricValue;
    this.version = builder.version;
    this.threshold = builder.threshold;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTblName() {
    return tblName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public MetricType getMetricType() {
    return metricType;
  }

  public int getMetricValue() {
    return metricValue;
  }

  public int getVersion() {
    return version;
  }

  public int getThreshold() {
    return threshold;
  }

  public boolean isEmpty() {
    return dbName == null && tblName == null && partitionName == null && metricType == null && metricValue == 0
        && version == 0;
  }

  @Override
  public String toString() {
    return "DeltaMetricsInfo{" + "dbName='" + dbName + '\'' + ", tblName='" + tblName + '\'' + ", partitionName='"
        + partitionName + '\'' + ", metricType=" + metricType + ", metricValue=" + metricValue + ", version=" + version
        + '}';
  }

  public static class Builder {
    private String dbName;
    private String tblName;
    private String partitionName;
    private MetricType metricType;
    private int metricValue;
    private int version;
    private int threshold;

    public CompactionMetricsData build() {
      return new CompactionMetricsData(this);
    }

    public Builder dbName(String dbName) {
      this.dbName = dbName;
      return this;
    }

    public Builder tblName(String tblName) {
      this.tblName = tblName;
      return this;
    }

    public Builder partitionName(String partitionName) {
      this.partitionName = partitionName;
      return this;
    }

    public Builder metricType(MetricType metricType) {
      this.metricType = metricType;
      return this;
    }

    public Builder metricValue(int metricValue) {
      this.metricValue = metricValue;
      return this;
    }

    public Builder version(int version) {
      this.version = version;
      return this;
    }

    public Builder threshold(int threshold) {
      this.threshold = threshold;
      return this;
    }
  }
}
