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
package org.apache.hadoop.hive.metastore;


/**
 * Metadata related to Msck.
 */
public class MsckInfo {

  private final String catalogName;
  private final String dbName;
  private final String tableName;
  private final byte[] filterExp;
  private final String resFile;
  private final boolean repairPartitions;
  private final boolean addPartitions;
  private final boolean dropPartitions;
  private final long partitionExpirySeconds;

  public MsckInfo(String catalogName, String dbName, String tableName, byte[] filterExp, String resFile,
                  boolean repairPartitions, boolean addPartitions,
                  boolean dropPartitions, long partitionExpirySeconds) {
    this.catalogName = catalogName;
    this.dbName = dbName;
    this.tableName = tableName;
    this.filterExp = filterExp;
    this.resFile = resFile;
    this.repairPartitions = repairPartitions;
    this.addPartitions = addPartitions;
    this.dropPartitions = dropPartitions;
    this.partitionExpirySeconds = partitionExpirySeconds;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public byte[] getFilterExp() {
    return filterExp;
  }

  public String getResFile() {
    return resFile;
  }

  public boolean isRepairPartitions() {
    return repairPartitions;
  }

  public boolean isAddPartitions() {
    return addPartitions;
  }

  public boolean isDropPartitions() {
    return dropPartitions;
  }

  public long getPartitionExpirySeconds() {
    return partitionExpirySeconds;
  }
}
