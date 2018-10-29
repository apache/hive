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

import java.util.ArrayList;
import java.util.LinkedHashMap;

/**
 * Metadata related to Msck.
 */
public class MsckInfo {

  private String catalogName;
  private String dbName;
  private String tableName;
  private ArrayList<LinkedHashMap<String, String>> partSpecs;
  private String resFile;
  private boolean repairPartitions;
  private boolean addPartitions;
  private boolean dropPartitions;
  private long partitionExpirySeconds;

  public MsckInfo(final String catalogName, final String dbName, final String tableName,
    final ArrayList<LinkedHashMap<String, String>> partSpecs, final String resFile, final boolean repairPartitions,
    final boolean addPartitions,
    final boolean dropPartitions,
    final long partitionExpirySeconds) {
    this.catalogName = catalogName;
    this.dbName = dbName;
    this.tableName = tableName;
    this.partSpecs = partSpecs;
    this.resFile = resFile;
    this.repairPartitions = repairPartitions;
    this.addPartitions = addPartitions;
    this.dropPartitions = dropPartitions;
    this.partitionExpirySeconds = partitionExpirySeconds;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public void setCatalogName(final String catalogName) {
    this.catalogName = catalogName;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(final String dbName) {
    this.dbName = dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(final String tableName) {
    this.tableName = tableName;
  }

  public ArrayList<LinkedHashMap<String, String>> getPartSpecs() {
    return partSpecs;
  }

  public void setPartSpecs(final ArrayList<LinkedHashMap<String, String>> partSpecs) {
    this.partSpecs = partSpecs;
  }

  public String getResFile() {
    return resFile;
  }

  public void setResFile(final String resFile) {
    this.resFile = resFile;
  }

  public boolean isRepairPartitions() {
    return repairPartitions;
  }

  public void setRepairPartitions(final boolean repairPartitions) {
    this.repairPartitions = repairPartitions;
  }

  public boolean isAddPartitions() {
    return addPartitions;
  }

  public void setAddPartitions(final boolean addPartitions) {
    this.addPartitions = addPartitions;
  }

  public boolean isDropPartitions() {
    return dropPartitions;
  }

  public void setDropPartitions(final boolean dropPartitions) {
    this.dropPartitions = dropPartitions;
  }

  public long getPartitionExpirySeconds() {
    return partitionExpirySeconds;
  }

  public void setPartitionExpirySeconds(final long partitionExpirySeconds) {
    this.partitionExpirySeconds = partitionExpirySeconds;
  }
}
