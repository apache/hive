/**
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
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Contains the information needed to add a partition.
 */
public class AlterPartitionProtectModeDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  String tableName;
  String dbName;
  boolean protectModeEnable;
  ProtectModeType protectModeType;

  public static enum ProtectModeType {
    NO_DROP, OFFLINE, READ_ONLY
  };

  LinkedHashMap<String, String> partSpec;

  /**
   * For serialization only.
   */
  public AlterPartitionProtectModeDesc() {
  }

  /**
   * @param dbName
   *          database to add to.
   * @param tableName
   *          table to add to.
   * @param partSpec
   *          partition specification.
   * @param location
   *          partition location, relative to table location.
   * @param ifNotExists
   *          if true, the partition is only added if it doesn't exist
   */
  public AlterPartitionProtectModeDesc(String dbName, String tableName,
      Map<String, String> partSpec) {
    super();
    this.dbName = dbName;
    this.tableName = tableName;
    this.partSpec = new LinkedHashMap<String,String>(partSpec);
  }

  /**
   * @return database name
   */
  public String getDbName() {
    return dbName;
  }

  /**
   * @param dbName
   *          database name
   */
  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  /**
   * @return the table we're going to add the partitions to.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @param tableName
   *          the table we're going to add the partitions to.
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * @return partition specification.
   */
  public LinkedHashMap<String, String> getPartSpec() {
    return partSpec;
  }

  /**
   * @param partSpec
   *          partition specification
   */
  public void setPartSpec(LinkedHashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  public boolean isProtectModeEnable() {
    return protectModeEnable;
  }

  public void setProtectModeEnable(boolean protectModeEnable) {
    this.protectModeEnable = protectModeEnable;
  }

  public ProtectModeType getProtectModeType() {
    return protectModeType;
  }

  public void setProtectModeType(ProtectModeType protectModeType) {
    this.protectModeType = protectModeType;
  }
}
