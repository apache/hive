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
 * Contains the information needed to rename a partition.
 */
public class RenamePartitionDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  String tableName;
  String location;
  LinkedHashMap<String, String> oldPartSpec;
  LinkedHashMap<String, String> newPartSpec;

  /**
   * For serialization only.
   */
  public RenamePartitionDesc() {
  }

  /**
   * @param dbName
   *          database to add to.
   * @param tableName
   *          table to add to.
   * @param oldPartSpec
   *          old partition specification.
   * @param newPartSpec
   *          new partition specification.
   */
  public RenamePartitionDesc(String tableName,
      Map<String, String> oldPartSpec, Map<String, String> newPartSpec) {
    this.tableName = tableName;
    this.oldPartSpec = new LinkedHashMap<String,String>(oldPartSpec);
    this.newPartSpec = new LinkedHashMap<String,String>(newPartSpec);
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
   * @return location of partition in relation to table
   */
  public String getLocation() {
    return location;
  }

  /**
   * @param location
   *          location of partition in relation to table
   */
  public void setLocation(String location) {
    this.location = location;
  }

  /**
   * @return old partition specification.
   */
  public LinkedHashMap<String, String> getOldPartSpec() {
    return oldPartSpec;
  }

  /**
   * @param partSpec
   *          partition specification
   */
  public void setOldPartSpec(LinkedHashMap<String, String> partSpec) {
    this.oldPartSpec = partSpec;
  }

  /**
   * @return new partition specification.
   */
  public LinkedHashMap<String, String> getNewPartSpec() {
    return newPartSpec;
  }

  /**
   * @param partSpec
   *          partition specification
   */
  public void setNewPartSpec(LinkedHashMap<String, String> partSpec) {
    this.newPartSpec = partSpec;
  }
}
