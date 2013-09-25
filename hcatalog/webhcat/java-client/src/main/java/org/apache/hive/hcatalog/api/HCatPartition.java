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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;

/**
 * The HCatPartition is a wrapper around org.apache.hadoop.hive.metastore.api.Partition.
 */
public class HCatPartition {

  private String tableName;
  private String dbName;
  private List<String> values;
  private List<HCatFieldSchema> tableCols;
  private int createTime;
  private int lastAccessTime;
  private StorageDescriptor sd;
  private Map<String, String> parameters;

  HCatPartition(Partition partition) throws HCatException {
    this.tableName = partition.getTableName();
    this.dbName = partition.getDbName();
    this.createTime = partition.getCreateTime();
    this.lastAccessTime = partition.getLastAccessTime();
    this.parameters = partition.getParameters();
    this.values = partition.getValues();
    this.sd = partition.getSd();
    this.tableCols = new ArrayList<HCatFieldSchema>();
    for (FieldSchema fs : this.sd.getCols()) {
      this.tableCols.add(HCatSchemaUtils.getHCatFieldSchema(fs));
    }
  }

  /**
   * Gets the table name.
   *
   * @return the table name
   */
  public String getTableName() {
    return this.tableName;
  }

  /**
   * Gets the database name.
   *
   * @return the database name
   */
  public String getDatabaseName() {
    return this.dbName;
  }

  /**
   * Gets the columns of the table.
   *
   * @return the columns
   */
  public List<HCatFieldSchema> getColumns() {
    return this.tableCols;
  }

  /**
   * Gets the input format.
   *
   * @return the input format
   */
  public String getInputFormat() {
    return this.sd.getInputFormat();
  }

  /**
   * Gets the output format.
   *
   * @return the output format
   */
  public String getOutputFormat() {
    return this.sd.getOutputFormat();
  }

  /**
   * Gets the storage handler.
   *
   * @return the storage handler
   */
  public String getStorageHandler() {
    return this.sd
      .getParameters()
      .get(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE);
  }

  /**
   * Gets the location.
   *
   * @return the location
   */
  public String getLocation() {
    return this.sd.getLocation();
  }

  /**
   * Gets the serde.
   *
   * @return the serde
   */
  public String getSerDe() {
    return this.sd.getSerdeInfo().getSerializationLib();
  }

  public Map<String, String> getParameters() {
    return this.parameters;
  }

  /**
   * Gets the last access time.
   *
   * @return the last access time
   */
  public int getLastAccessTime() {
    return this.lastAccessTime;
  }

  /**
   * Gets the creates the time.
   *
   * @return the creates the time
   */
  public int getCreateTime() {
    return this.createTime;
  }

  /**
   * Gets the values.
   *
   * @return the values
   */
  public List<String> getValues() {
    return this.values;
  }

  /**
   * Gets the bucket columns.
   *
   * @return the bucket columns
   */
  public List<String> getBucketCols() {
    return this.sd.getBucketCols();
  }

  /**
   * Gets the number of buckets.
   *
   * @return the number of buckets
   */
  public int getNumBuckets() {
    return this.sd.getNumBuckets();
  }

  /**
   * Gets the sort columns.
   *
   * @return the sort columns
   */
  public List<Order> getSortCols() {
    return this.sd.getSortCols();
  }

  @Override
  public String toString() {
    return "HCatPartition ["
      + (tableName != null ? "tableName=" + tableName + ", " : "tableName=null")
      + (dbName != null ? "dbName=" + dbName + ", " : "dbName=null")
      + (values != null ? "values=" + values + ", " : "values=null")
      + "createTime=" + createTime + ", lastAccessTime="
      + lastAccessTime + ", " + (sd != null ? "sd=" + sd + ", " : "sd=null")
      + (parameters != null ? "parameters=" + parameters : "parameters=null") + "]";
  }

}
