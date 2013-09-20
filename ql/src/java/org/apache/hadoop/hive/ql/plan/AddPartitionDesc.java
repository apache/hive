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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;

/**
 * Contains the information needed to add a partition.
 */
public class AddPartitionDesc extends DDLDesc implements Serializable {

  private static final long serialVersionUID = 1L;

  String tableName;
  String dbName;
  String location;
  boolean ifNotExists;
  boolean expectView;
  LinkedHashMap<String, String> partSpec;
  Map<String, String> partParams;
  String inputFormat = null;
  String outputFormat = null;
  int numBuckets = -1;
  List<FieldSchema> cols = null;
  String serializationLib = null;
  Map<String, String> serdeParams = null;
  List<String> bucketCols = null;
  List<Order> sortCols = null;

  /**
   * For serialization only.
   */
  public AddPartitionDesc() {
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
   * @param params
   *          partition parameters.
   */
  public AddPartitionDesc(String dbName, String tableName,
      Map<String, String> partSpec, String location, Map<String, String> params) {
    this(dbName, tableName, partSpec, location, true, false);
    this.partParams = params;
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
   * @param expectView
   *          true for ALTER VIEW, false for ALTER TABLE
   */
  public AddPartitionDesc(String dbName, String tableName,
      Map<String, String> partSpec, String location, boolean ifNotExists,
      boolean expectView) {
    super();
    this.dbName = dbName;
    this.tableName = tableName;
    this.partSpec = new LinkedHashMap<String,String>(partSpec);
    this.location = location;
    this.ifNotExists = ifNotExists;
    this.expectView = expectView;
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
   * @return location of partition in relation to table
   */
  @Explain(displayName = "Location")
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
   * @return partition specification.
   */
  public LinkedHashMap<String, String> getPartSpec() {
    return partSpec;
  }

  @Explain(displayName = "Spec")
  public String getPartSpecString() {
    return partSpec.toString();
  }

  /**
   * @param partSpec
   *          partition specification
   */
  public void setPartSpec(LinkedHashMap<String, String> partSpec) {
    this.partSpec = partSpec;
  }

  /**
   * @return if the partition should only be added if it doesn't exist already
   */
  public boolean getIfNotExists() {
    return this.ifNotExists;
  }

  /**
   * @param ifNotExists
   *          if the part should be added only if it doesn't exist
   */
  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  /**
   * @return partition parameters.
   */
  public Map<String, String> getPartParams() {
    return partParams;
  }

  /**
   * @param partParams
   *          partition parameters
   */

  public void setPartParams(Map<String, String> partParams) {
    this.partParams = partParams;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  public List<FieldSchema> getCols() {
    return cols;
  }

  public void setCols(List<FieldSchema> cols) {
    this.cols = cols;
  }

  public String getSerializationLib() {
    return serializationLib;
  }

  public void setSerializationLib(String serializationLib) {
    this.serializationLib = serializationLib;
  }

  public Map<String, String> getSerdeParams() {
    return serdeParams;
  }

  public void setSerdeParams(Map<String, String> serdeParams) {
    this.serdeParams = serdeParams;
  }

  public List<String> getBucketCols() {
    return bucketCols;
  }

  public void setBucketCols(List<String> bucketCols) {
    this.bucketCols = bucketCols;
  }

  public List<Order> getSortCols() {
    return sortCols;
  }

  public void setSortCols(List<Order> sortCols) {
    this.sortCols = sortCols;
  }

  public String getInputFormat() {
    return inputFormat;
  }

  public void setInputFormat(String inputFormat) {
    this.inputFormat = inputFormat;
  }

  public String getOutputFormat() {
    return outputFormat;
  }

  public void setOutputFormat(String outputFormat) {
    this.outputFormat = outputFormat;
  }

  /*
   * @return whether to expect a view being altered
   */
  public boolean getExpectView() {
    return expectView;
  }

  /**
   * @param expectView
   *          set whether to expect a view being altered
   */
  public void setExpectView(boolean expectView) {
    this.expectView = expectView;
  }
}
