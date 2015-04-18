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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;

/**
 * Contains the information needed to add one or more partitions.
 */
public class AddPartitionDesc extends DDLDesc implements Serializable {

  public static class OnePartitionDesc {
    public OnePartitionDesc() {}

    OnePartitionDesc(
        Map<String, String> partSpec, String location, Map<String, String> params) {
      this(partSpec, location);
      this.partParams = params;
    }

    OnePartitionDesc(Map<String, String> partSpec, String location) {
      this.partSpec = partSpec;
      this.location = location;
    }

    Map<String, String> partSpec;
    Map<String, String> partParams;
    String location;
    String inputFormat = null;
    String outputFormat = null;
    int numBuckets = -1;
    List<FieldSchema> cols = null;
    String serializationLib = null;
    Map<String, String> serdeParams = null;
    List<String> bucketCols = null;
    List<Order> sortCols = null;

    public Map<String, String> getPartSpec() {
      return partSpec;
    }

    /**
     * @return location of partition in relation to table
     */
    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public Map<String, String> getPartParams() {
      return partParams;
    }

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
  }

  private static final long serialVersionUID = 1L;

  String tableName;
  String dbName;
  boolean ifNotExists;
  List<OnePartitionDesc> partitions = null;
  boolean replaceMode = false;


  /**
   * For serialization only.
   */
  public AddPartitionDesc() {
  }

  public AddPartitionDesc(
      String dbName, String tableName, boolean ifNotExists) {
    super();
    this.dbName = dbName;
    this.tableName = tableName;
    this.ifNotExists = ifNotExists;
  }

  /**
   * Legacy single-partition ctor for ImportSemanticAnalyzer
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
  @Deprecated
  public AddPartitionDesc(String dbName, String tableName,
      Map<String, String> partSpec, String location, Map<String, String> params) {
    super();
    this.dbName = dbName;
    this.tableName = tableName;
    this.ifNotExists = true;
    addPartition(partSpec, location, params);
  }

  public void addPartition(Map<String, String> partSpec, String location) {
    addPartition(partSpec, location, null);
  }

  private void addPartition(
      Map<String, String> partSpec, String location, Map<String, String> params) {
    if (this.partitions == null) {
      this.partitions = new ArrayList<OnePartitionDesc>();
    }
    this.partitions.add(new OnePartitionDesc(partSpec, location, params));
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
  public String getLocationForExplain() {
    if (this.partitions == null || this.partitions.isEmpty()) return "<no partition>";
    boolean isFirst = true;
    StringBuilder sb = new StringBuilder();
    for (OnePartitionDesc desc : this.partitions) {
      if (!isFirst) {
        sb.append(", ");
      }
      isFirst = false;
      sb.append(desc.location);
    }
    return sb.toString();
  }

  @Explain(displayName = "Spec")
  public String getPartSpecStringForExplain() {
    if (this.partitions == null || this.partitions.isEmpty()) return "<no partition>";
    boolean isFirst = true;
    StringBuilder sb = new StringBuilder();
    for (OnePartitionDesc desc : this.partitions) {
      if (!isFirst) {
        sb.append(", ");
      }
      isFirst = false;
      sb.append(desc.partSpec.toString());
    }
    return sb.toString();
  }

  /**
   * @return if the partition should only be added if it doesn't exist already
   */
  public boolean isIfNotExists() {
    return this.ifNotExists;
  }

  /**
   * @param ifNotExists
   *          if the part should be added only if it doesn't exist
   */
  public void setIfNotExists(boolean ifNotExists) {
    this.ifNotExists = ifNotExists;
  }

  public int getPartitionCount() {
    return this.partitions.size();
  }

  public OnePartitionDesc getPartition(int i) {
    return this.partitions.get(i);
  }

  /**
   * @param replaceMode Determine if this AddPartition should behave like a replace-into alter instead
   */
  public void setReplaceMode(boolean replaceMode){
    this.replaceMode = replaceMode;
  }

  /**
   * @return true if this AddPartition should behave like a replace-into alter instead
   */
  public boolean getReplaceMode() {
    return this.replaceMode;
  }
}
