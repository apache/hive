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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The HCatPartition is a wrapper around org.apache.hadoop.hive.metastore.api.Partition.
 */
public class HCatPartition {

  private static final Logger LOG = LoggerFactory.getLogger(HCatPartition.class);

  private HCatTable hcatTable;
  private String tableName;
  private String dbName = MetaStoreUtils.DEFAULT_DATABASE_NAME;
  private List<String> values;
  private int createTime;
  private int lastAccessTime;
  private StorageDescriptor sd;
  private List<HCatFieldSchema> columns; // Cache column-list from this.sd.
  private Map<String, String> parameters;

  // For use from within HCatClient.getPartitions().
  HCatPartition(HCatTable hcatTable, Partition partition) throws HCatException {
    this.hcatTable = hcatTable;
    this.tableName = partition.getTableName();
    this.dbName = partition.getDbName();
    this.createTime = partition.getCreateTime();
    this.lastAccessTime = partition.getLastAccessTime();
    this.parameters = partition.getParameters();
    this.values = partition.getValues();
    if (hcatTable != null && partition.getValuesSize() != hcatTable.getPartCols().size()) {
      throw new HCatException("Mismatched number of partition columns between table:" + hcatTable.getDbName() + "." + hcatTable.getTableName()
                              + " and partition " + partition.getValues());
    }

    this.sd = partition.getSd();
    this.columns = getColumns(this.sd);
  }

  // For constructing HCatPartitions afresh, as an argument to HCatClient.addPartitions().
  public HCatPartition(HCatTable hcatTable, Map<String, String> partitionKeyValues, String location) throws HCatException {
    this.hcatTable = hcatTable;
    this.tableName = hcatTable.getTableName();
    this.dbName = hcatTable.getDbName();
    this.sd = new StorageDescriptor(hcatTable.getSd());
    this.sd.setLocation(location);
    this.columns = getColumns(this.sd);
    this.createTime = (int)(System.currentTimeMillis()/1000);
    this.lastAccessTime = -1;
    this.values = new ArrayList<String>(hcatTable.getPartCols().size());
    for (HCatFieldSchema partField : hcatTable.getPartCols()) {
      if (!partitionKeyValues.containsKey(partField.getName())) {
        throw new HCatException("Missing value for partition-key \'" + partField.getName()
            + "\' in table: " + hcatTable.getDbName() + "." + hcatTable.getTableName());
      }
      else {
        values.add(partitionKeyValues.get(partField.getName()));
      }
    }
  }

  // For replicating an HCatPartition definition.
  public HCatPartition(HCatPartition rhs, Map<String, String> partitionKeyValues, String location) throws HCatException {
    this.hcatTable = rhs.hcatTable;
    this.tableName = rhs.tableName;
    this.dbName = rhs.dbName;
    this.sd = new StorageDescriptor(rhs.sd);
    this.sd.setLocation(location);
    this.columns = getColumns(this.sd);
    this.createTime = (int) (System.currentTimeMillis() / 1000);
    this.lastAccessTime = -1;
    this.values = new ArrayList<String>(hcatTable.getPartCols().size());
    for (HCatFieldSchema partField : hcatTable.getPartCols()) {
      if (!partitionKeyValues.containsKey(partField.getName())) {
        throw new HCatException("Missing value for partition-key \'" + partField.getName()
            + "\' in table: " + hcatTable.getDbName() + "." + hcatTable.getTableName());
      } else {
        values.add(partitionKeyValues.get(partField.getName()));
      }
    }
  }

  private static List<HCatFieldSchema> getColumns(StorageDescriptor sd) throws HCatException {
    ArrayList<HCatFieldSchema> columns = new ArrayList<HCatFieldSchema>(sd.getColsSize());
    for (FieldSchema fieldSchema : sd.getCols()) {
      columns.add(HCatSchemaUtils.getHCatFieldSchema(fieldSchema));
    }
    return columns;
  }

  // For use from HCatClient.addPartitions(), to construct from user-input.
  Partition toHivePartition() throws HCatException {
    Partition hivePtn = new Partition();
    hivePtn.setDbName(dbName);
    hivePtn.setTableName(tableName);
    hivePtn.setValues(values);

    hivePtn.setParameters(parameters);
    if (sd.getLocation() == null) {
      LOG.warn("Partition location is not set! Attempting to construct default partition location.");
      try {
        String partName = Warehouse.makePartName(HCatSchemaUtils.getFieldSchemas(hcatTable.getPartCols()), values);
        sd.setLocation(new Path(hcatTable.getSd().getLocation(), partName).toString());
      }
      catch(MetaException exception) {
        throw new HCatException("Could not construct default partition-path for "
            + hcatTable.getDbName() + "." + hcatTable.getTableName() + "[" + values + "]");
      }
    }
    hivePtn.setSd(sd);

    hivePtn.setCreateTime((int) (System.currentTimeMillis() / 1000));
    hivePtn.setLastAccessTimeIsSet(false);
    return hivePtn;
  }

  public HCatTable hcatTable() {
    return hcatTable;
  }

  public HCatPartition hcatTable(HCatTable hcatTable) {
    this.hcatTable = hcatTable;
    this.tableName = hcatTable.getTableName();
    this.dbName = hcatTable.getDbName();
    return this;
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
    return columns;
  }

  /**
   * Gets the partition columns of the table.
   *
   * @return the partition columns
   */
  public List<HCatFieldSchema> getPartColumns() {
    return hcatTable.getPartCols();
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
   * Setter for partition directory location.
   */
  public HCatPartition location(String location) {
    this.sd.setLocation(location);
    return this;
  }

  /**
   * Gets the serde.
   *
   * @return the serde
   */
  public String getSerDe() {
    return this.sd.getSerdeInfo().getSerializationLib();
  }

  /**
   * Getter for SerDe parameters.
   * @return The SerDe parameters.
   */
  public Map<String, String> getSerdeParams() {
    return this.sd.getSerdeInfo().getParameters();
  }

  public HCatPartition parameters(Map<String,String> parameters){
    if (this.parameters == null){
      this.parameters = new HashMap<String,String>();
    }
    if (!this.parameters.equals(parameters)) {
      this.parameters.clear();
      this.parameters.putAll(parameters);
    }
    return this;
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
   * Getter for partition-spec map.
   */
  public LinkedHashMap<String, String> getPartitionKeyValMap() {
    LinkedHashMap<String, String> map = new LinkedHashMap<String, String>(hcatTable.getPartCols().size());
    for (int i=0; i<hcatTable.getPartCols().size(); ++i) {
      map.put(hcatTable.getPartCols().get(i).getName(), values.get(i));
    }
    return map;
  }

  /**
   * Setter for partition key-values.
   */
  public HCatPartition setPartitionKeyValues(Map<String, String> partitionKeyValues) throws HCatException {
    for (HCatFieldSchema partField : hcatTable.getPartCols()) {
      if (!partitionKeyValues.containsKey(partField.getName())) {
        throw new HCatException("Missing value for partition-key \'" + partField.getName()
            + "\' in table: " + hcatTable.getDbName() + "." + hcatTable.getTableName());
      }
      else {
        values.add(partitionKeyValues.get(partField.getName()));
        // Keep partKeyValMap in synch as well.
      }
    }
    return this;
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
    return "HCatPartition [ "
        + "tableName=" + tableName + ","
        + "dbName=" + dbName + ","
        + "values=" + values + ","
        + "createTime=" + createTime + ","
        + "lastAccessTime=" + lastAccessTime + ","
        + "sd=" + sd + ","
        + "parameters=" + parameters + "]";
  }

}
