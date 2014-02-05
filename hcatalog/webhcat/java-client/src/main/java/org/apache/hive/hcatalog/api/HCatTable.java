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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;

/**
 * The HCatTable is a wrapper around org.apache.hadoop.hive.metastore.api.Table.
 */
public class HCatTable {

  private String tableName;
  private String tabletype;
  private List<HCatFieldSchema> cols;
  private List<HCatFieldSchema> partCols;
  private List<String> bucketCols;
  private List<Order> sortCols;
  private int numBuckets;
  private String inputFileFormat;
  private String outputFileFormat;
  private String storageHandler;
  private Map<String, String> tblProps;
  private String dbName;
  private String serde;
  private String location;
  private Map<String, String> serdeParams;

  HCatTable(Table hiveTable) throws HCatException {
    this.tableName = hiveTable.getTableName();
    this.dbName = hiveTable.getDbName();
    this.tabletype = hiveTable.getTableType();
    cols = new ArrayList<HCatFieldSchema>();
    for (FieldSchema colFS : hiveTable.getSd().getCols()) {
      cols.add(HCatSchemaUtils.getHCatFieldSchema(colFS));
    }
    partCols = new ArrayList<HCatFieldSchema>();
    for (FieldSchema colFS : hiveTable.getPartitionKeys()) {
      partCols.add(HCatSchemaUtils.getHCatFieldSchema(colFS));
    }
    bucketCols = hiveTable.getSd().getBucketCols();
    sortCols = hiveTable.getSd().getSortCols();
    numBuckets = hiveTable.getSd().getNumBuckets();
    inputFileFormat = hiveTable.getSd().getInputFormat();
    outputFileFormat = hiveTable.getSd().getOutputFormat();
    storageHandler = hiveTable
      .getSd()
      .getParameters()
      .get(org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_STORAGE);
    tblProps = hiveTable.getParameters();
    serde = hiveTable.getSd().getSerdeInfo().getSerializationLib();
    location = hiveTable.getSd().getLocation();
    serdeParams = hiveTable.getSd().getSerdeInfo().getParameters();
  }

  /**
   * Gets the table name.
   *
   * @return the table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Gets the db name.
   *
   * @return the db name
   */
  public String getDbName() {
    return dbName;
  }

  /**
   * Gets the columns.
   *
   * @return the columns
   */
  public List<HCatFieldSchema> getCols() {
    return cols;
  }

  /**
   * Gets the part columns.
   *
   * @return the part columns
   */
  public List<HCatFieldSchema> getPartCols() {
    return partCols;
  }

  /**
   * Gets the bucket columns.
   *
   * @return the bucket columns
   */
  public List<String> getBucketCols() {
    return bucketCols;
  }

  /**
   * Gets the sort columns.
   *
   * @return the sort columns
   */
  public List<Order> getSortCols() {
    return sortCols;
  }

  /**
   * Gets the number of buckets.
   *
   * @return the number of buckets
   */
  public int getNumBuckets() {
    return numBuckets;
  }

  /**
   * Gets the storage handler.
   *
   * @return the storage handler
   */
  public String getStorageHandler() {
    return storageHandler;
  }

  /**
   * Gets the table props.
   *
   * @return the table props
   */
  public Map<String, String> getTblProps() {
    return tblProps;
  }

  /**
   * Gets the tabletype.
   *
   * @return the tabletype
   */
  public String getTabletype() {
    return tabletype;
  }

  /**
   * Gets the input file format.
   *
   * @return the input file format
   */
  public String getInputFileFormat() {
    return inputFileFormat;
  }

  /**
   * Gets the output file format.
   *
   * @return the output file format
   */
  public String getOutputFileFormat() {
    return outputFileFormat;
  }

  /**
   * Gets the serde lib.
   *
   * @return the serde lib
   */
  public String getSerdeLib() {
    return serde;
  }

  /**
   * Gets the location.
   *
   * @return the location
   */
  public String getLocation() {
    return location;
  }
  /**
   * Returns parameters such as field delimiter,etc.
   */
  public Map<String, String> getSerdeParams() {
    return serdeParams;
  }

  @Override
  public String toString() {
    return "HCatTable ["
      + (tableName != null ? "tableName=" + tableName + ", " : "tableName=null")
      + (dbName != null ? "dbName=" + dbName + ", " : "dbName=null")
      + (tabletype != null ? "tabletype=" + tabletype + ", " : "tabletype=null")
      + (cols != null ? "cols=" + cols + ", " : "cols=null")
      + (partCols != null ? "partCols=" + partCols + ", " : "partCols==null")
      + (bucketCols != null ? "bucketCols=" + bucketCols + ", " : "bucketCols=null")
      + (sortCols != null ? "sortCols=" + sortCols + ", " : "sortCols=null")
      + "numBuckets="
      + numBuckets
      + ", "
      + (inputFileFormat != null ? "inputFileFormat="
      + inputFileFormat + ", " : "inputFileFormat=null")
      + (outputFileFormat != null ? "outputFileFormat="
      + outputFileFormat + ", " : "outputFileFormat=null")
      + (storageHandler != null ? "storageHandler=" + storageHandler
      + ", " : "storageHandler=null")
      + (tblProps != null ? "tblProps=" + tblProps + ", " : "tblProps=null")
      + (serde != null ? "serde=" + serde + ", " : "serde=")
      + (location != null ? "location=" + location : "location=")
      + ",serdeParams=" + (serdeParams == null ? "null" : serdeParams)
      + "]";
  }
}
