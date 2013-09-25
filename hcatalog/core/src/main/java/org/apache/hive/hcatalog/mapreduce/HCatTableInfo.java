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

package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatSchema;

/**
 *
 * HCatTableInfo - class to communicate table information to {@link HCatInputFormat}
 * and {@link HCatOutputFormat}
 *
 */
public class HCatTableInfo implements Serializable {


  private static final long serialVersionUID = 1L;

  /** The db and table names */
  private final String databaseName;
  private final String tableName;

  /** The table schema. */
  private final HCatSchema dataColumns;
  private final HCatSchema partitionColumns;

  /** The table being written to */
  private final Table table;

  /** The storer info */
  private StorerInfo storerInfo;

  /**
   * Initializes a new HCatTableInfo instance to be used with {@link HCatInputFormat}
   * for reading data from a table.
   * work with hadoop security, the kerberos principal name of the server - else null
   * The principal name should be of the form:
   * <servicename>/_HOST@<realm> like "hcat/_HOST@myrealm.com"
   * The special string _HOST will be replaced automatically with the correct host name
   * @param databaseName the db name
   * @param tableName the table name
   * @param dataColumns schema of columns which contain data
   * @param partitionColumns schema of partition columns
   * @param storerInfo information about storage descriptor
   * @param table hive metastore table class
   */
  HCatTableInfo(
    String databaseName,
    String tableName,
    HCatSchema dataColumns,
    HCatSchema partitionColumns,
    StorerInfo storerInfo,
    Table table) {
    this.databaseName = (databaseName == null) ? MetaStoreUtils.DEFAULT_DATABASE_NAME : databaseName;
    this.tableName = tableName;
    this.dataColumns = dataColumns;
    this.table = table;
    this.storerInfo = storerInfo;
    this.partitionColumns = partitionColumns;
  }

  /**
   * Gets the value of databaseName
   * @return the databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * Gets the value of tableName
   * @return the tableName
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * @return return schema of data columns as defined in meta store
   */
  public HCatSchema getDataColumns() {
    return dataColumns;
  }

  /**
   * @return schema of partition columns
   */
  public HCatSchema getPartitionColumns() {
    return partitionColumns;
  }

  /**
   * @return the storerInfo
   */
  public StorerInfo getStorerInfo() {
    return storerInfo;
  }

  public String getTableLocation() {
    return table.getSd().getLocation();
  }

  /**
   * minimize dependency on hive classes so this is package private
   * this should eventually no longer be used
   * @return hive metastore representation of table
   */
  Table getTable() {
    return table;
  }

  /**
   * create an HCatTableInfo instance from the supplied Hive Table instance
   * @param table to create an instance from
   * @return HCatTableInfo
   * @throws IOException
   */
  static HCatTableInfo valueOf(Table table) throws IOException {
    // Explicitly use {@link org.apache.hadoop.hive.ql.metadata.Table} when getting the schema,
    // but store @{link org.apache.hadoop.hive.metastore.api.Table} as this class is serialized
    // into the job conf.
    org.apache.hadoop.hive.ql.metadata.Table mTable =
      new org.apache.hadoop.hive.ql.metadata.Table(table);
    HCatSchema schema = HCatUtil.extractSchema(mTable);
    StorerInfo storerInfo =
      InternalUtil.extractStorerInfo(table.getSd(), table.getParameters());
    HCatSchema partitionColumns = HCatUtil.getPartitionColumns(mTable);
    return new HCatTableInfo(table.getDbName(), table.getTableName(), schema,
      partitionColumns, storerInfo, table);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    HCatTableInfo tableInfo = (HCatTableInfo) o;

    if (dataColumns != null ? !dataColumns.equals(tableInfo.dataColumns) : tableInfo.dataColumns != null)
      return false;
    if (databaseName != null ? !databaseName.equals(tableInfo.databaseName) : tableInfo.databaseName != null)
      return false;
    if (partitionColumns != null ? !partitionColumns.equals(tableInfo.partitionColumns) : tableInfo.partitionColumns != null)
      return false;
    if (storerInfo != null ? !storerInfo.equals(tableInfo.storerInfo) : tableInfo.storerInfo != null) return false;
    if (table != null ? !table.equals(tableInfo.table) : tableInfo.table != null) return false;
    if (tableName != null ? !tableName.equals(tableInfo.tableName) : tableInfo.tableName != null) return false;

    return true;
  }


  @Override
  public int hashCode() {
    int result = databaseName != null ? databaseName.hashCode() : 0;
    result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
    result = 31 * result + (dataColumns != null ? dataColumns.hashCode() : 0);
    result = 31 * result + (partitionColumns != null ? partitionColumns.hashCode() : 0);
    result = 31 * result + (table != null ? table.hashCode() : 0);
    result = 31 * result + (storerInfo != null ? storerInfo.hashCode() : 0);
    return result;
  }

}

