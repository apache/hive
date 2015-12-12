/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.client;

import java.io.Serializable;

import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Describes an ACID table that can receive mutation events. Used to encode the information required by workers to write
 * ACID events without requiring them to once more retrieve the data from the meta store db.
 */
public class AcidTable implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String databaseName;
  private final String tableName;
  private final boolean createPartitions;
  private final TableType tableType;
  private long transactionId;

  private Table table;

  AcidTable(String databaseName, String tableName, boolean createPartitions, TableType tableType) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.createPartitions = createPartitions;
    this.tableType = tableType;
  }

  /**
   * Returns {@code 0} until such a time that a {@link Transaction} has been acquired (when
   * {@link MutatorClient#newTransaction()} exits), at which point this will return the
   * {@link Transaction#getTransactionId() transaction id}.
   */
  public long getTransactionId() {
    return transactionId;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public boolean createPartitions() {
    return createPartitions;
  }

  /**
   * Returns {@code null} until such a time that the table described by the {@link #getDatabaseName() database_name}
   * {@code .}{@link #getTableName() table_name} has been resolved with the meta store database (when
   * {@link MutatorClient#connect()} exits), at which point this will then return the corresponding
   * {@link StorageDescriptor#getOutputFormat() OutputFormat}.
   */
  public String getOutputFormatName() {
    return table != null ? table.getSd().getOutputFormat() : null;
  }

  /**
   * Returns {@code 0} until such a time that the table described by the {@link #getDatabaseName() database_name}
   * {@code .}{@link #getTableName() table_name} has been resolved with the meta store database (when
   * {@link MutatorClient#connect()} exits), at which point this will then return the corresponding
   * {@link StorageDescriptor#getNumBuckets() total bucket count}.
   */
  public int getTotalBuckets() {
    return table != null ? table.getSd().getNumBuckets() : 0;
  }

  public TableType getTableType() {
    return tableType;
  }

  public String getQualifiedName() {
    return (databaseName + "." + tableName).toUpperCase();
  }

  /**
   * Returns {@code null} until such a time that the table described by the {@link #getDatabaseName() database_name}
   * {@code .}{@link #getTableName() table_name} has been resolved with the meta store database (when
   * {@link MutatorClient#connect()} exits), at which point this will then return the corresponding {@link Table}.
   * Provided as a convenience to API users who may wish to gather further meta data regarding the table without
   * connecting with the meta store once more.
   */
  public Table getTable() {
    return table;
  }

  void setTransactionId(long transactionId) {
    this.transactionId = transactionId;
  }

  void setTable(Table table) {
    if (!databaseName.equalsIgnoreCase(table.getDbName())) {
      throw new IllegalArgumentException("Incorrect database name.");
    }
    if (!tableName.equalsIgnoreCase(table.getTableName())) {
      throw new IllegalArgumentException("Incorrect table name.");
    }
    this.table = table;
  }

  @Override
  public String toString() {
    return "AcidTable [databaseName=" + databaseName + ", tableName=" + tableName + ", createPartitions="
        + createPartitions + ", tableType=" + tableType + ", outputFormatName=" + getOutputFormatName()
        + ", totalBuckets=" + getTotalBuckets() + ", transactionId=" + transactionId + "]";
  }

}