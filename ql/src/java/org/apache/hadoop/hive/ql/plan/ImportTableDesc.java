/*
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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.metric.ReplicationMetricCollector;

/**
 * ImportTableDesc.
 *
 */
public class ImportTableDesc {
  private String dbName = null;
  private CreateTableDesc createTblDesc = null;

  public ImportTableDesc(String dbName, Table table) throws Exception {
    if (table.getTableType() == TableType.VIRTUAL_VIEW || table.getTableType() == TableType.MATERIALIZED_VIEW) {
      throw new IllegalStateException("Trying to import view or materialized view: " + table.getTableName());
    }

    this.dbName = dbName;
    TableName tableName = HiveTableName.ofNullable(table.getTableName(), dbName);

    this.createTblDesc = new CreateTableDesc(tableName,
        false, // isExternal: set to false here, can be overwritten by the IMPORT stmt
        false,
        table.getSd().getCols(),
        table.getPartitionKeys(),
        table.getSd().getBucketCols(),
        table.getSd().getSortCols(),
        table.getSd().getNumBuckets(),
        null, null, null, null, null, // these 5 delims passed as serde params
        null, // comment passed as table params
        table.getSd().getInputFormat(),
        table.getSd().getOutputFormat(),
        null, // location: set to null here, can be overwritten by the IMPORT stmt
        table.getSd().getSerdeInfo().getSerializationLib(),
        null, // storagehandler passed as table params
        table.getSd().getSerdeInfo().getParameters(),
        table.getParameters(), false,
        (null == table.getSd().getSkewedInfo()) ? null : table.getSd().getSkewedInfo().getSkewedColNames(),
        (null == table.getSd().getSkewedInfo()) ? null : table.getSd().getSkewedInfo().getSkewedColValues(),
        null,
        null,
        null,
        null,
        null,
        null,
        table.getColStats(),
        table.getTTable().getWriteId());
    this.createTblDesc.setStoredAsSubDirectories(table.getSd().isStoredAsSubDirectories());
  }

  public void setReplicationSpec(ReplicationSpec replSpec) {
    createTblDesc.setReplicationSpec(replSpec);
  }

  public void setExternal(boolean isExternal) {
    createTblDesc.setExternal(isExternal);
  }

  public boolean isExternal() {
    return createTblDesc.isExternal();
  }

  public void setLocation(String location) {
    createTblDesc.setLocation(location);
  }

  public String getLocation() {
    return createTblDesc.getLocation();
  }

  public void setTableName(TableName tableName) throws SemanticException {
    createTblDesc.setTableName(tableName);
  }

  public String getTableName() throws SemanticException {
    return createTblDesc.getFullTableName().getTable();
  }

  public List<FieldSchema> getPartCols() {
    return createTblDesc.getPartCols();
  }

  public List<FieldSchema> getCols() {
    return createTblDesc.getCols();
  }

  public Map<String, String> getTblProps() {
    return createTblDesc.getTblProps();
  }

  public String getInputFormat() {
    return createTblDesc.getInputFormat();
  }

  public String getOutputFormat() {
    return createTblDesc.getOutputFormat();
  }

  public String getSerName() {
    return createTblDesc.getSerName();
  }

  public Map<String, String> getSerdeProps() {
    return createTblDesc.getSerdeProps();
  }

  public List<String> getBucketCols() {
    return createTblDesc.getBucketCols();
  }

  public List<Order> getSortCols() {
    return createTblDesc.getSortCols();
  }

  /**
   * @param replaceMode Determine if this CreateTable should behave like a replace-into alter instead
   */
  public void setReplaceMode(boolean replaceMode) {
    createTblDesc.setReplaceMode(replaceMode);
  }

  public String getDatabaseName() {
    return dbName;
  }

  public Task<?> getCreateTableTask(Set<ReadEntity> inputs, Set<WriteEntity> outputs, HiveConf conf) {
    return TaskFactory.get(new DDLWork(inputs, outputs, createTblDesc), conf);
  }

  public Task<?> getCreateTableTask(Set<ReadEntity> inputs, Set<WriteEntity> outputs, HiveConf conf,
                                    boolean isReplication, String dumpRoot,
                                    ReplicationMetricCollector metricCollector, boolean executeInParallel) {
    return TaskFactory.get(new DDLWork(inputs, outputs, createTblDesc, isReplication,
            dumpRoot, metricCollector, executeInParallel), conf);
  }

  public TableType tableType() {
    return TableType.MANAGED_TABLE;
  }

  public Table toTable(HiveConf conf) throws Exception {
    return createTblDesc.toTable(conf);
  }

  public void setReplWriteId(Long replWriteId) {
    this.createTblDesc.setReplWriteId(replWriteId);
  }

  public void setOwnerName(String ownerName) {
    createTblDesc.setOwnerName(ownerName);
  }

  public Long getReplWriteId() {
    return this.createTblDesc.getReplWriteId();
  }

  public void setForceOverwriteTable(){
    this.createTblDesc.getReplicationSpec().setForceOverwrite(true);
  }
}
