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
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * ImportTableDesc.
 *
 */
public class ImportTableDesc {
  private String dbName = null;
  private Table table = null;
  private CreateTableDesc createTblDesc = null;
  private CreateViewDesc createViewDesc = null;

  public enum TYPE { TABLE, VIEW };

  public ImportTableDesc(String dbName, Table table) throws Exception {
    this.dbName = dbName;
    this.table = table;

    switch (getDescType()) {
      case TABLE:
        this.createTblDesc = new CreateTableDesc(dbName,
                table.getTableName(),
                false, // isExternal: set to false here, can be overwritten by the IMPORT stmt
                table.isTemporary(),
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
                (null == table.getSd().getSkewedInfo()) ? null : table.getSd().getSkewedInfo()
                        .getSkewedColNames(),
                (null == table.getSd().getSkewedInfo()) ? null : table.getSd().getSkewedInfo()
                        .getSkewedColValues(),
                null,
                null,
                null,
                null);
        this.createTblDesc.setStoredAsSubDirectories(table.getSd().isStoredAsSubDirectories());
        break;
      case VIEW:
        String[] qualViewName = { dbName, table.getTableName() };
        String dbDotView = BaseSemanticAnalyzer.getDotName(qualViewName);
        if (table.isMaterializedView()) {
          this.createViewDesc = new CreateViewDesc(dbDotView,
                  table.getAllCols(),
                  null, // comment passed as table params
                  table.getParameters(),
                  table.getPartColNames(),
                  false,false,false,false,
                  table.getSd().getInputFormat(),
                  table.getSd().getOutputFormat(),
                  null, // location: set to null here, can be overwritten by the IMPORT stmt
                  table.getSd().getSerdeInfo().getSerializationLib(),
                  null, // storagehandler passed as table params
                  table.getSd().getSerdeInfo().getParameters());
        } else {
          this.createViewDesc = new CreateViewDesc(dbDotView,
                  table.getAllCols(),
                  null, // comment passed as table params
                  table.getParameters(),
                  table.getPartColNames(),
                  false,false,false,
                  table.getSd().getInputFormat(),
                  table.getSd().getOutputFormat(),
                  table.getSd().getSerdeInfo().getSerializationLib());
        }

        this.setViewAsReferenceText(dbName, table);
        this.createViewDesc.setPartCols(table.getPartCols());
        break;
      default:
        throw new HiveException("Invalid table type");
    }
  }

  public TYPE getDescType() {
    if (table.isView() || table.isMaterializedView()) {
      return TYPE.VIEW;
    }
    return TYPE.TABLE;
  }

  public void setViewAsReferenceText(String dbName, Table table) {
    String originalText = table.getViewOriginalText();
    String expandedText = table.getViewExpandedText();

    if (!dbName.equals(table.getDbName())) {
      // TODO: If the DB name doesn't match with the metadata from dump, then need to rewrite the original and expanded
      // texts using new DB name. Currently it refers to the source database name.
    }

    this.createViewDesc.setViewOriginalText(originalText);
    this.createViewDesc.setViewExpandedText(expandedText);
  }

  public void setReplicationSpec(ReplicationSpec replSpec) {
    switch (getDescType()) {
      case TABLE:
        createTblDesc.setReplicationSpec(replSpec);
        break;
      case VIEW:
        createViewDesc.setReplicationSpec(replSpec);
        break;
    }
  }

  public void setExternal(boolean isExternal) {
    if (TYPE.TABLE.equals(getDescType())) {
      createTblDesc.setExternal(isExternal);
    }
  }

  public boolean isExternal() {
    if (TYPE.TABLE.equals(getDescType())) {
      return createTblDesc.isExternal();
    }
    return false;
  }

  public void setLocation(String location) {
    switch (getDescType()) {
      case TABLE:
        createTblDesc.setLocation(location);
        break;
      case VIEW:
        createViewDesc.setLocation(location);
        break;
    }
  }

  public String getLocation() {
    switch (getDescType()) {
      case TABLE:
        return createTblDesc.getLocation();
      case VIEW:
        return createViewDesc.getLocation();
    }
    return null;
  }

  public void setTableName(String tableName) throws SemanticException {
    switch (getDescType()) {
      case TABLE:
        createTblDesc.setTableName(tableName);
        break;
      case VIEW:
        String[] qualViewName = { dbName, tableName };
        String dbDotView = BaseSemanticAnalyzer.getDotName(qualViewName);
        createViewDesc.setViewName(dbDotView);
        break;
    }
  }

  public String getTableName() throws SemanticException {
    switch (getDescType()) {
      case TABLE:
        return createTblDesc.getTableName();
      case VIEW:
        String dbDotView = createViewDesc.getViewName();
        String[] names = Utilities.getDbTableName(dbDotView);
        return names[1]; // names[0] have the Db name and names[1] have the view name
    }
    return null;
  }

  public List<FieldSchema> getPartCols() {
    switch (getDescType()) {
      case TABLE:
        return createTblDesc.getPartCols();
      case VIEW:
        return createViewDesc.getPartCols();
    }
    return null;
  }

  public List<FieldSchema> getCols() {
    switch (getDescType()) {
      case TABLE:
        return createTblDesc.getCols();
      case VIEW:
        return createViewDesc.getSchema();
    }
    return null;
  }

  public Map<String, String> getTblProps() {
    switch (getDescType()) {
      case TABLE:
        return createTblDesc.getTblProps();
      case VIEW:
        return createViewDesc.getTblProps();
    }
    return null;
  }

  public String getInputFormat() {
    switch (getDescType()) {
      case TABLE:
        return createTblDesc.getInputFormat();
      case VIEW:
        return createViewDesc.getInputFormat();
    }
    return null;
  }

  public String getOutputFormat() {
    switch (getDescType()) {
      case TABLE:
        return createTblDesc.getOutputFormat();
      case VIEW:
        return createViewDesc.getOutputFormat();
    }
    return null;
  }

  public String getSerName() {
    switch (getDescType()) {
      case TABLE:
        return createTblDesc.getSerName();
      case VIEW:
        return createViewDesc.getSerde();
    }
    return null;
  }

  public Map<String, String> getSerdeProps() {
    switch (getDescType()) {
      case TABLE:
        return createTblDesc.getSerdeProps();
      case VIEW:
        return createViewDesc.getSerdeProps();
    }
    return null;
  }

  public List<String> getBucketCols() {
    if (TYPE.TABLE.equals(getDescType())) {
      return createTblDesc.getBucketCols();
    }
    return null;
  }

  public List<Order> getSortCols() {
    if (TYPE.TABLE.equals(getDescType())) {
      return createTblDesc.getSortCols();
    }
    return null;
  }

  /**
   * @param replaceMode Determine if this CreateTable should behave like a replace-into alter instead
   */
  public void setReplaceMode(boolean replaceMode) {
    switch (getDescType()) {
      case TABLE:
        createTblDesc.setReplaceMode(replaceMode);
        break;
      case VIEW:
        createViewDesc.setReplace(replaceMode);
    }
  }

  public String getDatabaseName() {
    return dbName;
  }

  public Task<? extends Serializable> getCreateTableTask(HashSet<ReadEntity> inputs, HashSet<WriteEntity> outputs,
      HiveConf conf) {
    switch (getDescType()) {
      case TABLE:
        return TaskFactory.get(new DDLWork(inputs, outputs, createTblDesc), conf);
      case VIEW:
        return TaskFactory.get(new DDLWork(inputs, outputs, createViewDesc), conf);
    }
    return null;
  }

  /**
   * @return whether this table is actually a view
   */
  public boolean isView() {
    return table.isView();
  }

  public boolean isMaterializedView() {
    return table.isMaterializedView();
  }

  public TableType tableType() {
    if (isView()) {
      return TableType.VIRTUAL_VIEW;
    } else if (isMaterializedView()) {
      return TableType.MATERIALIZED_VIEW;
    }
    return TableType.MANAGED_TABLE;
  }
}
