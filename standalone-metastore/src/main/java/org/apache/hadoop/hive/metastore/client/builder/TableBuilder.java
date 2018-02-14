/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.client.builder;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Build a {@link Table}.  The database name and table name must be provided, plus whatever is
 * needed by the underlying {@link StorageDescriptorBuilder}.
 */
public class TableBuilder extends StorageDescriptorBuilder<TableBuilder> {
  private String dbName, tableName, owner, viewOriginalText, viewExpandedText, type;
  private List<FieldSchema> partCols;
  private int createTime, lastAccessTime, retention;
  private Map<String, String> tableParams;
  private boolean rewriteEnabled, temporary;

  public TableBuilder() {
    // Set some reasonable defaults
    tableParams = new HashMap<>();
    createTime = lastAccessTime = (int)(System.currentTimeMillis() / 1000);
    retention = 0;
    partCols = new ArrayList<>();
    type = TableType.MANAGED_TABLE.name();
    super.setChild(this);
  }

  public TableBuilder setDbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  public TableBuilder setDbName(Database db) {
    this.dbName = db.getName();
    return this;
  }

  public TableBuilder setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public TableBuilder setOwner(String owner) {
    this.owner = owner;
    return this;
  }

  public TableBuilder setViewOriginalText(String viewOriginalText) {
    this.viewOriginalText = viewOriginalText;
    return this;
  }

  public TableBuilder setViewExpandedText(String viewExpandedText) {
    this.viewExpandedText = viewExpandedText;
    return this;
  }

  public TableBuilder setType(String type) {
    this.type = type;
    return this;
  }

  public TableBuilder setPartCols(List<FieldSchema> partCols) {
    this.partCols = partCols;
    return this;
  }

  public TableBuilder addPartCol(String name, String type, String comment) {
    partCols.add(new FieldSchema(name, type, comment));
    return this;
  }

  public TableBuilder addPartCol(String name, String type) {
    return addPartCol(name, type, "");
  }

  public TableBuilder setCreateTime(int createTime) {
    this.createTime = createTime;
    return this;
  }

  public TableBuilder setLastAccessTime(int lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
    return this;
  }

  public TableBuilder setRetention(int retention) {
    this.retention = retention;
    return this;
  }

  public TableBuilder setTableParams(Map<String, String> tableParams) {
    this.tableParams = tableParams;
    return this;
  }

  public TableBuilder addTableParam(String key, String value) {
    if (tableParams == null) tableParams = new HashMap<>();
    tableParams.put(key, value);
    return this;
  }

  public TableBuilder setRewriteEnabled(boolean rewriteEnabled) {
    this.rewriteEnabled = rewriteEnabled;
    return this;
  }

  public TableBuilder setTemporary(boolean temporary) {
    this.temporary = temporary;
    return this;
  }

  public TableBuilder fromIndex(Index index) {
    dbName = index.getDbName();
    tableName = index.getIndexTableName();
    setCols(index.getSd().getCols());
    setType(TableType.INDEX_TABLE.name());
    return this;
  }

  public Table build() throws MetaException {
    if (dbName == null || tableName == null) {
      throw new MetaException("You must set the database and table name");
    }
    if (owner == null) {
      try {
        owner = SecurityUtils.getUser();
      } catch (IOException e) {
        throw MetaStoreUtils.newMetaException(e);
      }
    }
    Table t = new Table(tableName, dbName, owner, createTime, lastAccessTime, retention, buildSd(),
        partCols, tableParams, viewOriginalText, viewExpandedText, type);
    if (rewriteEnabled) t.setRewriteEnabled(true);
    if (temporary) t.setTemporary(temporary);
    return t;
  }

}
