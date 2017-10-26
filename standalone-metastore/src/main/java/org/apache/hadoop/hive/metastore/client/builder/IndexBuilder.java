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

import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder for indices.  You must supply the database name and table name (or table reference), a
 * name for the index, and whatever StorageDescriptorBuilder requires.  All other fields will be
 * given reasonable defaults.
 */
public class IndexBuilder extends StorageDescriptorBuilder<IndexBuilder> {
  private String dbName, tableName, indexName, indexTableName, handlerClass;
  private int createTime, lastAccessTime;
  private Map<String, String> indexParams;
  private boolean deferredRebuild;

  public IndexBuilder() {
    // Set some reasonable defaults
    indexParams = new HashMap<>();
    createTime = lastAccessTime = (int)(System.currentTimeMillis() / 1000);
    super.setChild(this);
  }

  public IndexBuilder setDbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  public IndexBuilder setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public IndexBuilder setDbAndTableName(Table table) {
    this.dbName = table.getDbName();
    this.tableName = table.getTableName();
    return this;
  }

  public IndexBuilder setCreateTime(int createTime) {
    this.createTime = createTime;
    return this;
  }

  public IndexBuilder setLastAccessTime(int lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
    return this;
  }

  public IndexBuilder setIndexParams(Map<String, String> indexParams) {
    this.indexParams = indexParams;
    return this;
  }

  public IndexBuilder addIndexParam(String key, String value) {
    indexParams.put(key, value);
    return this;
  }

  public IndexBuilder setIndexName(String indexName) {
    this.indexName = indexName;
    return this;
  }

  public IndexBuilder setIndexTableName(String indexTableName) {
    this.indexTableName = indexTableName;
    return this;
  }

  public IndexBuilder setHandlerClass(String handlerClass) {
    this.handlerClass = handlerClass;
    return this;
  }

  public IndexBuilder setDeferredRebuild(boolean deferredRebuild) {
    this.deferredRebuild = deferredRebuild;
    return this;
  }

  public Index build() throws MetaException {
    if (dbName == null || tableName == null || indexName == null) {
      throw new MetaException("You must provide database name, table name, and index name");
    }
    if (indexTableName == null) indexTableName = tableName + "_" + indexName + "_table";
    return new Index(indexName, handlerClass, dbName, tableName, createTime, lastAccessTime,
        indexTableName, buildSd(), indexParams, deferredRebuild);
  }
}
