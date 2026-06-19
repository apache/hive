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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builder for {@link Partition}.  The only requirements are 1. (database name and table name) or table
 * reference; 2. partition values; 3. whatever {@link StorageDescriptorBuilder} requires.
 */
public class PartitionBuilder extends StorageDescriptorBuilder<PartitionBuilder> {
  private String catName, dbName, tableName;
  private int createTime, lastAccessTime;
  private Map<String, String> partParams;
  private List<String> values;

  public PartitionBuilder() {
    // Set some reasonable defaults
    partParams = new HashMap<>();
    createTime = lastAccessTime = (int)(System.currentTimeMillis() / 1000);
    dbName = Warehouse.DEFAULT_DATABASE_NAME;
    super.setChild(this);
  }

  public PartitionBuilder setDbName(String dbName) {
    this.dbName = dbName;
    return this;
  }

  public PartitionBuilder setTableName(String tableName) {
    this.tableName = tableName;
    return this;
  }

  public PartitionBuilder inTable(Table table) {
    this.dbName = table.getDbName();
    this.tableName = table.getTableName();
    this.catName = table.getCatName();
    setCols(table.getSd().getCols());
    return this;
  }

  public PartitionBuilder setValues(List<String> values) {
    this.values = values;
    return this;
  }

  public PartitionBuilder addValue(String value) {
    if (values == null) values = new ArrayList<>();
    values.add(value);
    return this;
  }

  public PartitionBuilder setCreateTime(int createTime) {
    this.createTime = createTime;
    return this;
  }

  public PartitionBuilder setLastAccessTime(int lastAccessTime) {
    this.lastAccessTime = lastAccessTime;
    return this;
  }

  public PartitionBuilder setPartParams(Map<String, String> partParams) {
    this.partParams = partParams;
    return this;
  }

  public PartitionBuilder addPartParam(String key, String value) {
    if (partParams == null) partParams = new HashMap<>();
    partParams.put(key, value);
    return this;
  }

  public Partition build(Configuration conf) throws MetaException {
    if (tableName == null) {
      throw new MetaException("table name must be provided");
    }
    if (values == null) throw new MetaException("You must provide partition values");
    if (catName == null) catName = MetaStoreUtils.getDefaultCatalog(conf);
    Partition p = new Partition(values, dbName, tableName, createTime, lastAccessTime, buildSd(),
        partParams);
    p.setCatName(catName);
    return p;
  }

  public Partition addToTable(IMetaStoreClient client, Configuration conf) throws TException {
    Partition p = build(conf);
    client.add_partition(p);
    return p;
  }
}
