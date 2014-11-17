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

package org.apache.hadoop.hive.metastore;

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Default no-op implementation of the MetaStoreFilterHook that returns the result as is
 */
public class DefaultMetaStoreFilterHookImpl implements MetaStoreFilterHook {

  public DefaultMetaStoreFilterHookImpl(HiveConf conf) {
  }

  @Override
  public List<String> filterDatabases(List<String> dbList) {
    return dbList;
  }

  @Override
  public Database filterDatabase(Database dataBase) throws NoSuchObjectException {
    return dataBase;
  }

  @Override
  public List<String> filterTableNames(String dbName, List<String> tableList) {
    return tableList;
  }

  @Override
  public Table filterTable(Table table)  throws NoSuchObjectException {
    return table;
  }

  @Override
  public List<Table> filterTables(List<Table> tableList) {
    return tableList;
  }

  @Override
  public List<Partition> filterPartitions(List<Partition> partitionList) {
    return partitionList;
  }

  @Override
  public List<PartitionSpec> filterPartitionSpecs(
      List<PartitionSpec> partitionSpecList) {
    return partitionSpecList;
  }

  @Override
  public Partition filterPartition(Partition partition)  throws NoSuchObjectException {
    return partition;
  }

  @Override
  public List<String> filterPartitionNames(String dbName, String tblName,
      List<String> partitionNames) {
    return partitionNames;
  }

  @Override
  public Index filterIndex(Index index)  throws NoSuchObjectException {
    return index;
  }

  @Override
  public List<String> filterIndexNames(String dbName, String tblName,
      List<String> indexList) {
    return indexList;
  }

  @Override
  public List<Index> filterIndexes(List<Index> indexeList) {
    return indexeList;
  }
}
