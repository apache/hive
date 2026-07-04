/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.testutil;

import org.apache.hadoop.hive.common.DatabaseName;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hive.search.exception.IndexException;
import org.apache.hive.search.index.Indexer;
import org.apache.hive.search.index.IndexManager;
import org.apache.hive.search.mapping.TableDocument;
import org.apache.hive.search.metastore.MetastoreEventListener;
import org.apache.hive.search.metastore.MetastoreTableMapper;

import java.io.IOException;
import java.util.List;

/** Applies {@link MetastoreEventListener.IndexTask} mutations to an {@link Indexer}. */
public final class IndexMutationApplier {
  private final IndexManager indexManager;
  private final Indexer indexer;

  public IndexMutationApplier(IndexManager indexManager, Indexer indexer) {
    this.indexManager = indexManager;
    this.indexer = indexer;
  }

  public void apply(MetastoreEventListener.IndexTask task)
      throws IndexException, IOException {
    if (!task.databasesToDrop.isEmpty()) {
      indexer.deleteDatabases(task.databasesToDrop.toArray(new DatabaseName[0]));
    }
    if (!task.tablesToDrop.isEmpty()) {
      String[] docIds = task.tablesToDrop.stream()
          .map(MetastoreTableMapper::tableId)
          .toList()
          .toArray(new String[0]);
      indexer.delete(docIds);
    }
    if (!task.tablesToAdd.isEmpty()) {
      List<TableDocument> newDocs = task.tablesToAdd.values().stream()
          .map(table -> MetastoreTableMapper.fromTable(table, indexManager.mapping()))
          .toList();
      indexer.addDocuments(newDocs);
    }
  }

  public void addTable(Table table) throws IndexException, IOException {
    MetastoreEventListener.IndexTask task = new MetastoreEventListener.IndexTask();
    TableName tableName = new TableName(table.getCatName(), table.getDbName(), table.getTableName());
    task.tablesToAdd.put(tableName, table);
    apply(task);
  }

  public void dropTable(TableName tableName) throws IndexException, IOException {
    MetastoreEventListener.IndexTask task = new MetastoreEventListener.IndexTask();
    task.tablesToDrop.add(tableName);
    apply(task);
  }

  public void replaceTable(Table before, Table after) throws IndexException, IOException {
    MetastoreEventListener.IndexTask task = new MetastoreEventListener.IndexTask();
    task.tablesToDrop.add(new TableName(before.getCatName(), before.getDbName(), before.getTableName()));
    task.tablesToAdd.put(new TableName(after.getCatName(), after.getDbName(), after.getTableName()), after);
    apply(task);
  }
}
