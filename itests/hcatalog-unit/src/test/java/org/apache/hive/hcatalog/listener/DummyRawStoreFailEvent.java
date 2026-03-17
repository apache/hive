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

package org.apache.hive.hcatalog.listener;

import java.util.List;

import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * An implementation {@link org.apache.hadoop.hive.metastore.RawStore}
 * with the ability to fail metastore events for the purpose of testing.
 * Events are expected to succeed by default and simply delegate to an
 * embedded ObjectStore object. The behavior can be changed based on
 * a flag by calling setEventSucceed().
 *
 */
public class DummyRawStoreFailEvent extends ObjectStore {

  private static boolean shouldEventSucceed = true;
  public static void setEventSucceed(boolean flag) {
    shouldEventSucceed = flag;
  }

  @Override
  public void createCatalog(Catalog cat) throws MetaException {
    if (shouldEventSucceed) {
      super.createCatalog(cat);
    } else {
      throw new RuntimeException("Failed event");
    }
  }

  @Override
  public void dropCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    if (shouldEventSucceed) {
      super.dropCatalog(catalogName);
    } else {
      throw new RuntimeException("Event failed.");
    }
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    if (shouldEventSucceed) {
      super.createDatabase(db);
    } else {
      throw new RuntimeException("Failed event");
    }
  }

  @Override
  public boolean dropDatabase(String catName, String dbName)
      throws NoSuchObjectException, MetaException {
    if (shouldEventSucceed) {
      return super.dropDatabase(catName, dbName);
    } else {
      throw new RuntimeException("Event failed.");
    }
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    if (shouldEventSucceed) {
      super.createTable(tbl);
    } else {
      throw new RuntimeException("Event failed.");
    }
  }

  @Override
  public boolean dropTable(String catName, String dbName, String tableName)
      throws MetaException, NoSuchObjectException,
      InvalidObjectException, InvalidInputException {
    if (shouldEventSucceed) {
      return super.dropTable(catName, dbName, tableName);
    } else {
      throw new RuntimeException("Event failed.");
    }
  }

  @Override
  public boolean dropPartition(String catName, String dbName, String tableName, String partName)
      throws MetaException, NoSuchObjectException,
      InvalidObjectException, InvalidInputException {
    if (shouldEventSucceed) {
      return super.dropPartition(catName, dbName, tableName, partName);
    } else {
      throw new RuntimeException("Event failed.");
    }
  }

  @Override
  public void dropPartitions(String catName, String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    if (shouldEventSucceed) {
      super.dropPartitions(catName, dbName, tblName, partNames);
    } else {
      throw new RuntimeException("Event failed.");
    }
  }

  @Override
  public Table alterTable(String catName, String dbName, String name, Table newTable, String queryValidWriteIds)
      throws InvalidObjectException, MetaException {
    if (shouldEventSucceed) {
      return super.alterTable(catName, dbName, name, newTable, queryValidWriteIds);
    } else {
      throw new RuntimeException("Event failed.");
    }
  }


  @Override
  public Partition alterPartition(String catName, String dbName, String tblName, List<String> partVals,
                             Partition newPart, String queryValidWriteIds) throws InvalidObjectException, MetaException {
    if (shouldEventSucceed) {
      return super.alterPartition(catName, dbName, tblName, partVals, newPart, queryValidWriteIds);
    } else {
      throw new RuntimeException("Event failed.");
    }
  }

  @Override
  public List<Partition> alterPartitions(String catName, String dbName, String tblName,
                              List<List<String>> partValsList, List<Partition> newParts,
                              long writeId, String queryValidWriteIds)
      throws InvalidObjectException, MetaException {
    if (shouldEventSucceed) {
      return super.alterPartitions(catName, dbName, tblName, partValsList, newParts, writeId, queryValidWriteIds);
    } else {
      throw new RuntimeException("Event failed.");
    }
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException,
      MetaException {
    if (shouldEventSucceed) {
      super.createFunction(func);
    } else {
      throw new RuntimeException("Event failed.");
    }
  }

  @Override
  public void dropFunction(String catName, String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException {
    if (shouldEventSucceed) {
      super.dropFunction(catName, dbName, funcName);
    } else {
      throw new RuntimeException("Event failed.");
    }
  }

  @Override
  public void cleanNotificationEvents(int olderThan) {
    if (!shouldEventSucceed) {
      //throw exception to simulate an issue with cleaner thread
      throw new RuntimeException("Dummy exception while cleaning notifications");
    }
    super.cleanNotificationEvents(olderThan);
  }

  @Override
  public void cleanWriteNotificationEvents(int olderThan) {
    if (!shouldEventSucceed) {
      //throw exception to simulate an issue with cleaner thread
      throw new RuntimeException("Dummy exception while cleaning write notifications");
    }
    super.cleanWriteNotificationEvents(olderThan);
  }
}
