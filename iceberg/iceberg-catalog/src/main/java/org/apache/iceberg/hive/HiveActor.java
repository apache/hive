/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.thrift.TException;

/**
 * Acts as the Hive client for the HiveCatalog benefit.
 */
public interface HiveActor {
  HiveActor initialize(Map<String, String> properties);

  Database getDatabase(Namespace namespace) throws TException, InterruptedException;

  void alterDatabase(Namespace namespace, Database database) throws TException, InterruptedException;

  List<String> listTableNames(String database) throws TException, InterruptedException;

  Table getTable(String fromDatabase, String fromName) throws TException, InterruptedException;

  List<Table> listTables(String database, List<String> tableNames) throws TException, InterruptedException;

  void alterTable(String fromDatabase, String fromName, Table table) throws TException, InterruptedException;

  void alterTable(String database, String tableName, Table hmsTable, String expectedMetadataLocation)
      throws TException, InterruptedException;

  void createTable(Table table) throws TException, InterruptedException;

  void dropTable(String databaseName, String tableName) throws TException, InterruptedException;

  void createNamespace(Database database)  throws TException, InterruptedException;

  List<String> listNamespaceNames() throws TException, InterruptedException;

  void dropNamespace(Namespace namespace) throws TException, InterruptedException;

  HiveLock newLock(TableMetadata metadata, String catalogName, String database, String tableName);

  void heartbeat(long txnId, long lockId) throws TException, InterruptedException;

  LockResponse checkLock(long lockId) throws TException, InterruptedException;

  LockResponse lock(LockRequest request) throws TException, InterruptedException;

  void unlock(long lockId) throws TException, InterruptedException;

  ShowLocksResponse showLocks(ShowLocksRequest request) throws TException, InterruptedException;
}
