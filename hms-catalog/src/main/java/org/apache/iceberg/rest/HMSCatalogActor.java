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

package org.apache.iceberg.rest;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.HMSHandlerProxyFactory;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hive.HiveActor;
import org.apache.thrift.TException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.hive.HiveLock;
import org.apache.iceberg.hive.MetastoreLock;

public class HMSCatalogActor implements HiveActor {
  /** The actor name (catalog). */
  private final String name;
  /** The configuration (the Hadoop).  */
  private final Configuration conf;
  private IHMSHandler hmsHandler;
  private RawStore rawStore;

  public HMSCatalogActor(String name, Configuration configuration) {
    this.name = name;
    this.conf = configuration;
  }

  @Override
  public HiveActor initialize(Map<String, String> properties) {
    return this;
  }

  @Override
  public void alterDatabase(Namespace namespace, Database database) throws TException {
    getMS().alterDatabase(name, namespace.level(0), database);
  }

  @Override
  public void alterTable(String databaseName, String tableName, Table table) throws TException {
    getMS().alterTable(name, databaseName, tableName, table, null);
  }

  @Override
  public void alterTable(String databaseName, String tableName, Table hmsTable, String metadataLocation)
      throws TException {
    getMS().alterTable(name, databaseName, tableName, hmsTable, metadataLocation);
  }

  @Override
  public Database getDatabase(Namespace namespace) throws TException {
    return getMS().getDatabase(name, namespace.level(0));
  }

  @Override
  public List<String> listTableNames(String database) throws TException {
    return getMS().getAllTables(name, database);
  }

  @Override
  public List<Table> listTables(String database, List<String> tableNames) throws TException {
    return tableNames.isEmpty() ? Collections.emptyList() : getMS().getTableObjectsByName(name, database, tableNames);
  }

  @Override
  public void createTable(Table table) throws TException {
    getMS().createTable(table);
  }

  @Override
  public void dropTable(String databaseName, String tableName) throws TException {
    getMS().dropTable(name, databaseName, tableName);
  }

  @Override
  public Table getTable(String databaseName, String tableName) throws TException {
    return getMS().getTable(name, databaseName, tableName);
  }

  @Override
  public void createNamespace(Database database) throws TException {
    final IHMSHandler handler = getHandler();
    handler.create_database(database);
  }

  @Override
  public List<String> listNamespaceNames() throws TException {
    return  getMS().getAllDatabases(name);
  }

  @Override
  public void dropNamespace(Namespace namespace) throws TException {
    String dbName = MetaStoreUtils.prependNotNullCatToDbName(name, namespace.level(0));
    IHMSHandler handler = getHandler();
    handler.drop_database(dbName, false, false);
  }

  @Override
  public void heartbeat(long txnId, long lockId) throws TException {
    final IHMSHandler handler = getHandler();
    HeartbeatRequest request = new HeartbeatRequest();
    request.setLockid(lockId);
    request.setTxnid(txnId);
    handler.heartbeat(request);
  }

  @Override
  public HiveLock newLock(TableMetadata metadata, String catalogName, String database, String tableName) {
    return new MetastoreLock(conf, this, catalogName, database, tableName);
  }

  @Override
  public LockResponse checkLock(long lockId) throws TException {
    final IHMSHandler handler = getHandler();
    return handler.check_lock(new CheckLockRequest(lockId));
  }

  @Override
  public LockResponse lock(LockRequest request) throws TException {
    final IHMSHandler handler = getHandler();
    return handler.lock(request);
  }

  @Override
  public void unlock(long lockId) throws TException {
    final IHMSHandler handler = getHandler();
    handler.unlock(new UnlockRequest(lockId));
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest request) throws TException {
    final IHMSHandler handler = getHandler();
    return handler.show_locks(request);
  }

  private IHMSHandler getHandler() throws MetaException {
    if (hmsHandler == null) {
      hmsHandler = new HMSHandler("JSON server", conf);
      try {
        hmsHandler = HMSHandlerProxyFactory.getProxy(conf, hmsHandler, true);
      } catch (MetaException e) {
        throw new RuntimeException(e);
      }
    }
    return hmsHandler;
  }

  private RawStore getMS() {
    if (rawStore == null) {
      try {
        rawStore = getHandler().getMS();
      } catch (MetaException e) {
        throw new RuntimeException(e);
      }
    }
    return rawStore;
  }

}
