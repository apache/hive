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
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.HMSHandlerProxyFactory;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.hive.HiveActor;
import org.apache.iceberg.hive.HiveLock;
import org.apache.iceberg.hive.MetastoreLock;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HMSCatalogActor implements HiveActor {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalogActor.class);
  /** The actor name (catalog). */
  private final String name;
  /** The configuration (the Hadoop).  */
  private final Configuration conf;
  /** The client pool. */
  private final ObjectPool<IHMSHandler> handlers;

  private static IHMSHandler getHandler(Configuration configuration) throws MetaException {
    IHMSHandler hmsHandler = new HMSHandler("HMSHandler", configuration);
    try {
      return HMSHandlerProxyFactory.getProxy(configuration, hmsHandler, true);
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  public HMSCatalogActor(String name, Configuration configuration) {
    this.name = name;
    this.conf = configuration;
    this.handlers = new GenericObjectPool<>(new BasePooledObjectFactory<IHMSHandler>() {
      @Override
      public IHMSHandler create() throws Exception {
        return getHandler(new Configuration(conf));
      }

      @Override
      public PooledObject<IHMSHandler> wrap(IHMSHandler ihmsHandler) {
        return new DefaultPooledObject<>(ihmsHandler);
      }
    });
  }

  @FunctionalInterface
  interface Action<R> {
    R execute(IHMSHandler handler) throws TException;
  }

  private <R> R run(Action<R> action) throws TException {
    IHMSHandler handler = null;
    try {
      try {
        handler = handlers.borrowObject();
      } catch (Exception e) {
        throw new TException("run/borrowObject", e);
      }
      return action.execute(handler);
    } finally {
      if (handler != null) {
        try {
          handlers.returnObject(handler);
        } catch (Exception e) {
          LOG.error("run/returnObject", e);
        }
      }
    }
  }

  @FunctionalInterface
  interface VoidAction {
    void execute(IHMSHandler handler) throws TException;
  }

  private void runVoid(VoidAction action) throws TException {
    IHMSHandler handler = null;
    try {
      try {
        handler = handlers.borrowObject();
      } catch (Exception e) {
        throw new TException("runVoid/borrowObject", e);
      }
      action.execute(handler);
    } finally {
      if (handler != null) {
        try {
          handlers.returnObject(handler);
        } catch (Exception e) {
          LOG.error("runVoid/returnObject", e);
        }
      }
    }
  }


  @Override
  public HiveActor initialize(Map<String, String> properties) {
    return this;
  }

  @Override
  public void alterDatabase(Namespace namespace, Database database) throws TException {
    runVoid(h -> h.alter_database(namespace.level(0), database));
  }

  @Override
  public void alterTable(String databaseName, String tableName, Table table) throws TException {
    runVoid(h -> h.alter_table(databaseName, tableName, table));
  }

  /** HiveTableOperations.NO_LOCK_EXPECTED_KEY */
  static final String NO_LOCK_EXPECTED_KEY = "expected_parameter_key";
  /** HiveTableOperations.NO_LOCK_EXPECTED_VALUE */
  static final String NO_LOCK_EXPECTED_VALUE = "expected_parameter_value";

  @Override
  public void alterTable(String databaseName, String tableName, Table hmsTable, String metadataLocation)
      throws TException {
    runVoid(h -> h.alter_table_with_environment_context(
        databaseName,
        tableName,
        hmsTable,
        new EnvironmentContext(
            metadataLocation != null ? ImmutableMap.of(
                /* HiveTableOperations.*/NO_LOCK_EXPECTED_KEY,
                BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
                /* HiveTableOperations.*/NO_LOCK_EXPECTED_VALUE,
                metadataLocation)
                : ImmutableMap.of())));
  }

  @Override
  public Database getDatabase(Namespace namespace) throws TException {
    return run(h -> h.get_database(namespace.level(0)));
  }

  @Override
  public List<String> listTableNames(String database) throws TException {
    return run(h -> h.get_all_tables(database));
  }

  @Override
  public List<Table> listTables(String database, List<String> tableNames) throws TException {
    if (tableNames.isEmpty()) {
      return Collections.emptyList();
    }
    GetTablesRequest query = new GetTablesRequest();
    query.setDbName(database);
    query.setCatName(name);
    query.setTblNames(tableNames);
    GetTablesResult result = run(h -> h.get_table_objects_by_name_req(query));
    return result.getTables();
  }

  @Override
  public void createTable(Table table) throws TException {
    runVoid(h -> h.create_table(table));
  }

  @Override
  public void dropTable(String databaseName, String tableName) throws TException {
    runVoid(h -> h.drop_table(databaseName, tableName, true));
  }

  @Override
  public Table getTable(String databaseName, String tableName) throws TException {
    GetTableRequest request = new GetTableRequest();
    if (databaseName == null) {
      throw new NullPointerException("no db name!");
    }
    request.setDbName(databaseName);
    request.setCatName(name);
    request.setTblName(tableName);
    return run(h -> h.get_table_core(request));
  }

  @Override
  public void createNamespace(Database database) throws TException {
    runVoid(h -> h.create_database(database));
  }

  @Override
  public List<String> listNamespaceNames() throws TException {
    return run(h -> h.get_all_databases());
  }

  @Override
  public void dropNamespace(Namespace namespace) throws TException {
    String dbName = MetaStoreUtils.prependNotNullCatToDbName(name, namespace.level(0));
    runVoid(h -> h.drop_database(dbName, false, false));
  }

  @Override
  public void heartbeat(long txnId, long lockId) throws TException {
    HeartbeatRequest request = new HeartbeatRequest();
    request.setLockid(lockId);
    request.setTxnid(txnId);
    runVoid(h -> h.heartbeat(request));
  }

  @Override
  public HiveLock newLock(TableMetadata metadata, String catalogName, String database, String tableName) {
    return new MetastoreLock(conf, this, catalogName, database, tableName);
  }

  @Override
  public LockResponse checkLock(long lockId) throws TException {
    return run(h -> h.check_lock(new CheckLockRequest(lockId)));
  }

  @Override
  public LockResponse lock(LockRequest request) throws TException {
    return run(h -> h.lock(request));
  }

  @Override
  public void unlock(long lockId) throws TException {
    runVoid(h -> h.unlock(new UnlockRequest(lockId)));
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest request) throws TException {
    return run(h -> h.show_locks(request));
  }
}
