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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.thrift.TException;

/**
 * Acts as the Hive client for the HiveCatalog benefit.
 */
public class HiveCatalogActor implements HiveActor {
  /** The actor name (catalog). */
  private final String name;
  /** The configuration (the Hadoop).  */
  private final Configuration conf;
  /** The client pool. */
  private ClientPool<IMetaStoreClient, TException> clients;

  public HiveCatalogActor(String name, Configuration configuration) {
    this.name = name;
    this.conf = configuration;
  }

  @Override public String toString() {
    return getClass().getSimpleName() + "{" + name + "}";
  }

  @VisibleForTesting
  ClientPool<IMetaStoreClient, TException> clientPool() {
    return clients;
  }

  protected ClientPool<IMetaStoreClient, TException> createPool(Map<String, String> properties) {
    return new CachedClientPool(conf, properties);
  }

  protected Configuration getConf() {
    return conf;
  }

  @Override
  public HiveActor initialize(Map<String, String> properties) {
    this.clients = createPool(properties);
    return this;
  }

  @Override
  public Database getDatabase(Namespace namespace) throws TException, InterruptedException {
    return clients.run(client -> client.getDatabase(namespace.level(0)));
  }

  @Override
  public void alterDatabase(Namespace namespace, Database database) throws TException, InterruptedException {
    clients.run(client -> {
      client.alterDatabase(namespace.level(0), database);
      return null;
    });
  }

  @Override
  public List<String> listTableNames(String database) throws TException, InterruptedException {
    return clients.run(client -> client.getAllTables(database));
  }

  @Override
  public Table getTable(String fromDatabase, String fromName) throws TException, InterruptedException {
    return clients.run(client -> client.getTable(fromDatabase, fromName));
  }

  @Override
  public List<Table> listTables(String database, List<String> tableNames)
      throws TException, InterruptedException {
    return clients.run(client -> client.getTableObjectsByName(database, tableNames));
  }

  @Override
  public void alterTable(String fromDatabase, String fromName, Table table)
      throws TException, InterruptedException {
    clients.run(client -> {
      MetastoreUtil.alterTable(client, fromDatabase, fromName, table);
      return null;
    });
  }

  @Override
  public void alterTable(String database, String tableName, Table hmsTable, String expectedMetadataLocation)
      throws TException, InterruptedException {
    clients.run(
        client -> {
          MetastoreUtil.alterTable(
              client,
              database,
              tableName,
              hmsTable,
              expectedMetadataLocation != null ?
                  ImmutableMap.of(
                      HiveTableOperations.NO_LOCK_EXPECTED_KEY,
                      BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
                      HiveTableOperations.NO_LOCK_EXPECTED_VALUE,
                      expectedMetadataLocation) :
                  ImmutableMap.of());
          return null;
        });
  }

  @Override
  public void createTable(Table table) throws TException, InterruptedException {
    clients.run(client -> {
      client.createTable(table);
      return null;
    });
  }

  @Override
  public void dropTable(String databaseName, String tableName) throws TException, InterruptedException {
    clients.run(client -> {
      client.dropTable(databaseName, tableName,
          false /* do not delete data */,
          false /* throw NoSuchObjectException if the table doesn't exist */);
      return null;
    });
  }

  @Override
  public void createNamespace(Database database)
      throws TException, InterruptedException {
    clients.run(client -> {
      client.createDatabase(database);
      return null;
    });
  }

  @Override
  public List<String> listNamespaceNames() throws TException, InterruptedException {
    return clients.run(IMetaStoreClient::getAllDatabases);
  }

  @Override
  public void dropNamespace(Namespace namespace) throws TException, InterruptedException {
    clients.run(client -> {
      client.dropDatabase(namespace.level(0),
          false /* deleteData */,
          false /* ignoreUnknownDb */,
          false /* cascade */);
      return null;
    });
  }

  @Override
  public void heartbeat(long txnId, long lockId) throws TException, InterruptedException {
    clients.run(
        client -> {
          client.heartbeat(txnId, lockId);
          return null;
        });
  }

  @Override
  public HiveLock newLock(TableMetadata metadata, String catalogName, String database, String tableName) {
    return new MetastoreLock(conf, this, catalogName, database, tableName);
  }

  @Override
  public LockResponse checkLock(long lockId) throws TException, InterruptedException {
    return clients.run(client -> client.checkLock(lockId));
  }

  @Override
  public LockResponse lock(LockRequest request) throws TException, InterruptedException {
    return clients.run(client -> client.lock(request));
  }

  @Override
  public void unlock(long lockId) throws TException, InterruptedException {
    clients.run(client -> {
      client.unlock(lockId);
      return null;
    });
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest request) throws TException, InterruptedException {
    return clients.run(client -> client.showLocks(request));
  }
}
