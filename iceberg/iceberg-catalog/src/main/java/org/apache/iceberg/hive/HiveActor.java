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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
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
public abstract class HiveActor {
  /**
   * The actor class name property key.
   */
  public static final String ACTOR_CLASSNAME = "hive.metastore.catalog.actor.class";
  /**
   * The actor name (catalog).
   */
  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final String name;
  /**
   * The configuration (the Hadoop).
   */
  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final Configuration conf;

  public HiveActor(String name, Configuration configuration) {
    this.name = name;
    this.conf = configuration;
  }

  /**
   * The method to create an actor.
   * @param name the actor name
   * @param conf the actor configuration
   * @return an actor instance
   * @throws RuntimeException if instantiation fails
   */
  public static HiveActor createActor(String name, Configuration conf) {
    String clazzName = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_ICEBERG_CATALOG_ACTOR_CLASS);
    if (clazzName == null || HiveActor.class.getName().equals(clazzName)) {
      return new HiveCatalogActor(name, conf);
    }
    try {
      @SuppressWarnings("unchecked")
      Class<? extends HiveActor> clazz = (Class<? extends HiveActor>) Class.forName(clazzName);
      Constructor<? extends HiveActor> ctor = clazz.getConstructor(String.class, Configuration.class);
      return ctor.newInstance(name, conf);
    } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException |
             InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public abstract HiveActor initialize(Map<String, String> properties);

  protected abstract Database getDatabase(Namespace namespace) throws TException, InterruptedException;

  protected abstract void alterDatabase(Namespace namespace, Database database) throws TException, InterruptedException;

  protected abstract List<String> listTableNames(String database) throws TException, InterruptedException;

  protected abstract Table getTable(String fromDatabase, String fromName) throws TException, InterruptedException;

  protected abstract List<Table> listTables(String database, List<String> tableNames)
      throws TException, InterruptedException;

  protected abstract void alterTable(String fromDatabase, String fromName, Table table)
      throws TException, InterruptedException;

  protected abstract void alterTable(String database, String tableName, Table hmsTable, String expectedMetadataLocation)
      throws TException, InterruptedException;

  protected abstract void createTable(Table table) throws TException, InterruptedException;

  protected abstract void dropTable(String databaseName, String tableName) throws TException, InterruptedException;

  protected abstract void createNamespace(Database database) throws TException, InterruptedException;

  protected abstract List<String> listNamespaceNames() throws TException, InterruptedException;

  protected abstract void dropNamespace(Namespace namespace) throws TException, InterruptedException;

  protected HiveLock newLock(TableMetadata metadata, String catalogName, String database, String tableName) {
    return new MetastoreLock(conf, this, catalogName, database, tableName);
  }

  protected abstract void heartbeat(long txnId, long lockId) throws TException, InterruptedException;

  protected abstract LockResponse checkLock(long lockId) throws TException, InterruptedException;

  protected abstract LockResponse lock(LockRequest request) throws TException, InterruptedException;

  protected abstract void unlock(long lockId) throws TException, InterruptedException;

  protected abstract ShowLocksResponse showLocks(ShowLocksRequest request) throws TException, InterruptedException;
}
