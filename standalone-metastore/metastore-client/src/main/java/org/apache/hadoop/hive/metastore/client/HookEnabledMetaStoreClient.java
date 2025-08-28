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

package org.apache.hadoop.hive.metastore.client;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableIterable;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

public class HookEnabledMetaStoreClient extends MetaStoreClientWrapper {
  private final HiveMetaHookLoader hookLoader;

  private static final Logger LOG = LoggerFactory.getLogger(HookEnabledMetaStoreClient.class);

  public static HookEnabledMetaStoreClient newClient(Configuration conf, @Nullable HiveMetaHookLoader hookLoader,
      IMetaStoreClient delegate) {
    return new HookEnabledMetaStoreClient(conf, hookLoader, delegate);
  }

  public HookEnabledMetaStoreClient(Configuration conf, @Nullable HiveMetaHookLoader hookLoader,
      IMetaStoreClient delegate) {
    super(delegate, conf);

    this.hookLoader = hookLoader;
  }

  private HiveMetaHook getHook(Table tbl) throws MetaException {
    if (hookLoader == null) {
      return null;
    }
    return hookLoader.getHook(tbl);
  }

  @Override
  public void alter_table(String catName, String dbName, String tbl_name, Table new_tbl,
      EnvironmentContext envContext, String validWriteIds) throws TException {
    HiveMetaHook hook = getHook(new_tbl);
    if (hook != null) {
      hook.preAlterTable(new_tbl, envContext);
    }
    boolean success = false;
    try {
      boolean skipAlter = envContext != null && envContext.getProperties() != null &&
          Boolean.valueOf(envContext.getProperties().getOrDefault(HiveMetaHook.SKIP_METASTORE_ALTER, "false"));
      if (!skipAlter) {
        delegate.alter_table(catName, dbName, tbl_name, new_tbl, envContext, validWriteIds);
      }

      if (hook != null) {
        hook.commitAlterTable(new_tbl, envContext);
      }
      success = true;
    } finally {
      if (!success && (hook != null)) {
        hook.rollbackAlterTable(new_tbl, envContext);
      }
    }
  }

  @Override
  public void createTable(CreateTableRequest request) throws TException {
    Table tbl = request.getTable();
    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preCreateTable(request);
    }
    boolean success = false;
    try {
      // Subclasses can override this step (for example, for temporary tables)
      if (hook == null || !hook.createHMSTableInHook()) {
        delegate.createTable(request);
      }
      if (hook != null) {
        hook.commitCreateTable(tbl);
      }
      success = true;
    } finally {
      if (!success && (hook != null)) {
        try {
          hook.rollbackCreateTable(tbl);
        } catch (Exception e) {
          LOG.error("Create rollback failed with", e);
        }
      }
    }
  }

  @Override
  public void createTableWithConstraints(Table tbl,
      List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
      List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints,
      List<SQLDefaultConstraint> defaultConstraints,
      List<SQLCheckConstraint> checkConstraints) throws TException {

    CreateTableRequest createTableRequest = new CreateTableRequest(tbl);

    if (!tbl.isSetCatName()) {
      String defaultCat = getDefaultCatalog(conf);
      tbl.setCatName(defaultCat);
      if (primaryKeys != null) {
        primaryKeys.forEach(pk -> pk.setCatName(defaultCat));
      }
      if (foreignKeys != null) {
        foreignKeys.forEach(fk -> fk.setCatName(defaultCat));
      }
      if (uniqueConstraints != null) {
        uniqueConstraints.forEach(uc -> uc.setCatName(defaultCat));
        createTableRequest.setUniqueConstraints(uniqueConstraints);
      }
      if (notNullConstraints != null) {
        notNullConstraints.forEach(nn -> nn.setCatName(defaultCat));
      }
      if (defaultConstraints != null) {
        defaultConstraints.forEach(def -> def.setCatName(defaultCat));
      }
      if (checkConstraints != null) {
        checkConstraints.forEach(cc -> cc.setCatName(defaultCat));
      }
    }

    if (primaryKeys != null)
      createTableRequest.setPrimaryKeys(primaryKeys);

    if (foreignKeys != null)
      createTableRequest.setForeignKeys(foreignKeys);

    if (uniqueConstraints != null)
      createTableRequest.setUniqueConstraints(uniqueConstraints);

    if (notNullConstraints != null)
      createTableRequest.setNotNullConstraints(notNullConstraints);

    if (defaultConstraints != null)
      createTableRequest.setDefaultConstraints(defaultConstraints);

    if (checkConstraints != null)
      createTableRequest.setCheckConstraints(checkConstraints);

    createTable(createTableRequest);
  }

  @Override
  public void dropDatabase(DropDatabaseRequest req) throws TException {
    try {
      getDatabase(req.getCatalogName(), req.getName());
    } catch (NoSuchObjectException e) {
      if (!req.isIgnoreUnknownDb()) {
        throw e;
      }
      return;
    }

    if (req.isCascade()) {
      // Note that this logic may drop some of the tables of the database
      // even if the drop database fail for any reason
      // TODO: Fix this
      List<String> materializedViews =
          getTables(req.getCatalogName(), req.getName(), ".*", TableType.MATERIALIZED_VIEW);
      for (String table : materializedViews) {
        // First we delete the materialized views
        Table materializedView = getTable(req.getCatalogName(), req.getName(), table);
        boolean isSoftDelete = req.isSoftDelete() && Boolean.parseBoolean(
            materializedView.getParameters().get(SOFT_DELETE_TABLE));
        materializedView.setTxnId(req.getTxnId());
        dropTable(materializedView, req.isDeleteData() && !isSoftDelete, true, false);
      }

      /**
       * When dropping db cascade, client side hooks have to be called at each table removal.
       * If {@link org.apache.hadoop.hive.metastore.conf.MetastoreConf#ConfVars.BATCH_RETRIEVE_MAX
       * BATCH_RETRIEVE_MAX} is less than the number of tables in the DB, we'll have to call the
       * hooks one by one each alongside with a
       * {@link #dropTable(String, String, boolean, boolean, EnvironmentContext) dropTable} call to
       * ensure transactionality.
       */
      List<String> tableNameList = getAllTables(req.getCatalogName(), req.getName());
      int tableCount = tableNameList.size();
      int maxBatchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
      LOG.debug("Selecting dropDatabase method for " + req.getName() + " (" + tableCount + " tables), " +
          MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX.getVarname() + "=" + maxBatchSize);

      if (tableCount > maxBatchSize) {
        LOG.debug("Dropping database in a per table batch manner.");
        dropDatabaseCascadePerTable(req, tableNameList, maxBatchSize);
      } else {
        LOG.debug("Dropping database in a per DB manner.");
        dropDatabaseCascadePerDb(req, tableNameList);
      }

    } else {
      delegate.dropDatabase(req);
    }
  }

  /**
   * Handles dropDatabase by invoking drop_table in HMS for each table.
   * Useful when table list in DB is too large to fit in memory. It will retrieve tables in
   * chunks and for each table with a drop_table hook it will invoke drop_table on both HMS and
   * the hook. This is a timely operation so hookless tables are skipped and will be dropped on
   * server side when the client invokes drop_database.
   * Note that this is 'less transactional' than dropDatabaseCascadePerDb since we're dropping
   * table level objects, so the overall outcome of this method might result in a halfly dropped DB.
   * @param tableList
   * @param maxBatchSize
   * @throws TException
   */
  private void dropDatabaseCascadePerTable(DropDatabaseRequest req, List<String> tableList, int maxBatchSize)
      throws TException {
    for (Table table: new TableIterable(this, req.getCatalogName(), req.getName(), tableList, maxBatchSize)) {
      boolean isSoftDelete =
          req.isSoftDelete() && Boolean.parseBoolean(table.getParameters().get(SOFT_DELETE_TABLE));
      boolean deleteData = req.isDeleteData() && !isSoftDelete;

      if (req.isSetTxnId()) {
        // The correct way to propagate txnId is passing EnvironmentContext to underlying clients.
        // However, to prevent adding one more method in IMetaStoreClient, we hack Table object here.
        table.setTxnId(req.getTxnId());
        req.setDeleteManagedDir(false);
      }
      dropTable(table, deleteData, false, false);
    }
    delegate.dropDatabase(req);
  }

  /**
   * Handles dropDatabase by invoking drop_database in HMS.
   * Useful when table list in DB can fit in memory, it will retrieve all tables at once and
   * call drop_database once. Also handles drop_table hooks.
   * @param req
   * @param tableList
   * @throws TException
   */
  private void dropDatabaseCascadePerDb(DropDatabaseRequest req, List<String> tableList) throws TException {
    List<Table> tables = getTableObjectsByName(req.getCatalogName(), req.getName(), tableList);
    boolean success = false;
    try {
      for (Table table : tables) {
        HiveMetaHook hook = getHook(table);
        if (hook == null) {
          continue;
        }
        hook.preDropTable(table);
      }
      delegate.dropDatabase(req);
      for (Table table : tables) {
        HiveMetaHook hook = getHook(table);
        if (hook == null) {
          continue;
        }
        hook.commitDropTable(table, req.isDeleteData());
      }
      success = true;
    } finally {
      if (!success) {
        for (Table table : tables) {
          HiveMetaHook hook = getHook(table);
          if (hook == null) {
            continue;
          }
          hook.rollbackDropTable(table);
        }
      }
    }
  }

  @Override
  public List<Partition> dropPartitions(TableName tableName,
      RequestPartsSpec partsSpec, PartitionDropOptions options, EnvironmentContext context)
      throws TException {
    Table table = delegate.getTable(tableName.getCat(), tableName.getDb(), tableName.getTable());
    HiveMetaHook hook = getHook(table);
    if (hook != null) {
      if (context == null) {
        context = new EnvironmentContext();
      }
      hook.preDropPartitions(table, context, partsSpec);
    }
    return delegate.dropPartitions(tableName, partsSpec, options, context);
  }

  @Override
  public void dropTable(Table table, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge)
      throws TException {
    HiveMetaHook hook = getHook(table);
    if (hook != null) {
      hook.preDropTable(table, deleteData || ifPurge);
    }
    boolean success = false;
    try {
      delegate.dropTable(table, deleteData, ignoreUnknownTab, ifPurge);
      if (hook != null) {
        hook.commitDropTable(table, deleteData || ifPurge);
      }
      success = true;
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw e;
      }
    } finally {
      if (!success && (hook != null)) {
        hook.rollbackDropTable(table);
      }
    }
  }

  @Override
  public void truncateTable(String catName, String dbName, String tableName, String ref,
      List<String> partNames, String validWriteIds, long writeId, boolean deleteData,
      EnvironmentContext context) throws TException {
    Table table = getTable(catName, dbName, tableName);
    HiveMetaHook hook = getHook(table);
    if (hook != null) {
      if (context == null) {
        context = new EnvironmentContext();
      }

      if (ref != null) {
        context.putToProperties(ThriftHiveMetaStoreClient.SNAPSHOT_REF, ref);
      }
      context.putToProperties(ThriftHiveMetaStoreClient.TRUNCATE_SKIP_DATA_DELETION, Boolean.toString(!deleteData));

      hook.preTruncateTable(table, context, partNames);
    }
    delegate.truncateTable(catName, dbName, tableName, ref, partNames, validWriteIds, writeId,
        deleteData, context);
  }


  @Override
  public Table getTable(GetTableRequest getTableRequest) throws TException {
    Table t = delegate.getTable(getTableRequest);
    executePostGetTableHook(t);
    return t;
  }

  private void executePostGetTableHook(Table t) throws MetaException {
    HiveMetaHook hook = getHook(t);
    if (hook != null) {
      hook.postGetTable(t);
    }
  }

  @Override
  public List<Table> getTables(String catName, String dbName, List<String> tableNames,
      GetProjectionsSpec projectionsSpec) throws TException {
    List<Table> tabs = delegate.getTables(catName, dbName, tableNames, projectionsSpec);
    for (Table tbl : tabs) {
      executePostGetTableHook(tbl);
    }
    return tabs;
  }

  @Override
  public void insertTable(Table table, boolean overwrite) throws MetaException {
    boolean failed = true;
    HiveMetaHook hook = getHook(table);
    if (hook == null || !(hook instanceof DefaultHiveMetaHook)) {
      return;
    }
    DefaultHiveMetaHook hiveMetaHook = (DefaultHiveMetaHook) hook;
    try {
      hiveMetaHook.commitInsertTable(table, overwrite);
      failed = false;
    } finally {
      if (failed) {
        hiveMetaHook.rollbackInsertTable(table, overwrite);
      }
    }
  }
}
