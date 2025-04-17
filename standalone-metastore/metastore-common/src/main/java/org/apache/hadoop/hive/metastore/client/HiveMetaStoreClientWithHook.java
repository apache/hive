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
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.DefaultHiveMetaHook;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableIterable;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsExpr;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsResponse;
import org.apache.hadoop.hive.metastore.api.RequestPartsSpec;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.FilterUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient.SNAPSHOT_REF;
import static org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient.TRUNCATE_SKIP_DATA_DELETION;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

public class HiveMetaStoreClientWithHook extends NoopHiveMetaStoreClientDelegator implements IMetaStoreClient {
  private final Configuration conf;
  private final HiveMetaHookLoader hookLoader;
  private final MetaStoreFilterHook filterHook;
  private final boolean isClientFilterEnabled;

  private static final Logger LOG = LoggerFactory.getLogger(HiveMetaStoreClientWithHook.class);

  public HiveMetaStoreClientWithHook(Configuration conf, HiveMetaHookLoader hookLoader,
      IMetaStoreClient delegate) {
    super(delegate);
    if (conf == null) {
      conf = MetastoreConf.newMetastoreConf();
      this.conf = conf;
    } else {
      this.conf = new Configuration(conf);
    }

    this.hookLoader = hookLoader;

    filterHook = loadFilterHooks();
    isClientFilterEnabled = getIfClientFilterEnabled();
  }

  private MetaStoreFilterHook loadFilterHooks() throws IllegalStateException {
    Class<? extends MetaStoreFilterHook> authProviderClass =
        MetastoreConf.getClass(
            conf,
            MetastoreConf.ConfVars.FILTER_HOOK, DefaultMetaStoreFilterHookImpl.class,
            MetaStoreFilterHook.class);
    String msg = "Unable to create instance of " + authProviderClass.getName() + ": ";
    try {
      Constructor<? extends MetaStoreFilterHook> constructor =
          authProviderClass.getConstructor(Configuration.class);
      return constructor.newInstance(conf);
    } catch (NoSuchMethodException | SecurityException | IllegalAccessException | InstantiationException |
          IllegalArgumentException | InvocationTargetException e) {
      throw new IllegalStateException(msg + e.getMessage(), e);
    }
  }

  private boolean getIfClientFilterEnabled() {
    boolean isEnabled =
        MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_CLIENT_FILTER_ENABLED);
    LOG.info("HMS client filtering is " + (isEnabled ? "enabled." : "disabled."));
    return isEnabled;
  }

  private HiveMetaHook getHook(Table tbl) throws MetaException {
    if (hookLoader == null) {
      return null;
    }
    return hookLoader.getHook(tbl);
  }

  // SG:FIXME for alterTable, createTable, dropDatabase, etc. families
  // Option 1. Keep the HMSC style: wrap only one method by hook,
  //   and the other methods eventually calls the wrapped one
  // Option 2. Wrap each delegating call with hook
  // Option 3. Implement a wrapper method something like Spark's withXXX methods.

  @Override
  public void alter_table(String dbname, String tbl_name, Table new_tbl) throws TException {
    alter_table_with_environmentContext(dbname, tbl_name, new_tbl, null);
  }

  @Override
  public void alter_table(String defaultDatabaseName, String tblName, Table table,
      boolean cascade) throws TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    if (cascade) {
      environmentContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
    }
    alter_table_with_environmentContext(defaultDatabaseName, tblName, table, environmentContext);
  }

  @Override
  public void alter_table_with_environmentContext(String dbname, String tbl_name, Table new_tbl,
      EnvironmentContext envContext) throws TException {
    HiveMetaHook hook = getHook(new_tbl);
    if (hook != null) {
      hook.preAlterTable(new_tbl, envContext);
    }
    boolean success = false;
    try {
      getDelegate().alter_table_with_environmentContext(dbname, tbl_name, new_tbl, envContext);
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
        getDelegate().alter_table(catName, dbName, tbl_name, new_tbl, envContext, validWriteIds);
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
  public Catalog getCatalog(String catName) throws TException {
    Catalog catalog = getDelegate().getCatalog(catName);
    return catalog == null ?
        null : FilterUtils.filterCatalogIfEnabled(isClientFilterEnabled, filterHook, catalog);
  }

  @Override
  public List<String> getCatalogs() throws TException {
    List<String> catalogs = getDelegate().getCatalogs();
    return catalogs == null ?
        null : FilterUtils.filterCatalogNamesIfEnabled(isClientFilterEnabled, filterHook, catalogs);
  }

  @Override
  public List<Partition> add_partitions(List<Partition> parts, boolean ifNotExists, boolean needResults)
      throws TException {
    List<Partition> partitions = getDelegate().add_partitions(parts, ifNotExists, needResults);
    if (needResults) {
      return FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, partitions);
    } else {
      return null;
    }
  }

  @Override
  public List<String> getAllDataConnectorNames() throws MetaException, TException {
    List<String> connectorNames = getDelegate().getAllDataConnectorNames();
    return FilterUtils.filterDataConnectorsIfEnabled(isClientFilterEnabled, filterHook, connectorNames);
  }

  @Override
  public void createTable(Table tbl) throws MetaException, NoSuchObjectException, TException {
    CreateTableRequest request = new CreateTableRequest(tbl);
    createTable(request);
  }

  @Override
  public void createTable(CreateTableRequest request)
      throws MetaException, NoSuchObjectException, TException {
    Table tbl = request.getTable();
    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preCreateTable(request);
    }
    boolean success = false;
    try {
      // Subclasses can override this step (for example, for temporary tables)
      if (hook == null || !hook.createHMSTableInHook()) {
        getDelegate().createTable(request);
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
  public void dropDatabase(String name)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropDatabase(getDefaultCatalog(conf), name, true, false, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb) throws TException {
    dropDatabase(getDefaultCatalog(conf), name, deleteData, ignoreUnknownDb, false);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws TException {
    dropDatabase(getDefaultCatalog(conf), name, deleteData, ignoreUnknownDb, cascade);
  }

  @Override
  public void dropDatabase(DropDatabaseRequest req) throws TException {
    if (req.isCascade()) {
      // Note that this logic may drop some of the tables of the database
      // even if the drop database fail for any reason
      // TODO: Fix this
      List<String> materializedViews = getDelegate().getTables(req.getName(), ".*", TableType.MATERIALIZED_VIEW);
      for (String table : materializedViews) {
        // First we delete the materialized views
        Table materializedView = getDelegate().getTable(getDefaultCatalog(conf), req.getName(), table);
        boolean isSoftDelete = req.isSoftDelete() && Boolean.parseBoolean(
            materializedView.getParameters().get(SOFT_DELETE_TABLE));
        materializedView.setTxnId(req.getTxnId());
        getDelegate().dropTable(materializedView, req.isDeleteData() && !isSoftDelete, true, false);
      }

      /**
       * When dropping db cascade, client side hooks have to be called at each table removal.
       * If {@link org.apache.hadoop.hive.metastore.conf.MetastoreConf#ConfVars.BATCH_RETRIEVE_MAX
       * BATCH_RETRIEVE_MAX} is less than the number of tables in the DB, we'll have to call the
       * hooks one by one each alongside with a
       * {@link #dropTable(String, String, boolean, boolean, EnvironmentContext) dropTable} call to
       * ensure transactionality.
       */
      List<String> tableNameList = getDelegate().getAllTables(req.getName());
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
      getDelegate().dropDatabase(req);
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
    boolean ifPurge = false;
    boolean ignoreUnknownTable = false;

    for (Table table: new TableIterable(this, req.getCatalogName(), req.getName(), tableList, maxBatchSize)) {
      boolean isSoftDelete =
          req.isSoftDelete() && Boolean.parseBoolean(table.getParameters().get(SOFT_DELETE_TABLE));
      boolean deleteData = req.isDeleteData() && !isSoftDelete;
      getDelegate().dropTable(req.getCatalogName(), req.getName(), table.getTableName(), deleteData, ifPurge,
          ignoreUnknownTable);
      // SG:FIXME, introduce IMetaStoreClient.dropTable :: DropTableRequest -> void
      // We need a new method that takes catName and checks tbl.isSetTxnId()
      // Or does TxN stuff meaningful only if we are in default catalog?
    }
    getDelegate().dropDatabase(req);
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
    List<Table> tables = getDelegate().getTableObjectsByName(req.getCatalogName(), req.getName(), tableList);
    boolean success = false;
    try {
      for (Table table : tables) {
        HiveMetaHook hook = getHook(table);
        if (hook == null) {
          continue;
        }
        hook.preDropTable(table);
      }
      getDelegate().dropDatabase(req);
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
  public List<Partition> dropPartitions(String dbName, String tblName, List<Pair<Integer, byte[]>> partExprs,
      PartitionDropOptions options) throws TException {
    return dropPartitions(getDefaultCatalog(conf), dbName, tblName, partExprs, options);
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName, List<Pair<Integer, byte[]>> partExprs,
      boolean deleteData, boolean ifExists, boolean needResult) throws TException {

    return dropPartitions(getDefaultCatalog(conf), dbName, tblName, partExprs,
        PartitionDropOptions.instance()
            .deleteData(deleteData)
            .ifExists(ifExists)
            .returnResults(needResult));

  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName, List<Pair<Integer, byte[]>> partExprs,
      boolean deleteData, boolean ifExists) throws TException {
    // By default, we need the results from dropPartitions();
    return dropPartitions(getDefaultCatalog(conf), dbName, tblName, partExprs,
        PartitionDropOptions.instance()
            .deleteData(deleteData)
            .ifExists(ifExists));
  }

  @Override
  public List<Partition> dropPartitions(String catName, String dbName, String tblName,
      List<Pair<Integer, byte[]>> partExprs, PartitionDropOptions options) throws TException {
    RequestPartsSpec rps = new RequestPartsSpec();
    List<DropPartitionsExpr> exprs = new ArrayList<>(partExprs.size());
    Table table = getDelegate().getTable(catName, dbName, tblName);
    HiveMetaHook hook = getHook(table);
    EnvironmentContext context = new EnvironmentContext();
    if (hook != null) {
      hook.preDropPartitions(table, context, partExprs);
    }
    return getDelegate().dropPartitions(catName, dbName, tblName, partExprs, options);
  }

  @Override
  public void dropTable(String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab) throws TException, UnsupportedOperationException {
    dropTable(getDefaultCatalog(conf), dbname, name, deleteData, ignoreUnknownTab, null);
  }

  @Override
  public void dropTable(String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab, boolean ifPurge) throws TException {
    dropTable(getDefaultCatalog(conf), dbname, name, deleteData, ignoreUnknownTab, ifPurge);
  }

  @Override
  public void dropTable(Table tbl, boolean deleteData, boolean ignoreUnknownTbl, boolean ifPurge)
      throws TException {
    EnvironmentContext context = null;
    if (ifPurge) {
      Map<String, String> warehouseOptions = new HashMap<>();
      warehouseOptions.put("ifPurge", "TRUE");
      context = new EnvironmentContext(warehouseOptions);
    }
    if (tbl.isSetTxnId()) {
      context = Optional.ofNullable(context).orElse(new EnvironmentContext());
      context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(tbl.getTxnId()));
    }
    String catName = Optional.ofNullable(tbl.getCatName()).orElse(getDefaultCatalog(conf));

    dropTable(catName, tbl.getDbName(), tbl.getTableName(), deleteData,
        ignoreUnknownTbl, context);
  }

  @Override
  public void dropTable(String dbname, String name) throws TException {
    dropTable(getDefaultCatalog(conf), dbname, name, true, true, null);
  }

  @Override
  public void dropTable(String catName, String dbName, String tableName, boolean deleteData,
      boolean ignoreUnknownTable, boolean ifPurge) throws TException {
    //build new environmentContext with ifPurge;
    EnvironmentContext envContext = null;
    if (ifPurge) {
      Map<String, String> warehouseOptions;
      warehouseOptions = new HashMap<>();
      warehouseOptions.put("ifPurge", "TRUE");
      envContext = new EnvironmentContext(warehouseOptions);
    }
    dropTable(catName, dbName, tableName, deleteData, ignoreUnknownTable, envContext);
  }

  private void dropTable(String catName, String dbname, String name, boolean deleteData,
      boolean ignoreUnknownTab, EnvironmentContext envContext) throws MetaException, TException,
      NoSuchObjectException, UnsupportedOperationException {
    Table tbl;
    try {
      tbl = getTable(catName, dbname, name);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw e;
      }
      return;
    }
    HiveMetaHook hook = getHook(tbl);
    if (hook != null) {
      hook.preDropTable(tbl, deleteData || (envContext != null && "TRUE".equals(envContext.getProperties().get("ifPurge"))));
    }
    boolean success = false;
    try {
      // SG:FIXME
      getDelegate().dropTable(dbname, name);
      if (hook != null) {
        hook.commitDropTable(tbl, deleteData || (envContext != null && "TRUE".equals(envContext.getProperties().get("ifPurge"))));
      }
      success = true;
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTab) {
        throw e;
      }
    } finally {
      if (!success && (hook != null)) {
        hook.rollbackDropTable(tbl);
      }
    }
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames,
      String validWriteIds, long writeId, boolean deleteData) throws TException {
    truncateTableInternal(getDefaultCatalog(conf), dbName, tableName, null, partNames, validWriteIds, writeId,
        deleteData);
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames,
      String validWriteIds, long writeId) throws TException {
    truncateTable(dbName, tableName, partNames, validWriteIds, writeId, true);
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames) throws TException {
    truncateTable(getDefaultCatalog(conf), dbName, tableName, partNames);
  }

  @Override
  public void truncateTable(TableName table, List<String> partNames) throws TException {
    truncateTableInternal(table.getCat(), table.getDb(), table.getTable(), table.getTableMetaRef(), partNames,
        null, -1, true);
  }

  @Override
  public void truncateTable(String catName, String dbName, String tableName, List<String> partNames)
      throws TException {
    truncateTable(TableName.fromString(tableName, catName, dbName), partNames);
  }

  private void truncateTableInternal(String catName, String dbName, String tableName, String ref,
      List<String> partNames, String validWriteIds, long writeId, boolean deleteData)
      throws TException {
    Table table = getTable(catName, dbName, tableName);
    HiveMetaHook hook = getHook(table);
    EnvironmentContext context = new EnvironmentContext();
    if (ref != null) {
      context.putToProperties(SNAPSHOT_REF, ref);
    }
    context.putToProperties(TRUNCATE_SKIP_DATA_DELETION, Boolean.toString(!deleteData));
    if (hook != null) {
      hook.preTruncateTable(table, context, partNames);
    }
    // SG:FIXME
    getDelegate().truncateTable(dbName, tableName, partNames, validWriteIds, writeId, deleteData);
  }

  @Override
  public List<String> getDatabases(String databasePattern) throws TException {
    return getDatabases(getDefaultCatalog(conf), databasePattern);
  }

  @Override
  public List<String> getDatabases(String catName, String databasePattern) throws TException {
    List<String> databases = getDelegate().getDatabases(catName, databasePattern);
    return FilterUtils.filterDbNamesIfEnabled(isClientFilterEnabled, filterHook, databases);
  }

  @Override
  public List<String> getAllDatabases() throws TException {
    return getAllDatabases(getDefaultCatalog(conf));
  }

  @Override
  public List<String> getAllDatabases(String catName) throws TException {
    List<String> databases = getDelegate().getAllDatabases(catName);
    return FilterUtils.filterDbNamesIfEnabled(isClientFilterEnabled, filterHook, databases);
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
      throws TException {
    return listPartitions(getDefaultCatalog(conf), db_name, tbl_name, max_parts);
  }

  @Override
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
      int max_parts) throws TException {
    List<Partition> parts = getDelegate().listPartitions(catName, db_name, tbl_name, max_parts);
    return HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts)
      throws TException {
    return listPartitionSpecs(getDefaultCatalog(conf), dbName, tableName, maxParts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName,
      int maxParts) throws TException {
    List<PartitionSpec> partitionSpecs =
        getDelegate().listPartitionSpecs(catName, dbName, tableName, maxParts).toPartitionSpec();
    partitionSpecs =
        FilterUtils.filterPartitionSpecsIfEnabled(isClientFilterEnabled, filterHook, partitionSpecs);
    return PartitionSpecProxy.Factory.get(partitionSpecs);
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name,
      List<String> part_vals, short max_parts) throws TException {
    return listPartitions(getDefaultCatalog(conf), db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
      List<String> part_vals, int max_parts) throws TException {
    List<Partition> parts = getDelegate().listPartitions(catName, db_name, tbl_name, part_vals, max_parts);
    return HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String db_name, String tbl_name, short max_parts,
      String user_name, List<String> group_names) throws TException {
    return listPartitionsWithAuthInfo(getDefaultCatalog(conf), db_name, tbl_name, max_parts, user_name,
        group_names);
  }

  @Override
  public GetPartitionsPsWithAuthResponse listPartitionsWithAuthInfoRequest(GetPartitionsPsWithAuthRequest req)
      throws TException {
    GetPartitionsPsWithAuthResponse res = getDelegate().listPartitionsWithAuthInfoRequest(req);
    List<Partition> parts = HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, res.getPartitions()));
    res.setPartitions(parts);
    return res;
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
      int maxParts, String userName, List<String> groupNames) throws TException {
    List<Partition> parts = getDelegate().listPartitionsWithAuthInfo(
        catName, dbName, tableName, maxParts, userName, groupNames);
    return HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String db_name, String tbl_name, List<String> part_vals,
      short max_parts, String user_name, List<String> group_names) throws TException {
    return listPartitionsWithAuthInfo(getDefaultCatalog(conf), db_name, tbl_name, part_vals, max_parts,
        user_name, group_names);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
      List<String> partialPvals, int maxParts, String userName, List<String> groupNames) throws TException {

    List<Partition> parts = getDelegate().listPartitionsWithAuthInfo(catName, dbName, tableName, partialPvals,
        maxParts, userName, groupNames);
    return HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
  }

  @Override
  public List<Partition> listPartitionsByFilter(String db_name, String tbl_name, String filter,
      short max_parts) throws TException {
    return listPartitionsByFilter(getDefaultCatalog(conf), db_name, tbl_name, filter, max_parts);
  }

  @Override
  public List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name,
      String filter, int max_parts) throws TException {
    List<Partition> parts = getDelegate().listPartitionsByFilter(catName, db_name, tbl_name, filter, max_parts);
    return HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name, String filter,
      int max_parts) throws TException {
    return listPartitionSpecsByFilter(getDefaultCatalog(conf), db_name, tbl_name, filter, max_parts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String db_name, String tbl_name,
      String filter, int max_parts) throws TException {
    List<PartitionSpec> partitionSpecs =
        getDelegate().listPartitionSpecsByFilter(catName, db_name, tbl_name, filter, max_parts)
            .toPartitionSpec();
    return PartitionSpecProxy.Factory.get(
        FilterUtils.filterPartitionSpecsIfEnabled(isClientFilterEnabled, filterHook, partitionSpecs));
  }

  @Override
  public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr,
      String default_partition_name, short max_parts, List<Partition> result) throws TException {
    return listPartitionsByExpr(getDefaultCatalog(conf), db_name, tbl_name, expr,
        default_partition_name, max_parts, result);
  }

  @Override
  public boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr,
      String default_partition_name, int max_parts, List<Partition> result) throws TException {
    // SG:FIXME, remove assert?
    assert result != null;

    // to ensure that result never contains any unfiltered partitions.
    List<Partition> tempList = new ArrayList<>();
    boolean r = getDelegate().listPartitionsByExpr(
        catName, db_name, tbl_name, expr, default_partition_name, max_parts, tempList);

    result.addAll(FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, tempList));

    return r;
  }

  @Override
  public boolean listPartitionsSpecByExpr(PartitionsByExprRequest req, List<PartitionSpec> result)
      throws TException {
    // SG:FIXME, remove assert?
    assert result != null;

    // to ensure that result never contains any unfiltered partitions.
    List<PartitionSpec> tempList = new ArrayList<>();
    boolean r = getDelegate().listPartitionsSpecByExpr(req, tempList);

    result.addAll(FilterUtils.filterPartitionSpecsIfEnabled(isClientFilterEnabled, filterHook, tempList));

    return r;
  }

  @Override
  public Database getDatabase(String name) throws TException {
    return getDatabase(getDefaultCatalog(conf), name);
  }

  @Override
  public Database getDatabase(String catalogName, String databaseName) throws TException {
    Database d = getDelegate().getDatabase(catalogName, databaseName);
    return HiveMetaStoreClientUtils.deepCopy(
        FilterUtils.filterDbIfEnabled(isClientFilterEnabled, filterHook, d));
  }

  @Override
  public GetPartitionResponse getPartitionRequest(GetPartitionRequest req) throws TException {
    GetPartitionResponse res = getDelegate().getPartitionRequest(req);
    res.setPartition(HiveMetaStoreClientUtils.deepCopy(
        FilterUtils.filterPartitionIfEnabled(isClientFilterEnabled, filterHook, res.getPartition())));
    return res;
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, List<String> partVals)
      throws TException {
    Partition part = getDelegate().getPartition(catName, dbName, tblName, partVals);
    return HiveMetaStoreClientUtils.deepCopy(
        FilterUtils.filterPartitionIfEnabled(isClientFilterEnabled, filterHook, part));
  }

  @Override
  public List<Partition> getPartitionsByNames(String db_name, String tbl_name,
      List<String> part_names) throws TException {
    return getPartitionsByNames(getDefaultCatalog(conf), db_name, tbl_name, part_names);
  }

  @Override
  public PartitionsResponse getPartitionsRequest(PartitionsRequest req) throws TException {
    PartitionsResponse res = getDelegate().getPartitionsRequest(req);
    List<Partition> parts = HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, res.getPartitions()));
    res.setPartitions(parts);
    return res;
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String db_name, String tbl_name,
      List<String> part_names) throws TException {
    checkDbAndTableFilters(catName, db_name, tbl_name);
    List<Partition> parts = getDelegate().getPartitionsByNames(catName, db_name, tbl_name, part_names);
    return HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
  }

  @Override
  public GetPartitionsByNamesResult getPartitionsByNames(GetPartitionsByNamesRequest req)
      throws TException {
    checkDbAndTableFilters(getDefaultCatalog(conf), req.getDb_name(), req.getTbl_name());
    List<Partition> parts = getDelegate().getPartitionsByNames(req).getPartitions();
    GetPartitionsByNamesResult res = new GetPartitionsByNamesResult();
    res.setPartitions(HiveMetaStoreClientUtils.deepCopyPartitions(FilterUtils.filterPartitionsIfEnabled(
        isClientFilterEnabled, filterHook, parts)));
    return res;
  }

  @Override
  public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request) throws TException {
    if (!request.isSetCatName()) {
      request.setCatName(getDefaultCatalog(conf));
    }

    String catName = request.isSetCatName() ? request.getCatName() : getDefaultCatalog(conf);
    String dbName = request.getDbName();
    String tblName = request.getTblName();

    checkDbAndTableFilters(catName, dbName, tblName);
    return getDelegate().listPartitionValues(request);
  }

  /**
   * Check if the current user has access to a given database and table name. Throw
   * NoSuchObjectException if user has no access. When the db or table is filtered out, we don't need
   * to even fetch the partitions. Therefore this check ensures table-level security and
   * could improve performance when filtering partitions.
   *
   * @param catName the catalog name
   * @param dbName  the database name
   * @param tblName the table name contained in the database
   * @throws NoSuchObjectException if the database or table is filtered out
   */
  private void checkDbAndTableFilters(final String catName, final String dbName, final String tblName)
      throws NoSuchObjectException, MetaException {

    // HIVE-20776 causes view access regression
    // Therefore, do not do filtering here. Call following function only to check
    // if dbName and tblName is valid
    FilterUtils.checkDbAndTableFilters(false, filterHook, catName, dbName, tblName);
  }

  @Override
  public Partition getPartitionWithAuthInfo(String db_name, String tbl_name,
      List<String> part_vals, String user_name, List<String> group_names)
      throws TException {
    return getPartitionWithAuthInfo(getDefaultCatalog(conf), db_name, tbl_name, part_vals,
        user_name, group_names);
  }

  @Override
  public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName,
      List<String> pvals, String userName, List<String> groupNames) throws TException {
    Partition p = getDelegate().getPartitionWithAuthInfo(catName, dbName, tableName, pvals, userName, groupNames);
    return HiveMetaStoreClientUtils.deepCopy(
        FilterUtils.filterPartitionIfEnabled(isClientFilterEnabled, filterHook, p));
  }

  @Override
  public Table getTable(String dbname, String name) throws TException {
    GetTableRequest req = new GetTableRequest(dbname, name);
    req.setCatName(getDefaultCatalog(conf));
    return getTable(req);
  }

  @Override
  public Table getTable(String dbname, String name, boolean getColumnStats, String engine) throws TException {
    GetTableRequest req = new GetTableRequest(dbname, name);
    req.setCatName(getDefaultCatalog(conf));
    req.setGetColumnStats(getColumnStats);
    if (getColumnStats) {
      req.setEngine(engine);
    }
    return getTable(req);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    return getTable(req);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName,
      String validWriteIdList) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    req.setValidWriteIdList(validWriteIdList);
    return getTable(req);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName, String validWriteIdList,
      boolean getColumnStats, String engine) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    req.setValidWriteIdList(validWriteIdList);
    req.setGetColumnStats(getColumnStats);
    if (getColumnStats) {
      req.setEngine(engine);
    }
    return getTable(req);
  }

  @Override
  public Table getTable(GetTableRequest getTableRequest) throws TException {
    Table t = getDelegate().getTable(getTableRequest);
    executePostGetTableHook(t);
    return HiveMetaStoreClientUtils.deepCopy(
        FilterUtils.filterTableIfEnabled(isClientFilterEnabled, filterHook, t));
  }

  private void executePostGetTableHook(Table t) throws MetaException {
    HiveMetaHook hook = getHook(t);
    if (hook != null) {
      hook.postGetTable(t);
    }
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws TException {
    return getTables(getDefaultCatalog(conf), dbName, tableNames, null);
  }

  @Override
  public List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
      throws TException {
    return getTables(catName, dbName, tableNames, null);
  }

  @Override
  public List<Table> getTables(String catName, String dbName, List<String> tableNames,
      GetProjectionsSpec projectionsSpec) throws TException {
    List<Table> tabs = getDelegate().getTables(catName, dbName, tableNames, projectionsSpec);
    for (Table tbl : tabs) {
      executePostGetTableHook(tbl);
    }
    return HiveMetaStoreClientUtils.deepCopyTables(
        FilterUtils.filterTablesIfEnabled(isClientFilterEnabled, filterHook, tabs));
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws TException {
    return listTableNamesByFilter(getDefaultCatalog(conf), dbName, filter, maxTables);
  }

  @Override
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter,
      int maxTables) throws TException {
    List<String> tableNames = getDelegate().listTableNamesByFilter(catName, dbName, filter, maxTables);
    return FilterUtils.filterTableNamesIfEnabled(
        isClientFilterEnabled, filterHook, catName, dbName, tableNames);
  }

  @Override
  public List<String> getTables(String dbname, String tablePattern) throws MetaException {
    try {
      return getTables(getDefaultCatalog(conf), dbname, tablePattern);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern) throws TException {
    // SG:FIXME, inefficient
    List<String> tableNames = getDelegate().getTables(catName, dbName, tablePattern);
    // tables is filtered by FilterHook.
    List<Table> tables = getTableObjectsByName(catName, dbName, tableNames);
    List<String> result = new ArrayList<>();
    for (Table tbl: tables) {
      result.add(tbl.getTableName());
    }
    return result;
  }

  @Override
  public List<String> getTables(String dbname, String tablePattern, TableType tableType) throws MetaException {
    try {
      return getTables(getDefaultCatalog(conf), dbname, tablePattern, tableType);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern,
      TableType tableType) throws TException {
    List<String> tables = getDelegate().getTables(catName, dbName, tablePattern, tableType);
    return FilterUtils.filterTableNamesIfEnabled(isClientFilterEnabled, filterHook, catName, dbName, tables);
  }

  @Override
  public List<Table> getAllMaterializedViewObjectsForRewriting() throws TException {
    try {
      List<Table> views = getDelegate().getAllMaterializedViewObjectsForRewriting();
      return FilterUtils.filterTablesIfEnabled(isClientFilterEnabled, filterHook, views);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String dbName) throws TException {
    return getMaterializedViewsForRewriting(getDefaultCatalog(conf), dbName);
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catName, String dbname)
      throws MetaException {
    try {
      List<String> views = getDelegate().getMaterializedViewsForRewriting(catName, dbname);
      return FilterUtils.filterTableNamesIfEnabled(isClientFilterEnabled, filterHook, catName, dbname, views);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException {
    try {
      return getTableMeta(getDefaultCatalog(conf), dbPatterns, tablePatterns, tableTypes);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
      List<String> tableTypes) throws TException {
    List<TableMeta> tableMetas = getDelegate().getTableMeta(catName, dbPatterns, tablePatterns, tableTypes);
    return FilterUtils.filterTableMetasIfEnabled(isClientFilterEnabled, filterHook, tableMetas);
  }

  @Override
  public List<String> getAllTables(String dbname) throws MetaException {
    try {
      return getAllTables(getDefaultCatalog(conf), dbname);
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return null;
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) throws TException {
    List<String> tableNames = getDelegate().getAllTables(catName, dbName);
    return FilterUtils.filterTableNamesIfEnabled(isClientFilterEnabled, filterHook, catName, dbName, tableNames);
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) throws TException {
    return tableExists(getDefaultCatalog(conf), databaseName, tableName);
  }

  @Override
  public boolean tableExists(String catName, String dbName, String tableName) throws TException {
    try {
      Table table = getTable(catName, dbName, tableName);
      return FilterUtils.filterTableIfEnabled(isClientFilterEnabled, filterHook, table) != null;
    } catch (NoSuchObjectException e) {
      return false;
    }
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName, short max) throws TException {
    return listPartitionNames(getDefaultCatalog(conf), dbName, tblName, max);
  }

  @Override
  public GetPartitionNamesPsResponse listPartitionNamesRequest(GetPartitionNamesPsRequest req)
      throws TException {
    GetPartitionNamesPsResponse res = getDelegate().listPartitionNamesRequest(req);
    List<String> partNames = FilterUtils.filterPartitionNamesIfEnabled(
        isClientFilterEnabled, filterHook, getDefaultCatalog(conf), req.getDbName(),
        req.getTblName(), res.getNames());
    res.setNames(partNames);
    return res;
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tableName, int maxParts)
      throws TException {
    List<String> partNames = getDelegate().listPartitionNames(catName, dbName, tableName, maxParts);
    return FilterUtils.filterPartitionNamesIfEnabled(
        isClientFilterEnabled, filterHook, catName, dbName, tableName, partNames);
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, List<String> part_vals,
      short max_parts) throws TException {
    return listPartitionNames(getDefaultCatalog(conf), db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<String> listPartitionNames(String catName, String db_name, String tbl_name,
      List<String> part_vals, int max_parts) throws TException {
    List<String> partNames = getDelegate().listPartitionNames(catName, db_name, tbl_name, part_vals, max_parts);
    return FilterUtils.filterPartitionNamesIfEnabled(
        isClientFilterEnabled, filterHook, catName, db_name, tbl_name, partNames);
  }

  @Override
  public List<String> listPartitionNames(PartitionsByExprRequest req) throws TException {
    return FilterUtils.filterPartitionNamesIfEnabled(isClientFilterEnabled, filterHook, req.getCatName(),
        req.getDbName(), req.getTblName(), getDelegate().listPartitionNames(req));
  }

  @Override
  public Partition getPartition(String db, String tableName, String partName) throws TException {
    return getPartition(getDefaultCatalog(conf), db, tableName, partName);
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, String name)
      throws TException {
    Partition p = getDelegate().getPartition(catName, dbName, tblName, name);
    return HiveMetaStoreClientUtils.deepCopy(
        FilterUtils.filterPartitionIfEnabled(isClientFilterEnabled, filterHook, p));
  }

  @Override
  public ShowCompactResponse showCompactions() throws TException {
    ShowCompactResponse response = getDelegate().showCompactions();
    response.setCompacts(FilterUtils.filterCompactionsIfEnabled(isClientFilterEnabled,
        filterHook, getDefaultCatalog(conf), response.getCompacts()));
    return response;
  }

  @Override public ShowCompactResponse showCompactions(ShowCompactRequest request) throws TException {
    ShowCompactResponse response = getDelegate().showCompactions(request);
    response.setCompacts(FilterUtils.filterCompactionsIfEnabled(isClientFilterEnabled,
        filterHook, getDefaultCatalog(conf), response.getCompacts()));
    return response;
  }

  @Override
  public GetLatestCommittedCompactionInfoResponse getLatestCommittedCompactionInfo(
      GetLatestCommittedCompactionInfoRequest request)
      throws TException {
    GetLatestCommittedCompactionInfoResponse response = getDelegate().getLatestCommittedCompactionInfo(request);
    return FilterUtils.filterCommittedCompactionInfoStructIfEnabled(isClientFilterEnabled, filterHook,
        getDefaultCatalog(conf), request.getDbname(), request.getTablename(), response);
  }

  @Override
  public void insertTable(Table table, boolean overwrite) throws MetaException {
    // SG:FIXME, do we have to call getDelegate().insertTable() here?
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
