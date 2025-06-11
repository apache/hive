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
import org.apache.hadoop.hive.metastore.api.GetDatabaseObjectsRequest;
import org.apache.hadoop.hive.metastore.api.GetDatabaseObjectsResponse;
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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.FilterUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient.SNAPSHOT_REF;
import static org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient.TRUNCATE_SKIP_DATA_DELETION;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

public class HookMetaStoreClientProxy extends BaseMetaStoreClientProxy implements IMetaStoreClient {
  private final Configuration conf;
  private final HiveMetaHookLoader hookLoader;
  private final MetaStoreFilterHook filterHook;
  private final boolean isClientFilterEnabled;

  private static final Logger LOG = LoggerFactory.getLogger(HookMetaStoreClientProxy.class);

  public HookMetaStoreClientProxy(Configuration conf, @Nullable HiveMetaHookLoader hookLoader,
      IMetaStoreClient delegate) {
    super(delegate, conf);
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
  public List<String> getAllDataConnectorNames() throws TException {
    List<String> connectorNames = getDelegate().getAllDataConnectorNames();
    return FilterUtils.filterDataConnectorsIfEnabled(isClientFilterEnabled, filterHook, connectorNames);
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
    for (Table table: new TableIterable(this, req.getCatalogName(), req.getName(), tableList, maxBatchSize)) {
      boolean isSoftDelete =
          req.isSoftDelete() && Boolean.parseBoolean(table.getParameters().get(SOFT_DELETE_TABLE));
      boolean deleteData = req.isDeleteData() && !isSoftDelete;

      // SG:FIXME, introduce IMetaStoreClient.dropTable :: DropTableRequest -> void
      // We need a new method that takes catName and checks tbl.isSetTxnId()
      // Or does TxN stuff meaningful only if we are in default catalog?
      // cf. This mark also exists under ThriftHiveMetaStoreClient#dropDatabase.

      dropTable(table, deleteData, false, false);
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
  public void truncateTable(TableName table, List<String> partNames) throws TException {
    truncateTableInternal(table.getCat(), table.getDb(), table.getTable(), table.getTableMetaRef(), partNames,
        null, -1, true);
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
  public List<String> getDatabases(String catName, String databasePattern) throws TException {
    List<String> databases = getDelegate().getDatabases(catName, databasePattern);
    return FilterUtils.filterDbNamesIfEnabled(isClientFilterEnabled, filterHook, databases);
  }

  @Override
  public List<String> getAllDatabases(String catName) throws TException {
    List<String> databases = getDelegate().getAllDatabases(catName);
    return FilterUtils.filterDbNamesIfEnabled(isClientFilterEnabled, filterHook, databases);
  }

  @Override
  public GetDatabaseObjectsResponse get_databases_req(GetDatabaseObjectsRequest request) throws TException {
    GetDatabaseObjectsResponse response = getDelegate().get_databases_req(request);
    response.setDatabases(FilterUtils.filterDatabaseObjectsIfEnabled(
        isClientFilterEnabled, filterHook, response.getDatabases()));
    return response;
  }

  @Override
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
      int max_parts) throws TException {
    List<Partition> parts = getDelegate().listPartitions(catName, db_name, tbl_name, max_parts);
    return HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
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
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
      List<String> part_vals, int max_parts) throws TException {
    List<Partition> parts = getDelegate().listPartitions(catName, db_name, tbl_name, part_vals, max_parts);
    return HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
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
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
      List<String> partialPvals, int maxParts, String userName, List<String> groupNames) throws TException {

    List<Partition> parts = getDelegate().listPartitionsWithAuthInfo(catName, dbName, tableName, partialPvals,
        maxParts, userName, groupNames);
    return HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
  }

  @Override
  public List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name,
      String filter, int max_parts) throws TException {
    List<Partition> parts = getDelegate().listPartitionsByFilter(catName, db_name, tbl_name, filter, max_parts);
    return HiveMetaStoreClientUtils.deepCopyPartitions(
        FilterUtils.filterPartitionsIfEnabled(isClientFilterEnabled, filterHook, parts));
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
  public List<Partition> getPartitionsByNames(String dbName, String tableName,
      List<String> oartNames) throws TException {
    return getPartitionsByNames(getDefaultCatalog(conf), dbName, tableName, oartNames);
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
  public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName,
      List<String> pvals, String userName, List<String> groupNames) throws TException {
    Partition p = getDelegate().getPartitionWithAuthInfo(catName, dbName, tableName, pvals, userName, groupNames);
    return HiveMetaStoreClientUtils.deepCopy(
        FilterUtils.filterPartitionIfEnabled(isClientFilterEnabled, filterHook, p));
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
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter,
      int maxTables) throws TException {
    List<String> tableNames = getDelegate().listTableNamesByFilter(catName, dbName, filter, maxTables);
    return FilterUtils.filterTableNamesIfEnabled(
        isClientFilterEnabled, filterHook, catName, dbName, tableNames);
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern) throws TException {
    List<String> tables = getDelegate().getTables(catName, dbName, tablePattern);
    return FilterUtils.filterTableNamesIfEnabled(isClientFilterEnabled, filterHook, catName, dbName, tables);
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
  public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
      List<String> tableTypes) throws TException {
    List<TableMeta> tableMetas = getDelegate().getTableMeta(catName, dbPatterns, tablePatterns, tableTypes);
    return FilterUtils.filterTableMetasIfEnabled(isClientFilterEnabled, filterHook, tableMetas);
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) throws TException {
    List<String> tableNames = getDelegate().getAllTables(catName, dbName);
    return FilterUtils.filterTableNamesIfEnabled(isClientFilterEnabled, filterHook, catName, dbName, tableNames);
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
  public List<String> listPartitionNames(PartitionsByExprRequest req) throws TException {
    return FilterUtils.filterPartitionNamesIfEnabled(isClientFilterEnabled, filterHook, req.getCatName(),
        req.getDbName(), req.getTblName(), getDelegate().listPartitionNames(req));
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, String name)
      throws TException {
    Partition p = getDelegate().getPartition(catName, dbName, tblName, name);
    return HiveMetaStoreClientUtils.deepCopy(
        FilterUtils.filterPartitionIfEnabled(isClientFilterEnabled, filterHook, p));
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
    // SG:FIXME, do we have to call delegate.insertTable() here?
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
