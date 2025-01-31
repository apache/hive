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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidCleanerWriteIdList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AbortCompactResponse;
import org.apache.hadoop.hive.metastore.api.AbortCompactionRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddPackageRequest;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataRequest;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataStruct;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.DropPackageRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.ExtendedTableInfo;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetAllWriteEventInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetFieldsRequest;
import org.apache.hadoop.hive.metastore.api.GetFieldsResponse;
import org.apache.hadoop.hive.metastore.api.GetFunctionsRequest;
import org.apache.hadoop.hive.metastore.api.GetFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetPackageRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionNamesPsResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsByNamesResult;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsPsWithAuthResponse;
import org.apache.hadoop.hive.metastore.api.GetPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.GetReplicationMetricsRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.GetSchemaRequest;
import org.apache.hadoop.hive.metastore.api.GetSchemaResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.ListPackageRequest;
import org.apache.hadoop.hive.metastore.api.ListStoredProcedureRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.OptionalCompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.ReplicationMetricList;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StoredProcedure;
import org.apache.hadoop.hive.metastore.api.StoredProcedureRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.UpdateTransactionalStatsRequest;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogBatchRequest;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergRESTCatalogClientAdapter implements IMetaStoreClient {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergRESTCatalogClientAdapter.class);
  public static final String NAMESPACE_SEPARATOR = ".";
  public static final String NAME = "name";
  public static final String LOCATION = "location";
  public static final String CATALOG_NAME = "iceberg.catalog";
  public static final String DB_OWNER = "owner";
  public static final String DB_OWNER_TYPE = "ownerType";
  public static final String SERDE_INFO = "serdeInfo";
  public static final String DEFAULT_INPUT_FORMAT_CLASS = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat";
  public static final String DEFAULT_OUTPUT_FORMAT_CLASS
      = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat";
  public static final String DEFAULT_SERDE_CLASS = "org.apache.iceberg.mr.hive.HiveIcebergSerDe";
  public static final String CATALOG_CONFIG_PREFIX = "iceberg.catalog.";
  public static final Set<String> PROPERTIES_TO_REMOVE = ImmutableSet
      // We don't want to push down the metadata location props to Iceberg from HMS,
      // since the snapshot pointer in HMS would always be one step ahead
      .of(BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
          BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP);
  private final Configuration conf;
  private RESTCatalog restCatalog;
  private final HiveMetaHookLoader hookLoader;

  private final long maxHiveTablePropertySize;

  public HiveIcebergRESTCatalogClientAdapter(Configuration conf, HiveMetaHookLoader hookLoader) {
    this.conf = conf;
    this.hookLoader = hookLoader;
    this.maxHiveTablePropertySize = conf.getLong(HiveOperationsBase.HIVE_TABLE_PROPERTY_MAX_SIZE,
          HiveOperationsBase.HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT);
  }

  @Override
  public boolean isCompatibleWith(Configuration configuration) {
    return false;
  }

  @Override
  public void setHiveAddedJars(String addedJars) {

  }

  @Override
  public boolean isLocalMetaStore() {
    return false;
  }

  @Override
  public void reconnect() throws MetaException {
    SessionCatalog.SessionContext context = SessionCatalog.SessionContext.createEmpty();
    String catalogName = conf.get(CATALOG_NAME);
    Map<String, String> properties = getCatalogProperties(conf, catalogName);
    restCatalog = (RESTCatalog) CatalogUtil.buildIcebergCatalog(catalogName, properties, conf);
    restCatalog.initialize(catalogName, properties);
  }
  private static Map<String, String> getCatalogProperties(
      Configuration conf, String catalogName) {
    Map<String, String> catalogProperties = Maps.newHashMap();
    String keyPrefix = CATALOG_CONFIG_PREFIX + catalogName;
    conf.forEach(config -> {
      if (config.getKey().startsWith(keyPrefix)) {
        catalogProperties.put(
            config.getKey().substring(keyPrefix.length() + 1),
            config.getValue());
      }
    });
    return catalogProperties;
  }
  @Override
  public void close() {

  }

  @Override
  public void setMetaConf(String key, String value) throws MetaException, TException {

  }

  @Override
  public String getMetaConf(String key) throws MetaException, TException {
    return "";
  }

  @Override
  public void createCatalog(Catalog catalog)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
  }

  @Override
  public void alterCatalog(String catalogName, Catalog newCatalog)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

  }

  @Override
  public Catalog getCatalog(String catName) throws NoSuchObjectException, MetaException, TException {
    return null;
  }

  @Override
  public List<String> getCatalogs() throws MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public void dropCatalog(String catName)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

  }

  @Override
  public void dropCatalog(String catName, boolean ifExists) throws TException {

  }

  @Override
  public List<String> getDatabases(String databasePattern) throws MetaException, TException {
    return getAllDatabases();
  }

  @Override
  public List<String> getDatabases(String catName, String databasePattern) throws MetaException, TException {
    return getAllDatabases();
  }

  @Override
  public List<String> getAllDatabases() throws MetaException, TException {
    return restCatalog.listNamespaces(Namespace.empty()).stream().map(Namespace::toString).collect(Collectors.toList());
  }

  @Override
  public List<String> getAllDatabases(String catName) throws MetaException, TException {
    return getAllDatabases();
  }

  @Override
  public List<String> getTables(String dbName, String tablePattern)
      throws MetaException, TException, UnknownDBException {
    return getTables(null, dbName, tablePattern, null);
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern)
      throws MetaException, TException, UnknownDBException {
    return getTables(catName, dbName, tablePattern, null);
  }

  @Override
  public List<String> getTables(String dbName, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException {
    return getTables(null, dbName, tablePattern, tableType);
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException {
    List<TableIdentifier> tableIdentifiers = restCatalog.listTables(Namespace.of(dbName));
    return tableIdentifiers.stream().map(tableIdentifier -> tableIdentifier.name()).collect(Collectors.toList());
  }

  @Override
  public List<Table> getAllMaterializedViewObjectsForRewriting() throws MetaException, TException, UnknownDBException {
    return new LinkedList<>();
  }

  @Override
  public List<ExtendedTableInfo> getTablesExt(String catName, String dbName, String tablePattern, int requestedFields,
      int limit) throws MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String dbName)
      throws MetaException, TException, UnknownDBException {
    return new LinkedList<>();
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, TException, UnknownDBException {
    return new LinkedList<>();
  }

  @Override
  public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException, TException, UnknownDBException {
    return new LinkedList<>();
  }

  @Override
  public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException, TException, UnknownDBException {
    return new LinkedList<>();
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException, TException, UnknownDBException {
    return getTables(null, dbName, "", null);
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) throws MetaException, TException, UnknownDBException {
    return getTables(catName, dbName, "", null);
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws TException, InvalidOperationException, UnknownDBException {
    return new LinkedList<>();
  }

  @Override
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter, int maxTables)
      throws TException, InvalidOperationException, UnknownDBException {
    return new LinkedList<>();
  }

  @Override
  public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab)
      throws MetaException, TException, NoSuchObjectException {
    dropTable(dbname, tableName);
  }

  @Override
  public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge)
      throws MetaException, TException, NoSuchObjectException {
    dropTable(dbname, tableName);
  }

  @Override
  public void dropTable(Table table, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws TException {
    dropTable(table.getDbName(), table.getTableName());
  }

  @Override
  public void dropTable(String dbname, String tableName) throws MetaException, TException, NoSuchObjectException {
    restCatalog.dropTable(TableIdentifier.of(dbname, tableName));
  }

  @Override
  public void dropTable(String catName, String dbName, String tableName, boolean deleteData, boolean ignoreUnknownTable,
      boolean ifPurge) throws MetaException, NoSuchObjectException, TException {
    dropTable(dbName, tableName);
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames) throws MetaException, TException {

  }

  @Override
  public void truncateTable(TableName table, List<String> partNames) throws TException {

  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames, String validWriteIds, long writeId)
      throws TException {

  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames, String validWriteIds, long writeId,
      boolean deleteData) throws TException {

  }

  @Override
  public void truncateTable(String catName, String dbName, String tableName, List<String> partNames)
      throws MetaException, TException {

  }

  @Override
  public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest request) throws MetaException, TException {
    return new CmRecycleResponse();
  }

  @Override
  public boolean tableExists(String databaseName, String tableName)
      throws MetaException, TException, UnknownDBException {
    try {
      getTables(databaseName, tableName);
    } catch (NoSuchTableException e) {
      return false;
    }
    return true;
  }

  @Override
  public boolean tableExists(String catName, String dbName, String tableName)
      throws MetaException, TException, UnknownDBException {
    return tableExists(dbName, tableName);
  }

  @Override
  public Database getDatabase(String databaseName) throws NoSuchObjectException, MetaException, TException {
    return restCatalog.listNamespaces(Namespace.empty()).stream()
        .filter(namespace -> namespace.levels()[0].equals(databaseName)).map(namespace -> {
          Database database = new Database();
          database.setName(String.join(NAMESPACE_SEPARATOR, namespace.levels()));
          Map<String, String> namespaceMetadata = restCatalog.loadNamespaceMetadata(Namespace.of(databaseName));
          database.setLocationUri(namespaceMetadata.get(LOCATION));
          database.setCatalogName("REST");
          database.setOwnerName(namespaceMetadata.get(DB_OWNER));
          try {
            database.setOwnerType(PrincipalType.valueOf(namespaceMetadata.get(DB_OWNER_TYPE)));
          } catch (Exception e) {
            LOG.warn("Can not set ownerType: {}", namespaceMetadata.get(DB_OWNER_TYPE), e);
          }
          return database;
        }).findFirst().get();
  }

  @Override
  public Database getDatabase(String catalogName, String databaseName)
      throws NoSuchObjectException, MetaException, TException {
    return getDatabase(databaseName);
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException, TException, NoSuchObjectException {
    org.apache.iceberg.Table icebergTable = null;
    try {
      icebergTable = restCatalog.loadTable(TableIdentifier.of(dbName, tableName));
    } catch (NoSuchTableException exception) {
      throw  new NoSuchObjectException();
    }
    Table hiveTable = convertIcebergTableToHiveTable(icebergTable);
    return hiveTable;
  }

  @NotNull
  private Table convertIcebergTableToHiveTable(org.apache.iceberg.Table icebergTable) {
    Table hiveTable = new Table();
    TableMetadata metadata = ((BaseTable) icebergTable).operations().current();
    HMSTablePropertyHelper.updateHmsTableForIcebergTable(metadata.metadataFileLocation(), hiveTable,
        metadata, null, true, maxHiveTablePropertySize, null);
    hiveTable.getParameters().put(CATALOG_NAME, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    hiveTable.setTableName(getTableName(icebergTable));
    hiveTable.setDbName(getDbName(icebergTable));
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    hiveTable.setSd(storageDescriptor);
    hiveTable.setTableType("EXTERNAL_TABLE");
    hiveTable.setPartitionKeys(new LinkedList<>());
    List<FieldSchema> cols = new LinkedList<>();
    storageDescriptor.setCols(cols);
    storageDescriptor.setLocation(icebergTable.location());
    storageDescriptor.setInputFormat(DEFAULT_INPUT_FORMAT_CLASS);
    storageDescriptor.setOutputFormat(DEFAULT_OUTPUT_FORMAT_CLASS);
    storageDescriptor.setBucketCols(new LinkedList<>());
    storageDescriptor.setSortCols(new LinkedList<>());
    storageDescriptor.setParameters(Maps.newHashMap());
    if (icebergTable.properties().containsKey(SERDE_INFO)) {
      // TODO
    } else {
      SerDeInfo serDeInfo = new SerDeInfo("icebergSerde", DEFAULT_SERDE_CLASS, Maps.newHashMap());
      serDeInfo.getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1"); // Default serialization format.
      storageDescriptor.setSerdeInfo(serDeInfo);
    }
    icebergTable.schema().columns().forEach(icebergColumn -> {
      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setName(icebergColumn.name());
      fieldSchema.setType(icebergColumn.type().toString());
      cols.add(fieldSchema);
    });
    return hiveTable;
  }
  private String getTableName(org.apache.iceberg.Table icebergTable) {
    String[] nameParts = icebergTable.name().split("\\.");
    if (nameParts.length == 3) {
      return nameParts[2];
    }
    if (nameParts.length == 2) {
      return nameParts[1];
    }
    return icebergTable.name();
  }

  private String getDbName(org.apache.iceberg.Table icebergTable) {
    String[] nameParts = icebergTable.name().split("\\.");
    return nameParts.length == 3 ? nameParts[1] : nameParts[0];
  }

  @Override
  public Table getTable(String dbName, String tableName, boolean getColumnStats, String engine)
      throws MetaException, TException, NoSuchObjectException {
    return getTable(dbName, tableName);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName) throws MetaException, TException {
    return getTable(dbName, tableName);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName, String validWriteIdList) throws TException {
    return getTable(dbName, tableName);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName, String validWriteIdList,
      boolean getColumnStats, String engine) throws TException {
    return getTable(dbName, tableName);
  }

  @Override
  public Table getTable(GetTableRequest getTableRequest) throws MetaException, TException, NoSuchObjectException {
    return getTable(getTableRequest.getDbName(), getTableRequest.getTblName());
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return null;
  }

  @Override
  public List<Table> getTables(String catName, String dbName, List<String> tableNames,
      GetProjectionsSpec projectionsSpec)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return new LinkedList<>();
  }

  @Override
  public Materialization getMaterializationInvalidationInfo(CreationMetadata cm, String validTxnList)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return new Materialization();
  }

  @Override
  public void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm)
      throws MetaException, TException {
  }

  @Override
  public void updateCreationMetadata(String catName, String dbName, String tableName, CreationMetadata cm)
      throws MetaException, TException {

  }

  @Override
  public Partition appendPartition(String dbName, String tableName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return new Partition();
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return new Partition();
  }

  @Override
  public Partition appendPartition(String dbName, String tableName, String name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return new Partition();
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName, String name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return new Partition();
  }

  @Override
  public Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return new Partition();
  }

  @Override
  public int add_partitions(List<Partition> partitions)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return 0;
  }

  @Override
  public int add_partitions_pspec(PartitionSpecProxy partitionSpec)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return 0;
  }

  @Override
  public List<Partition> add_partitions(List<Partition> partitions, boolean ifNotExists, boolean needResults)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return null;
  }

  @Override
  public Partition getPartition(String dbName, String tblName, List<String> partVals)
      throws NoSuchObjectException, MetaException, TException {
    return new Partition();
  }

  @Override
  public GetPartitionResponse getPartitionRequest(GetPartitionRequest req)
      throws NoSuchObjectException, MetaException, TException {
    return new GetPartitionResponse();
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, List<String> partVals)
      throws NoSuchObjectException, MetaException, TException {
    return new Partition();
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceDb, String sourceTable,
      String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return new Partition();
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceCat, String sourceDb,
      String sourceTable, String destCat, String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return new Partition();
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceDb, String sourceTable,
      String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceCat, String sourceDb,
      String sourceTable, String destCat, String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return new LinkedList<>();
  }

  @Override
  public Partition getPartition(String dbName, String tblName, String name)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return new Partition();
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, String name)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return new Partition();
  }

  @Override
  public Partition getPartitionWithAuthInfo(String dbName, String tableName, List<String> pvals, String userName,
      List<String> groupNames) throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return new Partition();
  }

  @Override
  public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName, List<String> pvals,
      String userName, List<String> groupNames)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name, int max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts) throws TException {
    return null;
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName, int maxParts)
      throws TException {
    return null;
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name, List<String> part_vals, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name, List<String> part_vals,
      int max_parts) throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public GetPartitionNamesPsResponse listPartitionNamesRequest(GetPartitionNamesPsRequest req)
      throws NoSuchObjectException, MetaException, TException {
    return new GetPartitionNamesPsResponse();
  }

  @Override
  public List<String> listPartitionNames(String catName, String db_name, String tbl_name, int max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, List<String> part_vals, short max_parts)
      throws MetaException, TException, NoSuchObjectException {
    return new LinkedList<>();
  }

  @Override
  public List<String> listPartitionNames(String catName, String db_name, String tbl_name, List<String> part_vals,
      int max_parts) throws MetaException, TException, NoSuchObjectException {
    return new LinkedList<>();
  }

  @Override
  public List<String> listPartitionNames(PartitionsByExprRequest request)
      throws MetaException, TException, NoSuchObjectException {
    return new LinkedList<>();
  }

  @Override
  public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request)
      throws MetaException, TException, NoSuchObjectException {
    return new PartitionValuesResponse();
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tableName, String filter)
      throws MetaException, NoSuchObjectException, TException {
    return 0;
  }

  @Override
  public int getNumPartitionsByFilter(String catName, String dbName, String tableName, String filter)
      throws MetaException, NoSuchObjectException, TException {
    return 0;
  }

  @Override
  public List<Partition> listPartitionsByFilter(String db_name, String tbl_name, String filter, short max_parts)
      throws MetaException, NoSuchObjectException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name, String filter,
      int max_parts) throws MetaException, NoSuchObjectException, TException {
    return new LinkedList<>();
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name, String filter, int max_parts)
      throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String db_name, String tbl_name, String filter,
      int max_parts) throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public boolean listPartitionsSpecByExpr(PartitionsByExprRequest req, List<PartitionSpec> result) throws TException {
    return false;
  }

  @Override
  public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr, String default_partition_name,
      short max_parts, List<Partition> result) throws TException {
    return false;
  }

  @Override
  public boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr,
      String default_partition_name, int max_parts, List<Partition> result) throws TException {
    return false;
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName, short maxParts, String userName,
      List<String> groupNames) throws MetaException, TException, NoSuchObjectException {
    return new LinkedList<>();
  }

  @Override
  public GetPartitionsPsWithAuthResponse listPartitionsWithAuthInfoRequest(GetPartitionsPsWithAuthRequest req)
      throws MetaException, TException, NoSuchObjectException {
    return new GetPartitionsPsWithAuthResponse();
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName, int maxParts,
      String userName, List<String> groupNames) throws MetaException, TException, NoSuchObjectException {
    return new LinkedList<>();
  }

  @Override
  public List<Partition> getPartitionsByNames(String db_name, String tbl_name, List<String> part_names)
      throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String db_name, String tbl_name, List<String> part_names)
      throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public PartitionsResponse getPartitionsRequest(PartitionsRequest req)
      throws NoSuchObjectException, MetaException, TException {
    return new PartitionsResponse();
  }

  @Override
  public GetPartitionsByNamesResult getPartitionsByNames(GetPartitionsByNamesRequest req) throws TException {
    return new GetPartitionsByNamesResult();
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName, List<String> partialPvals,
      short maxParts, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException {
    return new LinkedList<>();
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
      List<String> partialPvals, int maxParts, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException {
    return new LinkedList<>();
  }

  @Override
  public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> partKVs,
      PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
      UnknownPartitionException, InvalidPartitionException {

  }

  @Override
  public void markPartitionForEvent(String catName, String db_name, String tbl_name, Map<String, String> partKVs,
      PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
      UnknownPartitionException, InvalidPartitionException {

  }

  @Override
  public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> partKVs,
      PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
      UnknownPartitionException, InvalidPartitionException {
    return false;
  }

  @Override
  public boolean isPartitionMarkedForEvent(String catName, String db_name, String tbl_name, Map<String, String> partKVs,
      PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, TException, UnknownTableException, UnknownDBException,
      UnknownPartitionException, InvalidPartitionException {
    return false;
  }

  @Override
  public void validatePartitionNameCharacters(List<String> partVals) throws TException, MetaException {

  }

  @Override
  public Table getTranslateTableDryrun(Table tbl)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    return new Table();
  }

  @Override
  public void createTable(Table tbl)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    List<FieldSchema> cols = Lists.newArrayList(tbl.getSd().getCols());
    if (tbl.isSetPartitionKeys() && !tbl.getPartitionKeys().isEmpty()) {
      cols.addAll(tbl.getPartitionKeys());
    }
    Map<String, String> catalogProperties = getCatalogProperties(tbl);
    Schema schema = HiveSchemaUtil.convert(cols, true);
    SortOrder sortOrder = HMSTablePropertyHelper.getSortOrder(catalogProperties, schema);
    org.apache.iceberg.PartitionSpec partitionSpec = HMSTablePropertyHelper.createPartitionSpec(this.conf, schema);
    org.apache.iceberg.Table table = restCatalog
        .buildTable(TableIdentifier.of(tbl.getDbName(), tbl.getTableName()), schema)
        .withPartitionSpec(partitionSpec)
        .withLocation(catalogProperties.get(LOCATION))
        .withProperties(catalogProperties)
        .withSortOrder(sortOrder)
        .create();
  }

  /**
   * Calculates the properties we would like to send to the catalog.
   * <ul>
   * <li>The base of the properties is the properties stored at the Hive Metastore for the given table
   * <li>We add the {@link HiveIcebergRESTCatalogClientAdapter#LOCATION} as the table location
   * <li>We add the {@link HiveIcebergRESTCatalogClientAdapter#NAME} as
   * TableIdentifier defined by the database name and table name
   * <li>We add the serdeProperties of the HMS table
   * <li>We remove some parameters that we don't want to push down to the Iceberg table props
   * </ul>
   * @param hmsTable Table for which we are calculating the properties
   * @return The properties we can provide for Iceberg functions
   */
  private static Map<String, String> getCatalogProperties(org.apache.hadoop.hive.metastore.api.Table hmsTable) {
    Map<String, String> properties = Maps.newHashMap();
    hmsTable.getParameters().entrySet().stream().filter(e -> e.getKey() != null && e.getValue() != null).forEach(e -> {
      // translate key names between HMS and Iceberg where needed
      String icebergKey = HMSTablePropertyHelper.translateToIcebergProp(e.getKey());
      properties.put(icebergKey, e.getValue());
    });

    if (properties.get(LOCATION) == null && hmsTable.getSd() != null && hmsTable.getSd().getLocation() != null) {
      properties.put(LOCATION, hmsTable.getSd().getLocation());
    }

    if (properties.get(NAME) == null) {
      properties.put(NAME, TableIdentifier.of(hmsTable.getDbName(), hmsTable.getTableName()).toString());
    }

    SerDeInfo serdeInfo = hmsTable.getSd().getSerdeInfo();
    if (serdeInfo != null) {
      serdeInfo.getParameters().entrySet().stream().filter(e -> e.getKey() != null && e.getValue() != null)
          .forEach(e -> {
            String icebergKey = HMSTablePropertyHelper.translateToIcebergProp(e.getKey());
            properties.put(icebergKey, e.getValue());
          });
    }

    // Remove HMS table parameters we don't want to propagate to Iceberg
    PROPERTIES_TO_REMOVE.forEach(properties::remove);

    return properties;
  }

  private HiveMetaHook getHook(Table tbl) throws MetaException {
    if (hookLoader == null) {
      return null;
    }
    return hookLoader.getHook(tbl);
  }
  @Override
  public void createTable(CreateTableRequest request)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    createTable(request.getTable());
  }

  @Override
  public void alter_table(String databaseName, String tblName, Table table)
      throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alter_table(String catName, String dbName, String tblName, Table newTable, EnvironmentContext envContext)
      throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alter_table(String defaultDatabaseName, String tblName, Table table, boolean cascade)
      throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alter_table_with_environmentContext(String databaseName, String tblName, Table table,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alter_table(String catName, String databaseName, String tblName, Table table,
      EnvironmentContext environmentContext, String validWriteIdList)
      throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void createDatabase(Database db)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    Map<String, String> props = Maps.newHashMap();
    props.put(LOCATION, db.getLocationUri());
    props.put(DB_OWNER, db.getOwnerName());
    props.put(DB_OWNER_TYPE, db.getOwnerType().toString());
    restCatalog.createNamespace(Namespace.of(db.getName()), props);
  }

  @Override
  public void dropDatabase(String name)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    restCatalog.dropNamespace(Namespace.of(name));
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropDatabase(name);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropDatabase(name);
  }

  @Override
  public void dropDatabase(DropDatabaseRequest req) throws TException {
    dropDatabase(req.getName());
  }

  @Override
  public void alterDatabase(String name, Database db) throws NoSuchObjectException, MetaException, TException {

  }

  @Override
  public void alterDatabase(String catName, String dbName, Database newDb)
      throws NoSuchObjectException, MetaException, TException {

  }

  @Override
  public void createDataConnector(DataConnector connector)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
  }

  @Override
  public void dropDataConnector(String name, boolean ifNotExists, boolean checkReferences)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alterDataConnector(String name, DataConnector connector)
      throws NoSuchObjectException, MetaException, TException {

  }

  @Override
  public DataConnector getDataConnector(String name) throws MetaException, TException {
    return new DataConnector();
  }

  @Override
  public List<String> getAllDataConnectorNames() throws MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return false;
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
      boolean deleteData) throws NoSuchObjectException, MetaException, TException {
    return false;
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException {
    return false;
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
      PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
    return false;
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName, List<Pair<Integer, byte[]>> partExprs,
      boolean deleteData, boolean ifExists) throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName, List<Pair<Integer, byte[]>> partExprs,
      boolean deleteData, boolean ifExists, boolean needResults)
      throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName, List<Pair<Integer, byte[]>> partExprs,
      PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
    return null;
  }

  @Override
  public List<Partition> dropPartitions(String catName, String dbName, String tblName,
      List<Pair<Integer, byte[]>> partExprs, PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name, String name, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return false;
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name, String name, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return false;
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alter_partition(String catName, String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext, String writeIdList)
      throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alter_partition(String catName, String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts)
      throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext, String writeIdList, long writeId)
      throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void alter_partitions(String catName, String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext, String writeIdList, long writeId)
      throws InvalidOperationException, MetaException, TException {

  }

  @Override
  public void renamePartition(String dbname, String tableName, List<String> part_vals, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
  }

  @Override
  public void renamePartition(String catName, String dbname, String tableName, List<String> part_vals,
      Partition newPart, String validWriteIds, long txnId, boolean makeCopy) throws TException {

  }

  @Override
  public List<FieldSchema> getFields(String db, String tableName)
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    return new LinkedList<>();
  }

  @Override
  public List<FieldSchema> getFields(String catName, String db, String tableName)
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    return new LinkedList<>();
  }

  @Override
  public GetFieldsResponse getFieldsRequest(GetFieldsRequest req)
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    return new GetFieldsResponse();
  }

  @Override
  public List<FieldSchema> getSchema(String db, String tableName)
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    return new LinkedList<>();
  }

  @Override
  public List<FieldSchema> getSchema(String catName, String db, String tableName)
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    return new LinkedList<>();
  }

  @Override
  public GetSchemaResponse getSchemaRequest(GetSchemaRequest req)
      throws MetaException, TException, UnknownTableException, UnknownDBException {
    return new GetSchemaResponse();
  }

  @Override
  public String getConfigValue(String name, String defaultValue) throws TException, ConfigValSecurityException {
    return "50";
  }

  @Override
  public List<String> partitionNameToVals(String name) throws MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
    return Maps.newHashMap();
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    return false;
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    return false;
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames,
      String engine) throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames,
      String engine, String validWriteIdList) throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colNames, String engine) throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colNames, String engine, String validWriteIdList)
      throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, TException {
    return Maps.newHashMap();
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine, String validWriteIdList)
      throws NoSuchObjectException, MetaException, TException {
    return Maps.newHashMap();
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catName, String dbName,
      String tableName, List<String> partNames, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, TException {
    return Maps.newHashMap();
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catName, String dbName,
      String tableName, List<String> partNames, List<String> colNames, String engine, String validWriteIdList)
      throws NoSuchObjectException, MetaException, TException {
    return null;
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName, String colName,
      String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    return false;
  }

  @Override
  public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName, String partName,
      String colName, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    return false;
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    return false;
  }

  @Override
  public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName, String colName,
      String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    return false;
  }

  @Override
  public boolean deleteColumnStatistics(DeleteColumnStatisticsRequest req) throws TException {
    return false;
  }

  @Override
  public void updateTransactionalStatistics(UpdateTransactionalStatsRequest req) throws TException {

  }

  @Override
  public boolean create_role(Role role) throws MetaException, TException {
    return false;
  }

  @Override
  public boolean drop_role(String role_name) throws MetaException, TException {
    return false;
  }

  @Override
  public List<String> listRoleNames() throws MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public boolean grant_role(String role_name, String user_name, PrincipalType principalType, String grantor,
      PrincipalType grantorType, boolean grantOption) throws MetaException, TException {
    return false;
  }

  @Override
  public boolean revoke_role(String role_name, String user_name, PrincipalType principalType, boolean grantOption)
      throws MetaException, TException {
    return false;
  }

  @Override
  public List<Role> list_roles(String principalName, PrincipalType principalType) throws MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name, List<String> group_names)
      throws MetaException, TException {
    return new PrincipalPrivilegeSet();
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String principal_name, PrincipalType principal_type,
      HiveObjectRef hiveObject) throws MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
    return false;
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption) throws MetaException, TException {
    return false;
  }

  @Override
  public boolean refresh_privileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
      throws MetaException, TException {
    return false;
  }

  @Override
  public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws MetaException, TException {
    return "";
  }

  @Override
  public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
    return 0;
  }

  @Override
  public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {

  }

  @Override
  public String getTokenStrForm() throws IOException {
    return "";
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
    return false;
  }

  @Override
  public boolean removeToken(String tokenIdentifier) throws TException {
    return false;
  }

  @Override
  public String getToken(String tokenIdentifier) throws TException {
    return "";
  }

  @Override
  public List<String> getAllTokenIdentifiers() throws TException {
    return new LinkedList<>();
  }

  @Override
  public int addMasterKey(String key) throws MetaException, TException {
    return 0;
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException, MetaException, TException {

  }

  @Override
  public boolean removeMasterKey(Integer keySeq) throws TException {
    return false;
  }

  @Override
  public String[] getMasterKeys() throws TException {
    return new String[0];
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException, MetaException, TException {

  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException, TException {

  }

  @Override
  public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException, TException {

  }

  @Override
  public void dropFunction(String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {

  }

  @Override
  public void dropFunction(String catName, String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {

  }

  @Override
  public Function getFunction(String dbName, String funcName) throws MetaException, TException {
    return new Function();
  }

  @Override
  public Function getFunction(String catName, String dbName, String funcName) throws MetaException, TException {
    return new Function();
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public GetFunctionsResponse getFunctionsRequest(GetFunctionsRequest functionRequest) throws TException {
    return new GetFunctionsResponse();
  }

  @Override
  public List<String> getFunctions(String catName, String dbName, String pattern) throws MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
    return new GetAllFunctionsResponse();
  }

  @Override
  public GetOpenTxnsResponse getOpenTxns() throws TException {
    return new GetOpenTxnsResponse();
  }

  @Override
  public ValidTxnList getValidTxns() throws TException {
    return new ValidReadTxnList();
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn) throws TException {
    return new ValidReadTxnList();
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn, List<TxnType> excludeTxnTypes) throws TException {
    return new ValidReadTxnList();
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
    return new ValidCleanerWriteIdList("", 0);
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName, Long writeId) throws TException {
    return new ValidCleanerWriteIdList("", 0);
  }

  @Override
  public List<TableValidWriteIds> getValidWriteIds(List<String> tablesList, String validTxnList) throws TException {
    return new LinkedList<>();
  }

  @Override
  public void addWriteIdsToMinHistory(long txnId, Map<String, Long> writeIds) throws TException {

  }

  @Override
  public long openTxn(String user) throws TException {
    return 0;
  }

  @Override
  public long openTxn(String user, TxnType txnType) throws TException {
    return 0;
  }

  @Override
  public List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user, TxnType txnType)
      throws TException {
    return new LinkedList<>();
  }

  @Override
  public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    return new OpenTxnsResponse();
  }

  @Override
  public void rollbackTxn(long txnid) throws NoSuchTxnException, TException {

  }

  @Override
  public void rollbackTxn(AbortTxnRequest abortTxnRequest) throws NoSuchTxnException, TException {

  }

  @Override
  public void replRollbackTxn(long srcTxnid, String replPolicy, TxnType txnType) throws NoSuchTxnException, TException {

  }

  @Override
  public void commitTxn(long txnid) throws NoSuchTxnException, TxnAbortedException, TException {

  }

  @Override
  public void commitTxnWithKeyValue(long txnid, long tableId, String key, String value)
      throws NoSuchTxnException, TxnAbortedException, TException {

  }

  @Override
  public void commitTxn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {

  }

  @Override
  public void abortTxns(List<Long> txnids) throws TException {

  }

  @Override
  public void abortTxns(AbortTxnsRequest abortTxnsRequest) throws TException {

  }

  @Override
  public long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
    return 0;
  }

  @Override
  public long allocateTableWriteId(long txnId, String dbName, String tableName, boolean reallocate) throws TException {
    return 0;
  }

  @Override
  public void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames)
      throws TException {

  }

  @Override
  public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName)
      throws TException {
    return new LinkedList<>();
  }

  @Override
  public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName, String replPolicy,
      List<TxnToWriteId> srcTxnToWriteIdList) throws TException {
    return new LinkedList<>();
  }

  @Override
  public long getMaxAllocatedWriteId(String dbName, String tableName) throws TException {
    return 0;
  }

  @Override
  public void seedWriteId(String dbName, String tableName, long seedWriteId) throws TException {

  }

  @Override
  public void seedTxnId(long seedTxnId) throws TException {

  }

  @Override
  public GetOpenTxnsInfoResponse showTxns() throws TException {
    return new GetOpenTxnsInfoResponse();
  }

  @Override
  public LockResponse lock(LockRequest request) throws NoSuchTxnException, TxnAbortedException, TException {
    return new LockResponse();
  }

  @Override
  public LockResponse checkLock(long lockid)
      throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    return new LockResponse();
  }

  @Override
  public void unlock(long lockid) throws NoSuchLockException, TxnOpenException, TException {

  }

  @Override
  public ShowLocksResponse showLocks() throws TException {
    return new ShowLocksResponse();
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    return new ShowLocksResponse();
  }

  @Override
  public void heartbeat(long txnid, long lockid)
      throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {

  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
    return new HeartbeatTxnRangeResponse();
  }

  @Override
  public void compact(String dbname, String tableName, String partitionName, CompactionType type) throws TException {

  }

  @Override
  public void compact(String dbname, String tableName, String partitionName, CompactionType type,
      Map<String, String> tblproperties) throws TException {

  }

  @Override
  public CompactionResponse compact2(String dbname, String tableName, String partitionName, CompactionType type,
      Map<String, String> tblproperties) throws TException {
    return new CompactionResponse();
  }

  @Override
  public CompactionResponse compact2(CompactionRequest request) throws TException {
    return new CompactionResponse();
  }

  @Override
  public ShowCompactResponse showCompactions() throws TException {
    return new ShowCompactResponse();
  }

  @Override
  public ShowCompactResponse showCompactions(ShowCompactRequest request) throws TException {
    return new ShowCompactResponse();
  }

  @Override
  public boolean submitForCleanup(CompactionRequest rqst, long highestWriteId, long txnId) throws TException {
    return false;
  }

  @Override
  public GetLatestCommittedCompactionInfoResponse getLatestCommittedCompactionInfo(
      GetLatestCommittedCompactionInfoRequest request) throws TException {
    return new GetLatestCommittedCompactionInfoResponse();
  }

  @Override
  public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames)
      throws TException {

  }

  @Override
  public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames,
      DataOperationType operationType) throws TException {

  }

  @Override
  public void insertTable(Table table, boolean overwrite) throws MetaException {

  }

  @Override
  public long getLatestTxnIdInConflict(long txnId) throws TException {
    return 0;
  }

  @Override
  public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents, NotificationFilter filter)
      throws TException {
    return new NotificationEventResponse();
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest request, boolean allowGapsInEventIds,
      NotificationFilter filter) throws TException {
    return new NotificationEventResponse();
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    return new CurrentNotificationEventId();
  }

  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst)
      throws TException {
    return new NotificationEventsCountResponse();
  }

  @Override
  public FireEventResponse fireListenerEvent(FireEventRequest request) throws TException {
    return new FireEventResponse();
  }

  @Override
  public void addWriteNotificationLog(WriteNotificationLogRequest rqst) throws TException {

  }

  @Override
  public void addWriteNotificationLogInBatch(WriteNotificationLogBatchRequest rqst) throws TException {

  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincRoleReq)
      throws MetaException, TException {
    return new GetPrincipalsInRoleResponse();
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException {
    return new GetRoleGrantsForPrincipalResponse();
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partName,
      String engine) throws NoSuchObjectException, MetaException, TException {
    return new AggrStats();
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partName,
      String engine, String writeIdList) throws NoSuchObjectException, MetaException, TException {
    return new AggrStats();
  }

  @Override
  public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames,
      List<String> partNames, String engine) throws NoSuchObjectException, MetaException, TException {
    return new AggrStats();
  }

  @Override
  public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames,
      List<String> partNames, String engine, String writeIdList)
      throws NoSuchObjectException, MetaException, TException {
    return new AggrStats();
  }

  @Override
  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    return false;
  }

  @Override
  public void flushCache() {

  }

  @Override
  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
    return new LinkedList<>();
  }

  @Override
  public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(List<Long> fileIds, ByteBuffer sarg,
      boolean doGetFooters) throws TException {
    return new LinkedList<>();
  }

  @Override
  public void clearFileMetadata(List<Long> fileIds) throws TException {

  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {

  }

  @Override
  public boolean isSameConfObj(Configuration c) {
    return false;
  }

  @Override
  public boolean cacheFileMetadata(String dbName, String tableName, String partName, boolean allParts)
      throws TException {
    return false;
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return new LinkedList<>();
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return new LinkedList<>();
  }

  @Override
  public SQLAllTableConstraints getAllTableConstraints(AllTableConstraintsRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return new SQLAllTableConstraints();
  }

  @Override
  public void createTableWithConstraints(Table tTbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
      List<SQLUniqueConstraint> uniqueConstraints, List<SQLNotNullConstraint> notNullConstraints,
      List<SQLDefaultConstraint> defaultConstraints, List<SQLCheckConstraint> checkConstraints)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    createTable(tTbl);
  }

  @Override
  public void dropConstraint(String dbName, String tableName, String constraintName)
      throws MetaException, NoSuchObjectException, TException {

  }

  @Override
  public void dropConstraint(String catName, String dbName, String tableName, String constraintName)
      throws MetaException, NoSuchObjectException, TException {

  }

  @Override
  public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
      throws MetaException, NoSuchObjectException, TException {

  }

  @Override
  public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
      throws MetaException, NoSuchObjectException, TException {

  }

  @Override
  public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols)
      throws MetaException, NoSuchObjectException, TException {

  }

  @Override
  public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols)
      throws MetaException, NoSuchObjectException, TException {

  }

  @Override
  public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints)
      throws MetaException, NoSuchObjectException, TException {

  }

  @Override
  public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints)
      throws MetaException, NoSuchObjectException, TException {

  }

  @Override
  public String getMetastoreDbUuid() throws MetaException, TException {
    return "";
  }

  @Override
  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName)
      throws InvalidObjectException, MetaException, TException {

  }

  @Override
  public WMFullResourcePlan getResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException, TException {
    return new WMFullResourcePlan();
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans(String ns) throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public void dropResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException, TException {

  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String resourcePlanName, String ns, WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean isForceDeactivate, boolean isReplace)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    return new WMFullResourcePlan();
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan(String ns) throws MetaException, TException {
    return null;
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    return new WMValidateResourcePlanResponse();
  }

  @Override
  public void createWMTrigger(WMTrigger trigger) throws InvalidObjectException, MetaException, TException {

  }

  @Override
  public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName, String ns)
      throws NoSuchObjectException, MetaException, TException {

  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlan, String ns)
      throws NoSuchObjectException, MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public void createWMPool(WMPool pool)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {

  }

  @Override
  public void alterWMPool(WMNullablePool pool, String poolPath)
      throws NoSuchObjectException, InvalidObjectException, TException {

  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath, String ns) throws TException {

  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate) throws TException {

  }

  @Override
  public void dropWMMapping(WMMapping mapping) throws TException {

  }

  @Override
  public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath,
      boolean shouldDrop, String ns)
      throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {

  }

  @Override
  public void createISchema(ISchema schema) throws TException {

  }

  @Override
  public void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema) throws TException {

  }

  @Override
  public ISchema getISchema(String catName, String dbName, String name) throws TException {
    return new ISchema();
  }

  @Override
  public void dropISchema(String catName, String dbName, String name) throws TException {

  }

  @Override
  public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {

  }

  @Override
  public SchemaVersion getSchemaVersion(String catName, String dbName, String schemaName, int version)
      throws TException {
    return new SchemaVersion();
  }

  @Override
  public SchemaVersion getSchemaLatestVersion(String catName, String dbName, String schemaName) throws TException {
    return new SchemaVersion();
  }

  @Override
  public List<SchemaVersion> getSchemaAllVersions(String catName, String dbName, String schemaName) throws TException {
    return new LinkedList<>();
  }

  @Override
  public void dropSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {

  }

  @Override
  public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst rqst) throws TException {
    return new FindSchemasByColsResp();
  }

  @Override
  public void mapSchemaVersionToSerde(String catName, String dbName, String schemaName, int version, String serdeName)
      throws TException {

  }

  @Override
  public void setSchemaVersionState(String catName, String dbName, String schemaName, int version,
      SchemaVersionState state) throws TException {

  }

  @Override
  public void addSerDe(SerDeInfo serDeInfo) throws TException {

  }

  @Override
  public SerDeInfo getSerDe(String serDeName) throws TException {
    return new SerDeInfo();
  }

  @Override
  public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
    return new LockResponse();
  }

  @Override
  public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
    return false;
  }

  @Override
  public void addRuntimeStat(RuntimeStat stat) throws TException {

  }

  @Override
  public List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException {
    return new LinkedList<>();
  }

  @Override
  public GetPartitionsResponse getPartitionsWithSpecs(GetPartitionsRequest request) throws TException {
    return new GetPartitionsResponse();
  }

  @Override
  public OptionalCompactionInfoStruct findNextCompact(String workerId) throws MetaException, TException {
    return new OptionalCompactionInfoStruct();
  }

  @Override
  public OptionalCompactionInfoStruct findNextCompact(FindNextCompactRequest rqst) throws MetaException, TException {
    return new OptionalCompactionInfoStruct();
  }

  @Override
  public void updateCompactorState(CompactionInfoStruct cr, long txnId) throws TException {

  }

  @Override
  public List<String> findColumnsWithStats(CompactionInfoStruct cr) throws TException {
    return new LinkedList<>();
  }

  @Override
  public void markCleaned(CompactionInfoStruct cr) throws MetaException, TException {

  }

  @Override
  public void markCompacted(CompactionInfoStruct cr) throws MetaException, TException {

  }

  @Override
  public void markFailed(CompactionInfoStruct cr) throws MetaException, TException {

  }

  @Override
  public void markRefused(CompactionInfoStruct cr) throws MetaException, TException {

  }

  @Override
  public boolean updateCompactionMetricsData(CompactionMetricsDataStruct struct) throws MetaException, TException {
    return false;
  }

  @Override
  public void removeCompactionMetricsData(CompactionMetricsDataRequest request) throws MetaException, TException {

  }

  @Override
  public void setHadoopJobid(String jobId, long cqId) throws MetaException, TException {

  }

  @Override
  public String getServerVersion() throws TException {
    return "";
  }

  @Override
  public ScheduledQuery getScheduledQuery(ScheduledQueryKey scheduleKey) throws TException {
    return new ScheduledQuery();
  }

  @Override
  public void scheduledQueryMaintenance(ScheduledQueryMaintenanceRequest request) throws MetaException, TException {

  }

  @Override
  public ScheduledQueryPollResponse scheduledQueryPoll(ScheduledQueryPollRequest request)
      throws MetaException, TException {
    return new ScheduledQueryPollResponse();
  }

  @Override
  public void scheduledQueryProgress(ScheduledQueryProgressInfo info) throws TException {

  }

  @Override
  public void addReplicationMetrics(ReplicationMetricList replicationMetricList) throws MetaException, TException {

  }

  @Override
  public ReplicationMetricList getReplicationMetrics(GetReplicationMetricsRequest replicationMetricsRequest)
      throws MetaException, TException {
    return new ReplicationMetricList();
  }

  @Override
  public void createStoredProcedure(StoredProcedure proc) throws NoSuchObjectException, MetaException, TException {

  }

  @Override
  public StoredProcedure getStoredProcedure(StoredProcedureRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return new StoredProcedure();
  }

  @Override
  public void dropStoredProcedure(StoredProcedureRequest request)
      throws MetaException, NoSuchObjectException, TException {

  }

  @Override
  public List<String> getAllStoredProcedures(ListStoredProcedureRequest request) throws MetaException, TException {
    return new LinkedList<>();
  }

  @Override
  public void addPackage(AddPackageRequest request) throws NoSuchObjectException, MetaException, TException {

  }

  @Override
  public Package findPackage(GetPackageRequest request) throws TException {
    return new Package();
  }

  @Override
  public List<String> listPackages(ListPackageRequest request) throws TException {
    return new LinkedList<>();
  }

  @Override
  public void dropPackage(DropPackageRequest request) throws TException {

  }

  @Override
  public List<WriteEventInfo> getAllWriteEventInfo(GetAllWriteEventInfoRequest request) throws TException {
    return new LinkedList<>();
  }

  @Override
  public AbortCompactResponse abortCompactions(AbortCompactionRequest request) throws TException {
    return new AbortCompactResponse();
  }

  public Configuration getConf() {
    return conf;
  }
}
