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

package org.apache.iceberg.hive.client;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.BaseMetaStoreClient;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HMSTablePropertyHelper;
import org.apache.iceberg.hive.HiveOperationsBase;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.hive.IcebergCatalogProperties;
import org.apache.iceberg.hive.IcebergTableProperties;
import org.apache.iceberg.hive.IcebergViewSupport;
import org.apache.iceberg.hive.MetastoreUtil;
import org.apache.iceberg.hive.RuntimeMetaException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveRESTCatalogClient extends BaseMetaStoreClient {

  public static final String NAMESPACE_SEPARATOR = ".";
  public static final String DB_OWNER = "owner";
  public static final String DB_OWNER_TYPE = "ownerType";

  private static final Logger LOG = LoggerFactory.getLogger(HiveRESTCatalogClient.class);

  private RESTCatalog restCatalog;

  public HiveRESTCatalogClient(Configuration conf) {
    super(conf);
    reconnect();
  }

  @Override
  public void reconnect()  {
    close();
    String catName = MetaStoreUtils.getDefaultCatalog(conf);
    Map<String, String> properties = IcebergCatalogProperties.getCatalogProperties(conf);
    restCatalog = (RESTCatalog) CatalogUtil.buildIcebergCatalog(catName, properties, null);
  }

  @Override
  public void close() {
    try {
      if (restCatalog != null) {
        restCatalog.close();
      }
    } catch (IOException e) {
      throw new RuntimeMetaException(e.getCause(), "Failed to close existing REST catalog");
    }
  }

  @Override
  public List<String> getDatabases(String catName, String dbPattern) {
    validateCurrentCatalog(catName);
    // Convert the Hive glob pattern (e.g., "db*") to a valid Java regex ("db.*").
    String regex = dbPattern.replace("*", ".*");
    Pattern pattern = Pattern.compile(regex);

    return restCatalog.listNamespaces(Namespace.empty()).stream()
        .map(Namespace::toString)
        .filter(pattern.asPredicate())
        .toList();
  }

  @Override
  public List<String> getAllDatabases(String catName) {
    return getDatabases(catName, "*");
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern) {
    validateCurrentCatalog(catName);

    // Convert the Hive glob pattern to a Java regex.
    String regex = tablePattern.replace("*", ".*");
    Pattern pattern = Pattern.compile(regex);

    // List tables from the specific database (namespace) and filter them.
    Set<String> names = new LinkedHashSet<>();
    restCatalog.listTables(Namespace.of(dbName)).stream()
        .map(TableIdentifier::name)
        .filter(pattern.asPredicate())
        .forEach(names::add);

    if (restCatalog instanceof ViewCatalog viewCatalog) {
      viewCatalog
          .listViews(Namespace.of(dbName)).stream()
          .map(TableIdentifier::name)
          .filter(pattern.asPredicate())
          .forEach(names::add);
    }
    return Lists.newArrayList(names);
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) {
    return getTables(catName, dbName, "*");
  }

  @Override
  public void dropTable(Table table, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws TException {
    TableIdentifier id = TableIdentifier.of(table.getDbName(), table.getTableName());
    if (restCatalog instanceof ViewCatalog viewCatalog && viewCatalog.viewExists(id)) {
      viewCatalog.dropView(id);
    } else {
      restCatalog.dropTable(id);
    }
  }

  private void validateCurrentCatalog(String catName) {
    if (!restCatalog.name().equals(catName)) {
      throw new IllegalArgumentException(
          String.format("Catalog name '%s' does not match the current catalog '%s'", catName, restCatalog.name()));
    }
  }

  @Override
  public boolean tableExists(String catName, String dbName, String tableName) {
    validateCurrentCatalog(catName);
    TableIdentifier id = TableIdentifier.of(dbName, tableName);
    if (restCatalog.tableExists(id)) {
      return true;
    }
    return restCatalog instanceof ViewCatalog viewCatalog && viewCatalog.viewExists(id);
  }

  @Override
  public Database getDatabase(String catName, String dbName) throws NoSuchObjectException {
    validateCurrentCatalog(catName);

    return restCatalog.listNamespaces(Namespace.empty()).stream()
        .filter(namespace -> namespace.levels()[0].equals(dbName))
        .map(namespace -> {
          Database database = new Database();
          database.setName(String.join(NAMESPACE_SEPARATOR, namespace.levels()));
          Map<String, String> namespaceMetadata = restCatalog.loadNamespaceMetadata(Namespace.of(dbName));
          database.setLocationUri(namespaceMetadata.get(IcebergTableProperties.LOCATION));
          database.setCatalogName(restCatalog.name());
          database.setOwnerName(namespaceMetadata.get(DB_OWNER));
          try {
            database.setOwnerType(PrincipalType.valueOf(namespaceMetadata.get(DB_OWNER_TYPE)));
          } catch (Exception e) {
            LOG.warn("Can not set ownerType: {}", namespaceMetadata.get(DB_OWNER_TYPE), e);
          }
          return database;
        }).findFirst().orElseThrow(() ->
            new NoSuchObjectException("Database " + dbName + " not found"));
  }

  @Override
  public Table getTable(GetTableRequest tableRequest) throws TException {
    validateCurrentCatalog(tableRequest.getCatName());
    TableIdentifier id =
        TableIdentifier.of(tableRequest.getDbName(), tableRequest.getTblName());
    try {
      org.apache.iceberg.Table icebergTable = restCatalog.loadTable(id);
      return MetastoreUtil.toHiveTable(icebergTable, conf);
    } catch (NoSuchTableException tableMissing) {
      if (restCatalog instanceof ViewCatalog viewCatalog) {
        if (!viewCatalog.viewExists(id)) {
          throw new NoSuchObjectException();
        }
        return MetastoreUtil.buildMinimalHMSView(
            tableRequest.getCatName(), tableRequest.getDbName(), tableRequest.getTblName());
      }
      throw new NoSuchObjectException();
    }
  }

  private static boolean hasIcebergViewTableType(Table table) {
    if (!TableType.VIRTUAL_VIEW.toString().equals(table.getTableType())) {
      return false;
    }
    Map<String, String> params = table.getParameters();
    if (params == null) {
      return false;
    }
    return HiveOperationsBase.ICEBERG_VIEW_TYPE_VALUE.equalsIgnoreCase(
        params.get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));
  }

  @Override
  public void alter_table(String catName, String dbName, String tblName, Table newTable,
      EnvironmentContext envContext, String validWriteIdList) {
    validateCurrentCatalog(catName);
    if (hasIcebergViewTableType(newTable) && restCatalog instanceof ViewCatalog) {
      createOrReplaceIcebergView(newTable, dbName, tblName);
    }
  }

  @Override
  public void createTable(CreateTableRequest request) throws TException {
    Table table = request.getTable();
    if (hasIcebergViewTableType(table) && restCatalog instanceof ViewCatalog) {
      createOrReplaceIcebergView(table, table.getDbName(), table.getTableName());
    } else {
      createIcebergTable(request);
    }
  }

  private void createIcebergTable(CreateTableRequest request) {
    Table table = request.getTable();
    Properties tableProperties = IcebergTableProperties.getTableProperties(table, conf);
    Schema schema = HiveSchemaUtil.convert(hmsTableColumns(table), Collections.emptyMap(), true);
    Map<String, String> envCtxProps = Optional.ofNullable(request.getEnvContext())
        .map(EnvironmentContext::getProperties)
        .orElse(Collections.emptyMap());
    org.apache.iceberg.PartitionSpec partitionSpec =
        HMSTablePropertyHelper.getPartitionSpec(envCtxProps, schema);
    SortOrder sortOrder = HMSTablePropertyHelper.getSortOrder(tableProperties, schema);

    restCatalog
        .buildTable(TableIdentifier.of(table.getDbName(), table.getTableName()), schema)
        .withPartitionSpec(partitionSpec)
        .withLocation(tableProperties.getProperty(IcebergTableProperties.LOCATION))
        .withSortOrder(sortOrder)
        .withProperties(Maps.fromProperties(tableProperties))
        .create();
  }

  private void createOrReplaceIcebergView(Table table, String dbName, String tableName) {
    Map<String, String> tblProps =
        table.getParameters() == null ? Maps.newHashMap() : Maps.newHashMap(table.getParameters());
    String comment = tblProps.get("comment");
    List<FieldSchema> cols = Lists.newArrayList(table.getSd().getCols());
    IcebergViewSupport.createOrReplaceView(
        conf, dbName, tableName, cols, table.getViewExpandedText(), tblProps, comment);
  }

  private static List<FieldSchema> hmsTableColumns(Table table) {
    List<FieldSchema> cols = Lists.newArrayList(table.getSd().getCols());
    if (table.isSetPartitionKeys() && !table.getPartitionKeys().isEmpty()) {
      cols.addAll(table.getPartitionKeys());
    }
    return cols;
  }

  @Override
  public void createDatabase(Database db) {
    validateCurrentCatalog(db.getCatalogName());
    Map<String, String> props = ImmutableMap.of(
        IcebergTableProperties.LOCATION, db.getLocationUri(),
        DB_OWNER, db.getOwnerName(),
        DB_OWNER_TYPE, db.getOwnerType().toString()
    );
    restCatalog.createNamespace(Namespace.of(db.getName()), props);
  }


  @Override
  public void dropDatabase(DropDatabaseRequest req) {
    validateCurrentCatalog(req.getCatalogName());
    restCatalog.dropNamespace(Namespace.of(req.getName()));
  }

  @Override
  public List<Table> getAllMaterializedViewObjectsForRewriting() {
    return Collections.emptyList();
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catName, String dbName) {
    return Collections.emptyList();
  }
}
