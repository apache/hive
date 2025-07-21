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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.CatalogUtils;
import org.apache.iceberg.hive.HMSTablePropertyHelper;
import org.apache.iceberg.hive.HiveSchemaUtil;
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
  public static final String CATALOG_CONFIG_PREFIX = "iceberg.rest-catalog.";

  private RESTCatalog restCatalog;

  public HiveRESTCatalogClient(Configuration conf, boolean allowEmbedded) {
    this(conf);
  }

  public HiveRESTCatalogClient(Configuration conf) {
    super(conf);
    reconnect();
  }

  @Override
  public void reconnect()  {
    close();
    Map<String, String> properties = getCatalogProperties(conf);
    String catalogName = MetaStoreUtils.getDefaultCatalog(conf);
    restCatalog = (RESTCatalog) CatalogUtil.buildIcebergCatalog(catalogName, properties, null);
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

  private static Map<String, String> getCatalogProperties(Configuration conf) {
    Map<String, String> catalogProperties = Maps.newHashMap();
    conf.forEach(config -> {
      if (config.getKey().startsWith(CATALOG_CONFIG_PREFIX)) {
        catalogProperties.put(
            config.getKey().substring(CATALOG_CONFIG_PREFIX.length()),
            config.getValue());
      }
    });
    catalogProperties.put(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    return catalogProperties;
  }


  @Override
  public List<String> getDatabases(String catName, String databasePattern) {
    validateCurrentCatalog(catName);
    // Convert the Hive glob pattern (e.g., "db*") to a valid Java regex ("db.*").
    String regex = databasePattern.replace("*", ".*");
    Pattern pattern = Pattern.compile(regex);

    return restCatalog.listNamespaces(Namespace.empty()).stream()
        .map(Namespace::toString)
        .filter(pattern.asPredicate())
        .collect(Collectors.toList());
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
    return restCatalog.listTables(Namespace.of(dbName)).stream()
        .map(TableIdentifier::name)
        .filter(pattern.asPredicate())
        .collect(Collectors.toList());
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) {
    return getTables(catName, dbName, "*");
  }

  @Override
  public void dropTable(Table table, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws TException {
    restCatalog.dropTable(TableIdentifier.of(table.getDbName(), table.getTableName()));
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
    return restCatalog.tableExists(TableIdentifier.of(dbName, tableName));
  }

  @Override
  public Database getDatabase(String catalogName, String databaseName) {
    validateCurrentCatalog(catalogName);
    return restCatalog.listNamespaces(Namespace.empty()).stream()
        .filter(namespace -> namespace.levels()[0].equals(databaseName))
        .map(namespace -> {
          Database database = new Database();
          database.setName(String.join(NAMESPACE_SEPARATOR, namespace.levels()));
          Map<String, String> namespaceMetadata = restCatalog.loadNamespaceMetadata(Namespace.of(databaseName));
          database.setLocationUri(namespaceMetadata.get(CatalogUtils.LOCATION));
          database.setCatalogName(restCatalog.name());
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
  public Table getTable(GetTableRequest getTableRequest) throws TException {
    org.apache.iceberg.Table icebergTable;
    try {
      icebergTable = restCatalog.loadTable(TableIdentifier.of(getTableRequest.getDbName(),
          getTableRequest.getTblName()));
    } catch (NoSuchTableException exception) {
      throw new NoSuchObjectException();
    }
    return MetastoreUtil.convertIcebergTableToHiveTable(icebergTable, conf);
  }

  @Override
  public void createTable(CreateTableRequest request) throws TException {
    Table table = request.getTable();
    List<FieldSchema> cols = Lists.newArrayList(table.getSd().getCols());
    if (table.isSetPartitionKeys() && !table.getPartitionKeys().isEmpty()) {
      cols.addAll(table.getPartitionKeys());
    }
    Properties catalogProperties = CatalogUtils.getCatalogProperties(table);
    Schema schema = HiveSchemaUtil.convert(cols, true);
    Map<String, String> envCtxProps = Optional.ofNullable(request.getEnvContext())
        .map(EnvironmentContext::getProperties)
        .orElse(Collections.emptyMap());
    org.apache.iceberg.PartitionSpec partitionSpec =
        HMSTablePropertyHelper.getPartitionSpec(envCtxProps, schema);
    SortOrder sortOrder = HMSTablePropertyHelper.getSortOrder(catalogProperties, schema);

    restCatalog.buildTable(TableIdentifier.of(table.getDbName(), table.getTableName()), schema)
        .withPartitionSpec(partitionSpec)
        .withLocation(catalogProperties.getProperty(CatalogUtils.LOCATION))
        .withSortOrder(sortOrder)
        .withProperties(catalogProperties.entrySet().stream()
            .collect(Collectors.toMap(entry -> ((Map.Entry<?, ?>) entry).getKey().toString(),
                    entry -> ((Map.Entry<?, ?>) entry).getValue().toString())
            )).create();
  }

  @Override
  public void createDatabase(Database db) {
    Map<String, String> props = ImmutableMap.of(
        CatalogUtils.LOCATION, db.getLocationUri(),
        DB_OWNER, db.getOwnerName(),
        DB_OWNER_TYPE, db.getOwnerType().toString()
    );
    restCatalog.createNamespace(Namespace.of(db.getName()), props);
  }


  @Override
  public void dropDatabase(DropDatabaseRequest req) {
    restCatalog.dropNamespace(Namespace.of(req.getName()));
  }
}
