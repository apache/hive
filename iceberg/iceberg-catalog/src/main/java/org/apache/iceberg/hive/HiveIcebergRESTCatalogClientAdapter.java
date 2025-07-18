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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataStruct;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.client.BaseMetaStoreClient;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergRESTCatalogClientAdapter extends BaseMetaStoreClient {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergRESTCatalogClientAdapter.class);
  public static final String NAMESPACE_SEPARATOR = ".";
  public static final String NAME = "name";
  public static final String LOCATION = "location";
  public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.default_iceberg.type";
  public static final String DB_OWNER = "owner";
  public static final String DB_OWNER_TYPE = "ownerType";
  public static final String DEFAULT_INPUT_FORMAT_CLASS = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat";
  public static final String DEFAULT_OUTPUT_FORMAT_CLASS
      = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat";
  public static final String DEFAULT_SERDE_CLASS = "org.apache.iceberg.mr.hive.HiveIcebergSerDe";
  public static final String CATALOG_CONFIG_PREFIX = "iceberg.rest-catalog.";
  public static final String WAREHOUSE = "warehouse";
  private final Configuration conf;
  private RESTCatalog restCatalog;

  private final long maxHiveTablePropertySize;

  public HiveIcebergRESTCatalogClientAdapter(Configuration conf) {
    super(conf);
    this.conf = conf;
    this.maxHiveTablePropertySize = conf.getLong(HiveOperationsBase.HIVE_TABLE_PROPERTY_MAX_SIZE,
          HiveOperationsBase.HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT);
  }

  @Override
  public void reconnect()  {
    Map<String, String> properties = getCatalogPropertiesFromConf(conf);
    String catalogName = properties.get(WAREHOUSE);
    restCatalog = new RESTCatalog();
    restCatalog.initialize(catalogName, properties);
  }

  @Override
  public void close() {
    try {
      if (restCatalog != null) {
        restCatalog.close();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, String> getCatalogPropertiesFromConf(
      Configuration conf) {
    Map<String, String> catalogProperties = Maps.newHashMap();
    conf.forEach(config -> {
      if (config.getKey().startsWith(CATALOG_CONFIG_PREFIX)) {
        catalogProperties.put(
            config.getKey().substring(CATALOG_CONFIG_PREFIX.length()),
            config.getValue());
      }
    });
    return catalogProperties;
  }


  @Override
  public List<String> getDatabases(String catName, String databasePattern) throws MetaException, TException {
    return restCatalog.listNamespaces(Namespace.empty()).stream().map(Namespace::toString).collect(Collectors.toList());
  }

  @Override
  public List<String> getAllDatabases(String catName) throws MetaException, TException {
    return getAllDatabases();
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern)
      throws MetaException, TException, UnknownDBException {
    return getTables(catName, dbName, tablePattern, null);
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException {
    List<TableIdentifier> tableIdentifiers = restCatalog.listTables(Namespace.of(dbName));
    return tableIdentifiers.stream().map(tableIdentifier -> tableIdentifier.name()).collect(Collectors.toList());
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) throws MetaException, TException, UnknownDBException {
    return getTables(catName, dbName, "", null);
  }

  @Override
  public void dropTable(Table table, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) throws TException {
    restCatalog.dropTable(TableIdentifier.of(table.getDbName(), table.getTableName()));
    dropTable(table.getDbName(), table.getTableName());
  }

  @Override
  public boolean tableExists(String catName, String dbName, String tableName)
      throws MetaException, TException, UnknownDBException {
    return tableExists(dbName, tableName);
  }

  @Override
  public Database getDatabase(String catalogName, String databaseName)
      throws NoSuchObjectException, MetaException, TException {
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

  private Table convertIcebergTableToHiveTable(org.apache.iceberg.Table icebergTable) {
    Table hiveTable = new Table();
    TableMetadata metadata = ((BaseTable) icebergTable).operations().current();
    HMSTablePropertyHelper.updateHmsTableForIcebergTable(metadata.metadataFileLocation(), hiveTable,
        metadata, null, true, maxHiveTablePropertySize, null);
    hiveTable.getParameters().put(ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
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
    SerDeInfo serDeInfo = new SerDeInfo("icebergSerde", DEFAULT_SERDE_CLASS, Maps.newHashMap());
    serDeInfo.getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1"); // Default serialization format.
    storageDescriptor.setSerdeInfo(serDeInfo);
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
  public Table getTable(GetTableRequest getTableRequest) throws MetaException, TException, NoSuchObjectException {
    org.apache.iceberg.Table icebergTable = null;
    try {
      icebergTable = restCatalog.loadTable(
          TableIdentifier.of(getTableRequest.getDbName(), getTableRequest.getTblName()));
    } catch (NoSuchTableException exception) {
      throw new NoSuchObjectException();
    }
    Table hiveTable = convertIcebergTableToHiveTable(icebergTable);
    return hiveTable;
  }

  @Override
  public void createTable(CreateTableRequest request)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    Table table = request.getTable();
    List<FieldSchema> cols = Lists.newArrayList(table.getSd().getCols());
    if (table.isSetPartitionKeys() && !table.getPartitionKeys().isEmpty()) {
      cols.addAll(table.getPartitionKeys());
    }
    Properties catalogProperties = HMSTablePropertyHelper.getCatalogProperties(table);
    Schema schema = HiveSchemaUtil.convert(cols, true);
    SortOrder sortOrder = HMSTablePropertyHelper.getSortOrder(catalogProperties, schema);
    org.apache.iceberg.PartitionSpec partitionSpec = HMSTablePropertyHelper.createPartitionSpec(this.conf, schema);
    restCatalog
        .buildTable(TableIdentifier.of(table.getDbName(), table.getTableName()), schema)
        .withPartitionSpec(partitionSpec)
        .withLocation(catalogProperties.getProperty(LOCATION))
        .withSortOrder(sortOrder)
        .withProperties(
            catalogProperties
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> ((Map.Entry) entry).getKey().toString(),
                    entry -> ((Map.Entry) entry).getValue().toString())
                ))
        .create();
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
  public void dropDatabase(DropDatabaseRequest req) throws TException {
    restCatalog.dropNamespace(Namespace.of(req.getName()));
  }

  @Override
  public void createTableWithConstraints(Table tTbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
      List<SQLUniqueConstraint> uniqueConstraints, List<SQLNotNullConstraint> notNullConstraints,
      List<SQLDefaultConstraint> defaultConstraints, List<SQLCheckConstraint> checkConstraints)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    createTable(tTbl);
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException, TException {
    return null;
  }

  @Override
  public boolean updateCompactionMetricsData(CompactionMetricsDataStruct struct) throws MetaException, TException {
    return false;
  }

  public Configuration getConf() {
    return conf;
  }
}
