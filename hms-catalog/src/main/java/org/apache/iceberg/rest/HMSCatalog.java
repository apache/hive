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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.HMSHandlerProxyFactory;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveHadoopUtil;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;


public class HMSCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable {
  public static final String LIST_ALL_TABLES = "list-all-tables";
  public static final String LIST_ALL_TABLES_DEFAULT = "false";

  public static final String HMS_DB_OWNER = "hive.metastore.database.owner";
  public static final String HMS_DB_OWNER_TYPE = "hive.metastore.database.owner-type";

  private static final Logger LOG = LoggerFactory.getLogger(HMSCatalog.class);

  private String name;
  private String catalog;
  private final Configuration configuration;
  private FileIO fileIO;
  private boolean listAllTables = false;
  private Map<String, String> catalogProperties;
  private IHMSHandler hmsHandler;
  private RawStore rawStore;

  /** The metric names prefix. */
  static final String HMS_METRIC_PREFIX = "hmscatalog.";

  /**
   * @param route a route/api-call name
   * @return the metric counter name for the api-call
   */
  static String hmsCatalogMetricCount(String route) {
    return HMS_METRIC_PREFIX + route.toLowerCase() + ".count";
  }

  /**
   * @param apis an optional list of known api call names
   * @return the list of metric names for the HMSCatalog class
   */
  public static List<String> getMetricNames(String...apis) {
    final List<HMSCatalogAdapter.Route> routes;
    if (apis != null && apis.length > 0) {
      routes = Arrays.asList(apis).stream()
          .map(api -> HMSCatalogAdapter.Route.byName(api))
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    } else {
      routes = Arrays.asList(HMSCatalogAdapter.Route.values());
    }
    final List<String> metricNames = new ArrayList<>(routes.size());
    for(HMSCatalogAdapter.Route route : routes) {
      metricNames.add(hmsCatalogMetricCount(route.name()));
    }
    return metricNames;
  }

  public HMSCatalog(Configuration configuration) {
    if (configuration == null) {
      LOG.warn("No Hadoop Configuration was set, using the default environment Configuration");
      this.configuration = new Configuration();
    } else {
      this.configuration = configuration;
    }
  }

  private IHMSHandler getHandler() throws MetaException {
    if (hmsHandler == null) {
      hmsHandler = new HMSHandler("JSON server", configuration);
      try {
        hmsHandler = HMSHandlerProxyFactory.getProxy(configuration, hmsHandler, true);
      } catch (MetaException e) {
        throw new RuntimeException(e);
      }
    }
    return hmsHandler;
  }

  private RawStore getMS() throws MetaException {
    if (rawStore == null) {
      try {
        rawStore = getHandler().getMS();
      } catch (MetaException e) {
        throw new RuntimeException(e);
      }

    }
    return rawStore;
  }

  @Override
  public void initialize(String inputName, Map<String, String> properties) {
    this.catalogProperties = ImmutableMap.copyOf(properties);
    this.name = inputName;

    // compatibility, set deprecated var
    if (properties.containsKey(CatalogProperties.URI)) {
      this.configuration.set(HiveConf.ConfVars.METASTORE_URIS.varname, properties.get(CatalogProperties.URI));
    }
    // compatibility, set deprecated var
    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      this.configuration.set(
          HiveConf.ConfVars.METASTORE_WAREHOUSE.varname,
          LocationUtil.stripTrailingSlash(properties.get(CatalogProperties.WAREHOUSE_LOCATION)));
    }

    this.listAllTables =
        Boolean.parseBoolean(properties.getOrDefault(LIST_ALL_TABLES, LIST_ALL_TABLES_DEFAULT));

    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO =
        fileIOImpl == null
            ? new HadoopFileIO(configuration)
            : CatalogUtil.loadFileIO(fileIOImpl, properties, configuration);
    catalog = MetaStoreUtils.getDefaultCatalog(configuration);
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Preconditions.checkArgument(
        isValidateNamespace(namespace), "Missing database in namespace: %s", namespace);
    String database = namespace.level(0);

    try {
      final RawStore store = getMS();
      List<String> tableNames = store.getAllTables(catalog, database);
      List<TableIdentifier> tableIdentifiers;

      if (tableNames.isEmpty()) {
        tableIdentifiers = Collections.emptyList();
      } else if (listAllTables) {
        tableIdentifiers =
            tableNames.stream()
                .map(t -> TableIdentifier.of(namespace, t))
                .collect(Collectors.toList());
      } else {
        List<Table> tableObjects = store.getTableObjectsByName(catalog, database, tableNames);
        tableIdentifiers =
            tableObjects.stream()
                .filter(
                    table ->
                        table.getParameters() != null
                            && ICEBERG_TABLE_TYPE_VALUE
                            .equalsIgnoreCase(
                                table
                                    .getParameters()
                                    .get(TABLE_TYPE_PROP)))
                .map(table -> TableIdentifier.of(namespace, table.getTableName()))
                .collect(Collectors.toList());
      }
      LOG.debug(
          "Listing of namespace: {} resulted in the following tables: {}",
          namespace,
          tableIdentifiers);
      return tableIdentifiers;

    } catch (UnknownDBException | MetaException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!isValidIdentifier(identifier)) {
      return false;
    }
    String database = identifier.namespace().level(0);
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata = null;
    if (purge) {
      try {
        lastMetadata = ops.current();
      } catch (NotFoundException e) {
        LOG.warn(
            "Failed to load table metadata for table: {}, continuing drop without purge",
            identifier,
            e);
      }
    }
    try {
      final RawStore store = getMS();
      store.dropTable( catalog, database, identifier.name());
      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
      }
      LOG.info("Dropped table: {}", identifier);
      return true;
    } catch (NoSuchTableException | NoSuchObjectException | InvalidInputException | MetaException |
             InvalidObjectException e) {
      LOG.info("Skipping drop, table does not exist: {}", identifier, e);
      return false;
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier originalTo) {
    if (!isValidIdentifier(from)) {
      throw new NoSuchTableException("Invalid identifier: %s", from);
    }
    TableIdentifier to = removeCatalogName(originalTo);
    Preconditions.checkArgument(isValidIdentifier(to), "Invalid identifier: %s", to);
    String toDatabase = to.namespace().level(0);
    String fromDatabase = from.namespace().level(0);
    String fromName = from.name();
    try {
      final RawStore store = getMS();
      Table table = store.getTable(catalog, fromDatabase, fromName);
      validateTableIsIceberg(table, fullTableName(name, from));
      table.setDbName(toDatabase);
      table.setTableName(to.name());
      store.alterTable(catalog, fromDatabase, fromName, table, null);
      LOG.info("Renamed table from {}, to {}", from, to);
    } catch (InvalidObjectException e) {
      throw new NoSuchTableException("Table does not exist: %s", from);
    }
    catch (MetaException e) {
      throw new RuntimeException("Failed to rename " + from + " to " + to, e);
    }
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> meta) {
    Preconditions.checkArgument(
        !namespace.isEmpty(), "Cannot create namespace with invalid name: %s", namespace);
    Preconditions.checkArgument(
        isValidateNamespace(namespace),
        "Cannot support multi part namespace in Hive Metastore: %s",
        namespace);
    Preconditions.checkArgument(
        meta.get(HMS_DB_OWNER_TYPE) == null || meta.get(HMS_DB_OWNER) != null,
        "Create namespace setting %s without setting %s is not allowed",
        HMS_DB_OWNER_TYPE,
        HMS_DB_OWNER);
    try {
        final IHMSHandler handler = getHandler();
        handler.create_database(convertToDatabase(namespace, meta));
        LOG.info("Created namespace: {}", namespace);
    } catch (AlreadyExistsException e) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException(
          e, "Namespace '%s' already exists!", namespace);

    } catch (TException e) {
      if (e.getCause() instanceof RuntimeException) {
        throw (RuntimeException) e.getCause();
      }
      throw new RuntimeException(
          "Failed to create namespace " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    if (!isValidateNamespace(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    if (!namespace.isEmpty()) {
      return ImmutableList.of();
    }
    try {
      final RawStore store = getMS();
      List<Namespace> namespaces = store.getAllDatabases(catalog).stream()
              .map(Namespace::of)
              .collect(Collectors.toList());
      LOG.debug("Listing namespace {} returned tables: {}", namespace, namespaces);
      return namespaces;
    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list all namespace: " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    if (!isValidateNamespace(namespace)) {
      return false;
    }
    try {
      String dbName = MetaStoreUtils.prependNotNullCatToDbName(catalog, namespace.level(0));
      IHMSHandler handler = getHandler();
      handler.drop_database(dbName, false, false);
      LOG.info("Dropped namespace: {}", namespace);
      return true;
    } catch (InvalidOperationException e) {
      throw new NamespaceNotEmptyException(
          e, "Namespace %s is not empty. One or more tables exist.", namespace);
    } catch (NoSuchObjectException e) {
      return false;
    } catch (TException e) {
      throw new RuntimeException("Failed to drop namespace " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    Preconditions.checkArgument(
        (properties.get(HMS_DB_OWNER_TYPE) == null) == (properties.get(HMS_DB_OWNER) == null),
        "Setting %s and %s has to be performed together or not at all",
        HMS_DB_OWNER_TYPE,
        HMS_DB_OWNER);
    Map<String, String> parameter = Maps.newHashMap();
    parameter.putAll(loadNamespaceMetadata(namespace));
    parameter.putAll(properties);
    Database database = convertToDatabase(namespace, parameter);
    alterHiveDataBase(namespace, database);
    LOG.debug("Successfully set properties {} for {}", properties.keySet(), namespace);
    // Always successful, otherwise exception is thrown
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    Preconditions.checkArgument(
        properties.contains(HMS_DB_OWNER_TYPE) == properties.contains(HMS_DB_OWNER),
        "Removing %s and %s has to be performed together or not at all",
        HMS_DB_OWNER_TYPE,
        HMS_DB_OWNER);
    Map<String, String> parameter = Maps.newHashMap();
    parameter.putAll(loadNamespaceMetadata(namespace));
    properties.forEach(key -> parameter.put(key, null));
    Database database = convertToDatabase(namespace, parameter);
    alterHiveDataBase(namespace, database);
    LOG.debug("Successfully removed properties {} from {}", properties, namespace);
    // Always successful, otherwise exception is thrown
    return true;
  }

  private void alterHiveDataBase(Namespace namespace, Database database) {
    try {
      final RawStore store = getMS();
      store.alterDatabase(catalog, namespace.level(0), database);
    } catch (NoSuchObjectException e) {
      throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);
    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list namespace under namespace: " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    if (!isValidateNamespace(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    try {
      final RawStore store = getMS();
      Database database = store.getDatabase(catalog, namespace.level(0));
      Map<String, String> metadata = convertToMetadata(database);
      LOG.debug("Loaded metadata for namespace {} found {}", namespace, metadata.keySet());
      return metadata;
    } catch (NoSuchObjectException e) {
      throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list namespace under namespace: " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return tableIdentifier.namespace().levels().length == 1;
  }

  private TableIdentifier removeCatalogName(TableIdentifier to) {
    if (isValidIdentifier(to)) {
      return to;
    }
    // check if the identifier includes the catalog name and remove it
    if (to.namespace().levels().length == 2 && name().equalsIgnoreCase(to.namespace().level(0))) {
      return TableIdentifier.of(Namespace.of(to.namespace().level(1)), to.name());
    }
    // return the original unmodified
    return to;
  }

  private boolean isValidateNamespace(Namespace namespace) {
    return namespace.levels().length == 1;
  }

  static void validateTableIsIceberg(Table table, String fullName) {
    String tableType = table.getParameters().get(TABLE_TYPE_PROP);
    NoSuchIcebergTableException.check(tableType != null && tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE),
        "Not an iceberg table: %s (type=%s)", fullName, tableType);
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    final RawStore store;
    try {
      store = getMS();
    } catch (MetaException e) {
      return null;
    }
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return new HMSTableOperations(configuration, store, fileIO, catalog, dbName, tableName);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    // This is a little edgy since we basically duplicate the HMS location generation logic.
    // Sadly I do not see a good way around this if we want to keep the order of events, like:
    // - Create meta files
    // - Create the metadata in HMS, and this way committing the changes

    // Create a new location based on the namespace / database if it is set on database level
    try {
      final RawStore store = getMS();
      Database databaseData = store.getDatabase(catalog, tableIdentifier.namespace().levels()[0]);
      if (databaseData.getLocationUri() != null) {
        // If the database location is set use it as a base.
        return String.format("%s/%s", databaseData.getLocationUri(), tableIdentifier.name());
      }

    } catch (TException e) {
      throw new RuntimeException(
          String.format("Metastore operation failed for %s", tableIdentifier), e);
    }

    // Otherwise, stick to the {WAREHOUSE_DIR}/{DB_NAME}.db/{TABLE_NAME} path
    String databaseLocation = databaseLocation(tableIdentifier.namespace().levels()[0]);
    return String.format("%s/%s", databaseLocation, tableIdentifier.name());
  }

  private String databaseLocation(String databaseName) {
    //MetastoreConf.ConfVars.WAREHOUSE
    String warehouseLocation = configuration.get(HiveConf.ConfVars.METASTORE_WAREHOUSE.varname);
    if (warehouseLocation == null) {
      warehouseLocation = configuration.get(MetastoreConf.ConfVars.WAREHOUSE.getVarname());
    }
    Preconditions.checkNotNull(
        warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    warehouseLocation = LocationUtil.stripTrailingSlash(warehouseLocation);
    return String.format("%s/%s.db", warehouseLocation, databaseName);
  }

  private Map<String, String> convertToMetadata(Database database) {
    Map<String, String> meta = Maps.newHashMap();
    meta.putAll(database.getParameters());
    meta.put("location", database.getLocationUri());
    if (database.getDescription() != null) {
      meta.put("comment", database.getDescription());
    }
    if (database.getOwnerName() != null) {
      meta.put(HMS_DB_OWNER, database.getOwnerName());
      if (database.getOwnerType() != null) {
        meta.put(HMS_DB_OWNER_TYPE, database.getOwnerType().name());
      }
    }

    return meta;
  }

  Database convertToDatabase(Namespace namespace, Map<String, String> meta) {
    if (!isValidateNamespace(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    Database database = new Database();
    Map<String, String> parameter = Maps.newHashMap();

    database.setName(namespace.level(0));
    database.setLocationUri(databaseLocation(namespace.level(0)));

    meta.forEach(
        (key, value) -> {
          if (key.equals("comment")) {
            database.setDescription(value);
          } else if (key.equals("location")) {
            database.setLocationUri(value);
          } else if (key.equals(HMS_DB_OWNER)) {
            database.setOwnerName(value);
          } else if (key.equals(HMS_DB_OWNER_TYPE) && value != null) {
            database.setOwnerType(PrincipalType.valueOf(value));
          } else {
            if (value != null) {
              parameter.put(key, value);
            }
          }
        });

    if (database.getOwnerName() == null) {
      database.setOwnerName(HiveHadoopUtil.currentUser());
      database.setOwnerType(PrincipalType.USER);
    }
    if (database.getCatalogName() == null) {
      database.setCatalogName(catalog);
    }

    database.setParameters(parameter);

    return database;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("uri", this.configuration == null ? "" : this.configuration.get(HiveConf.ConfVars.METASTORE_URIS.varname))
        .toString();
  }

  @Override
  public void setConf(Configuration conf) {
    //this.configuration = new Configuration(conf);
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }
}
