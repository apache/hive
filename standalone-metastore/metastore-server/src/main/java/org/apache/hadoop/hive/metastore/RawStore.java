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

package org.apache.hadoop.hive.metastore;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.AddPackageRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DropPackageRequest;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.GetPackageRequest;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetReplicationMetricsRequest;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.ListPackageRequest;
import org.apache.hadoop.hive.metastore.api.ListStoredProcedureRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.ReplicationMetricList;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
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
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StoredProcedure;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.properties.PropertyStore;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.apache.thrift.TException;

public interface RawStore extends Configurable {
  /***
   * Annotation to skip retries
   */
  @Target(value = ElementType.METHOD)
  @Retention(value = RetentionPolicy.RUNTIME)
  @interface CanNotRetry {
  }

  void shutdown();

  /**
   * Opens a new one or the one already created. Every call of this function must
   * have corresponding commit or rollback function call.
   *
   * @return an active transaction
   */
  boolean openTransaction();

  /**
   * Opens a new one or the one already created. Every call of this function must
   * have corresponding commit or rollback function call.
   *
   * @param isolationLevel The transaction isolation level. Only possible to set on the first call.
   * @return an active transaction
   */
  default boolean openTransaction(String isolationLevel) {
    throw new UnsupportedOperationException("Setting isolation level for this Store is not supported");
  }

  /**
   * if this is the commit of the first open call then an actual commit is
   * called.
   *
   * @return true or false
   */
  @CanNotRetry
  boolean commitTransaction();

  boolean isActiveTransaction();

  /**
   * Rolls back the current transaction if it is active
   */
  @CanNotRetry
  void rollbackTransaction();

  /**
   * Create a new catalog.
   * @param cat Catalog to create.
   * @throws MetaException if something goes wrong, usually in storing it to the database.
   */
  void createCatalog(Catalog cat) throws MetaException;

  /**
   * Alter an existing catalog.  Only description and location can be changed, and the change of
   * location is for internal use only.
   * @param catName name of the catalog to alter.
   * @param cat new version of the catalog.
   * @throws MetaException something went wrong, usually in the database.
   * @throws InvalidOperationException attempt to change something about the catalog that is not
   * changeable, like the name.
   */
  void alterCatalog(String catName, Catalog cat) throws MetaException, InvalidOperationException;

  /**
   * Get a catalog.
   * @param catalogName name of the catalog.
   * @return The catalog.
   * @throws NoSuchObjectException no catalog of this name exists.
   * @throws MetaException if something goes wrong, usually in reading it from the database.
   */
  Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException;

  /**
   * Get all the catalogs.
   * @return list of names of all catalogs in the system
   */
  List<String> getCatalogs();

  /**
   * Drop a catalog.  The catalog must be empty.
   * @param catalogName name of the catalog to drop.
   * @throws NoSuchObjectException no catalog of this name exists.
   * @throws MetaException could mean the catalog isn't empty, could mean general database error.
   */
  void dropCatalog(String catalogName) throws NoSuchObjectException, MetaException;

  /**
   * Create a database.
   * @param db database to create.
   * @throws InvalidObjectException not sure it actually ever throws this.
   * @throws MetaException if something goes wrong, usually in writing it to the database.
   */
  void createDatabase(Database db)
      throws InvalidObjectException, MetaException;

  /**
   * Get a database.
   * @param catalogName catalog the database is in.
   * @param name name of the database.
   * @return the database.
   * @throws NoSuchObjectException if no such database exists.
   */
  Database getDatabase(String catalogName, String name)
      throws NoSuchObjectException;

  /**
   * Drop a database.
   * @param catalogName catalog the database is in.
   * @param dbname name of the database.
   * @return true if the database was dropped, pretty much always returns this if it returns.
   * @throws NoSuchObjectException no database in this catalog of this name to drop
   * @throws MetaException something went wrong, usually with the database.
   */
  boolean dropDatabase(String catalogName, String dbname)
      throws NoSuchObjectException, MetaException;

  /**
   * Alter a database.
   * @param catalogName name of the catalog the database is in.
   * @param dbname name of the database to alter
   * @param db new version of the database.  This should be complete as it will fully replace the
   *          existing db object.
   * @return true if the change succeeds, could fail due to db constraint violations.
   * @throws NoSuchObjectException no database of this name exists to alter.
   * @throws MetaException something went wrong, usually with the database.
   */
  boolean alterDatabase(String catalogName, String dbname, Database db)
      throws NoSuchObjectException, MetaException;

  /**
   * Get all database in a catalog having names that match a pattern.
   * @param catalogName name of the catalog to search for databases in
   * @param pattern pattern names should match
   * @return list of matching database names.
   * @throws MetaException something went wrong, usually with the database.
   */
  List<String> getDatabases(String catalogName, String pattern) throws MetaException;

  /**
   * Get names of all the databases in a catalog.
   * @param catalogName name of the catalog to search for databases in
   * @return list of names of all databases in the catalog
   * @throws MetaException something went wrong, usually with the database.
   */
  List<String> getAllDatabases(String catalogName) throws MetaException;

  /**
   * Create a dataconnector.
   * @param dataConnector dataconnector to create.
   * @throws InvalidObjectException not sure it actually ever throws this.
   * @throws MetaException if something goes wrong, usually in writing it to the dataconnector.
   */
  void createDataConnector(DataConnector dataConnector)
      throws InvalidObjectException, MetaException;

  /**
   * Drop a dataconnector.
   * @param dcName name of the dataconnector.
   * @return true if the database was dropped, pretty much always returns this if it returns.
   * @throws NoSuchObjectException no database in this catalog of this name to drop
   * @throws MetaException something went wrong, usually with the database.
   */
  boolean dropDataConnector(String dcName)
      throws NoSuchObjectException, MetaException;

  /**
   * Alter a dataconnector.
   * @param dcName name of the dataconnector to alter
   * @param connector new version of the dataconnector.  This should be complete as it will fully replace the
   *          existing db object.
   * @return true if the change succeeds, false otherwise.
   * @throws NoSuchObjectException no dataconnector of this name exists to alter.
   * @throws MetaException something went wrong, usually with the backend HMSDB.
   */
  boolean alterDataConnector(String dcName, DataConnector connector)
      throws NoSuchObjectException, MetaException;

  /**
   * Get the dataconnector with a given name, if exists.
   * @param dcName pattern names should match
   * @return DataConnector object.
   * @throws NoSuchObjectException something went wrong, usually with the database.
   */
  DataConnector getDataConnector(String dcName) throws NoSuchObjectException;

  /**
   * Get names of all the databases in a catalog.
   * @return list of names of all dataconnectors
   * @throws MetaException something went wrong, usually with the database.
   */
  List<String> getAllDataConnectorNames() throws MetaException;

  boolean createType(Type type);

  Type getType(String typeName);

  boolean dropType(String typeName);

  void createTable(Table tbl) throws InvalidObjectException,
      MetaException;

  /**
   * Drop a table.
   * @param catalogName catalog the table is in
   * @param dbName database the table is in
   * @param tableName table name
   * @return true if the table was dropped
   * @throws MetaException something went wrong, usually in the RDBMS or storage
   * @throws NoSuchObjectException No table of this name
   * @throws InvalidObjectException Don't think this is ever actually thrown
   * @throws InvalidInputException Don't think this is ever actually thrown
   */
  boolean dropTable(String catalogName, String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  /**
   * Get a table object.
   * @param catalogName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @return table object, or null if no such table exists (wow it would be nice if we either
   * consistently returned null or consistently threw NoSuchObjectException).
   * @throws MetaException something went wrong in the RDBMS
   */
  Table getTable(String catalogName, String dbName, String tableName) throws MetaException;

  /**
   * Get a table object.
   * @param catalogName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @param writeIdList string format of valid writeId transaction list
   * @return table object, or null if no such table exists (wow it would be nice if we either
   * consistently returned null or consistently threw NoSuchObjectException).
   * @throws MetaException something went wrong in the RDBMS
   */
  Table getTable(String catalogName, String dbName, String tableName,
                 String writeIdList) throws MetaException;

  /**
   * Get a table object.
   * @param catalogName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @param writeIdList string format of valid writeId transaction list
   * @return table object, or null if no such table exists (wow it would be nice if we either
   * consistently returned null or consistently threw NoSuchObjectException).
   * @throws MetaException something went wrong in the RDBMS
   */
  Table getTable(String catalogName, String dbName, String tableName,
      String writeIdList, long tableId) throws MetaException;

  /**
   * Add a partition.
   * @param part partition to add
   * @return true if the partition was successfully added.
   * @throws InvalidObjectException the provided partition object is not valid.
   * @throws MetaException error writing to the RDBMS.
   */
  boolean addPartition(Partition part)
      throws InvalidObjectException, MetaException;

  /**
   * Add a list of partitions to a table.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param parts list of partitions to be added.
   * @return true if the operation succeeded.
   * @throws InvalidObjectException never throws this AFAICT
   * @throws MetaException the partitions don't belong to the indicated table or error writing to
   * the RDBMS.
   */
  boolean addPartitions(String catName, String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException;

  /**
   * @deprecated use {@link #addPartitions(String, String, String, List)} instead.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partitionSpec specification for the partition
   * @param ifNotExists whether it is in an error if the partition already exists.  If true, then
   *                   it is not an error if the partition exists, if false, it is.
   * @return whether the partition was created.
   * @throws InvalidObjectException The passed in partition spec or table specification is invalid.
   * @throws MetaException error writing to RDBMS.
   */
  @Deprecated
  boolean addPartitions(String catName, String dbName, String tblName,
                        PartitionSpecProxy partitionSpec, boolean ifNotExists)
      throws InvalidObjectException, MetaException;

  /**
   * Get a partition.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name.
   * @param part_vals partition values for this table.
   * @return the partition.
   * @throws MetaException error reading from RDBMS.
   * @throws NoSuchObjectException no partition matching this specification exists.
   */
  Partition getPartition(String catName, String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException;
  /**
   * Get a partition.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name.
   * @param part_vals partition values for this table.
   * @param writeIdList string format of valid writeId transaction list
   * @return the partition.
   * @throws MetaException error reading from RDBMS.
   * @throws NoSuchObjectException no partition matching this specification exists.
   */
  Partition getPartition(String catName, String dbName, String tableName,
                         List<String> part_vals,
                         String writeIdList)
      throws MetaException, NoSuchObjectException;

  /**
   * Check whether a partition exists.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name.
   * @param partKeys list of partition keys used to generate the partition name.
   * @param part_vals list of partition values.
   * @return true if the partition exists, false otherwise.
   * @throws MetaException failure reading RDBMS
   * @throws NoSuchObjectException this is never thrown.
   */
  boolean doesPartitionExist(String catName, String dbName, String tableName,
      List<FieldSchema> partKeys, List<String> part_vals)
      throws MetaException, NoSuchObjectException;

  /**
   * Drop a partition.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name.
   * @param part_vals list of partition values.
   * @return true if the partition was dropped.
   * @throws MetaException Error accessing the RDBMS.
   * @throws NoSuchObjectException no partition matching this description exists
   * @throws InvalidObjectException error dropping the statistics for the partition
   * @throws InvalidInputException error dropping the statistics for the partition
   */
  @Deprecated
  boolean dropPartition(String catName, String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException;

  /**
   * Drop a partition.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name.
   * @param partName partition name.
   * @return true if the partition was dropped.
   * @throws MetaException Error accessing the RDBMS.
   * @throws NoSuchObjectException no partition matching this description exists
   * @throws InvalidObjectException error dropping the statistics for the partition
   * @throws InvalidInputException error dropping the statistics for the partition
   */
  boolean dropPartition(String catName, String dbName, String tableName, String partName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  /**
   * Get some or all partitions for a table.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name
   * @param max maximum number of partitions, or -1 to get all partitions.
   * @return list of partitions
   * @throws MetaException error access the RDBMS.
   * @throws NoSuchObjectException no such table exists
   */
  @Deprecated
  default List<Partition> getPartitions(String catName, String dbName,
      String tableName, int max) throws MetaException, NoSuchObjectException {
    return getPartitions(catName, dbName, tableName, new GetPartitionsArgs
        .GetPartitionsArgsBuilder().max(max).build());
  }

  /**
   * Get some or all partitions for a table.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name
   * @param args additional arguments for getting partitions
   * @return list of partitions
   * @throws MetaException error access the RDBMS.
   * @throws NoSuchObjectException no such table exists
   */
  List<Partition> getPartitions(String catName, String dbName, String tableName,
      GetPartitionsArgs args) throws MetaException, NoSuchObjectException;

  /**
   * Get the location for every partition of a given table. If a partition location is a child of
   * baseLocationToNotShow then the partitionName is returned, but the only null location is
   * returned.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param baseLocationToNotShow Partition locations which are child of this path are omitted, and
   *     null value returned instead.
   * @param max The maximum number of partition locations returned, or -1 for all
   * @return The map of the partitionName, location pairs
   */
  Map<String, String> getPartitionLocations(String catName, String dbName, String tblName,
      String baseLocationToNotShow, int max);

  /**
   * Alter a table.
   * @param catName catalog the table is in.
   * @param dbname database the table is in.
   * @param name name of the table.
   * @param newTable New table object.  Which parts of the table can be altered are
   *                 implementation specific.
   * @return
   * @throws InvalidObjectException The new table object is invalid.
   * @throws MetaException something went wrong, usually in the RDBMS or storage.
   */
  Table alterTable(String catName, String dbname, String name, Table newTable,
      String queryValidWriteIds)
      throws InvalidObjectException, MetaException;

  /**
   * Update creation metadata for a materialized view.
   * @param catName catalog name.
   * @param dbname database name.
   * @param tablename table name.
   * @param cm new creation metadata
   * @throws MetaException error accessing the RDBMS.
   */
  void updateCreationMetadata(String catName, String dbname, String tablename, CreationMetadata cm)
      throws MetaException;

  /**
   * Get table names that match a pattern.
   * @param catName catalog to search in
   * @param dbName database to search in
   * @param pattern pattern to match
   * @return list of table names, if any
   * @throws MetaException failure in querying the RDBMS
   */
  List<String> getTables(String catName, String dbName, String pattern)
      throws MetaException;

  /**
   * Get table names that match a pattern.
   * @param catName catalog to search in
   * @param dbName database to search in
   * @param pattern pattern to match
   * @param tableType type of table to look for
   * @param limit Maximum number of tables to return (undeterministic set)
   * @return list of table names, if any
   * @throws MetaException failure in querying the RDBMS
   */
  List<String> getTables(String catName, String dbName, String pattern, TableType tableType, int limit)
      throws MetaException;

  /**
   * Retrieve all materialized views.
   * @return all materialized views in a catalog
   * @throws MetaException error querying the RDBMS
   */
  List<Table> getAllMaterializedViewObjectsForRewriting(String catName)
      throws MetaException;

  /**
   * Get list of materialized views in a database.
   * @param catName catalog name
   * @param dbName database name
   * @return names of all materialized views in the database
   * @throws MetaException error querying the RDBMS
   * @throws NoSuchObjectException no such database
   */
  List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, NoSuchObjectException;

  /**

   * @param catName catalog name to search in. Search must be confined to one catalog.
   * @param dbNames databases to search in.
   * @param tableNames names of tables to select.
   * @param tableTypes types of tables to look for.
   * @return list of matching table meta information.
   * @throws MetaException failure in querying the RDBMS.
   */
  List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames,
                               List<String> tableTypes) throws MetaException;

  /**
   * @param catName catalog name
   * @param dbname
   *        The name of the database from which to retrieve the tables
   * @param tableNames
   *        The names of the tables to retrieve.
   * @return A list of the tables retrievable from the database
   *          whose names are in the list tableNames.
   *         If there are duplicate names, only one instance of the table will be returned
   * @throws MetaException failure in querying the RDBMS.
   */
  List<Table> getTableObjectsByName(String catName, String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException;

  /**
   * @param catName catalog name
   * @param dbname
   *        The name of the database from which to retrieve the tables
   * @param tableNames
   *        The names of the tables to retrieve.
   * @param projectionSpec
   *        Projection Specification containing the columns that need to be returned.
   * @return A list of the tables retrievable from the database
   *          whose names are in the list tableNames.
   *         If there are duplicate names, only one instance of the table will be returned
   * @throws MetaException failure in querying the RDBMS.
   */
  List<Table> getTableObjectsByName(String catName, String dbname, List<String> tableNames,
                                    GetProjectionsSpec projectionSpec, String tablePattern) throws MetaException, UnknownDBException;

  /**
   * Get all tables in a database.
   * @param catName catalog name.
   * @param dbName database name.
   * @return list of table names
   * @throws MetaException failure in querying the RDBMS.
   */
  List<String> getAllTables(String catName, String dbName) throws MetaException;

  /**
   * Gets a list of tables based on a filter string and filter type.
   * @param catName catalog name
   * @param dbName
   *          The name of the database from which you will retrieve the table names
   * @param filter
   *          The filter string
   * @param max_tables
   *          The maximum number of tables returned
   * @return  A list of table names that match the desired filter
   * @throws MetaException
   * @throws UnknownDBException
   */
  List<String> listTableNamesByFilter(String catName, String dbName, String filter,
                                      short max_tables) throws MetaException, UnknownDBException;

  /**
   * Get a partial or complete list of names for partitions of a table.
   * @param catName catalog name.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param max_parts maximum number of partitions to retrieve, -1 for all.
   * @return list of partition names.
   * @throws MetaException there was an error accessing the RDBMS
   */
  List<String> listPartitionNames(String catName, String db_name,
      String tbl_name, short max_parts) throws MetaException;

  /**
   * Get a partial or complete list of names for partitions of a table.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param defaultPartName default partition name.
   * @param exprBytes expression for filtering resulting list, serialized from ExprNodeDesc.
   * @param order ordered the resulting list.
   * @param maxParts maximum number of partitions to retrieve, -1 for all.
   * @return list of partition names.
   * @throws MetaException there was an error accessing the RDBMS
   */
  List<String> listPartitionNames(String catName, String dbName, String tblName,
      String defaultPartName, byte[] exprBytes, String order,
      short maxParts) throws MetaException, NoSuchObjectException;

  /**
   * Get a list of partition values as one big struct.
   * @param catName catalog name.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param cols partition key columns
   * @param applyDistinct whether to apply distinct to the list
   * @param filter filter to apply to the partition names
   * @param ascending whether to put in ascending order
   * @param order whether to order
   * @param maxParts maximum number of parts to return, or -1 for all
   * @return struct with all of the partition value information
   * @throws MetaException error access the RDBMS
   */
  PartitionValuesResponse listPartitionValues(String catName, String db_name, String tbl_name,
                                              List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending,
                                              List<FieldSchema> order, long maxParts) throws MetaException;

  /**
   * Alter a partition.
   * @param catName catalog name.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param part_vals partition values that describe the partition.
   * @param new_part new partition object.  This should be a complete copy of the old with
   *                 changes values, not just the parts to update.
   * @return
   * @throws InvalidObjectException No such partition.
   * @throws MetaException error accessing the RDBMS.
   */
  Partition alterPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
      Partition new_part, String queryValidWriteIds)
          throws InvalidObjectException, MetaException;

  /**
   * Alter a set of partitions.
   * @param catName catalog name.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param part_vals_list list of list of partition values.  Each outer list describes one
   *                       partition (with its list of partition values).
   * @param new_parts list of new partitions.  The order must match the old partitions described in
   *                  part_vals_list.  Each of these should be a complete copy of the new
   *                  partition, not just the pieces to update.
   * @param writeId write id of the transaction for the table
   * @param queryValidWriteIds valid write id list of the transaction on the current table
   * @return
   * @throws InvalidObjectException One of the indicated partitions does not exist.
   * @throws MetaException error accessing the RDBMS.
   */
  List<Partition> alterPartitions(String catName, String db_name, String tbl_name,
      List<List<String>> part_vals_list, List<Partition> new_parts, long writeId,
      String queryValidWriteIds)
      throws InvalidObjectException, MetaException;

  /**
   * Get partitions with a filter.  This is a portion of the SQL where clause.
   * @param catName catalog name
   * @param dbName database name
   * @param tblName table name
   * @param filter SQL where clause filter
   * @param maxParts maximum number of partitions to return, or -1 for all.
   * @return list of partition objects matching the criteria
   * @throws MetaException Error accessing the RDBMS or processing the filter.
   * @throws NoSuchObjectException no such table.
   */
  @Deprecated
  default List<Partition> getPartitionsByFilter(
     String catName, String dbName, String tblName, String filter, short maxParts)
     throws MetaException, NoSuchObjectException {
    return getPartitionsByFilter(catName, dbName, tblName, new GetPartitionsArgs
        .GetPartitionsArgsBuilder()
        .filter(filter).max(maxParts)
        .build());
  }

  /**
   * Get partitions with a filter.  This is a portion of the SQL where clause.
   * @param catName catalog name
   * @param dbName database name
   * @param tblName table name
   * @param args additional arguments for getting partitions
   * @return list of partition objects matching the criteria
   * @throws MetaException Error accessing the RDBMS or processing the filter.
   * @throws NoSuchObjectException no such table.
   */
  List<Partition> getPartitionsByFilter(
      String catName, String dbName, String tblName, GetPartitionsArgs args)
      throws MetaException, NoSuchObjectException;

  /**
   * Generic Partition request API, providing different kinds of filtering and controlling output.
   *
   * @param table          table for which whose partitions are requested
   *                       * @param table table for which partitions are requested
   * @param projectionSpec the projection spec from the <code>GetPartitionsRequest</code>
   *                       This projection spec includes a fieldList which represents the fields which must be returned.
   *                       Any other field which is not in the fieldList may be unset in the returned
   *                       partitions (it is up to the implementation to decide whether it chooses to
   *                       include or exclude such fields). E.g. setting the field list to <em>sd.location</em>,
   *                       <em>serdeInfo.name</em>, <em>sd.cols.name</em>, <em>sd.cols.type</em> will
   *                       return partitions which will have location field set in the storage descriptor.
   *                       Also the serdeInf in the returned storage descriptor will only have name field
   *                       set. This applies to multi-valued fields as well like sd.cols, so in the
   *                       example above only name and type fields will be set for <em>sd.cols</em>.
   *                       If the <em>fieldList</em> is empty or not present, all the fields will be set.
   *                       Additionally, it also includes a includeParamKeyPattern and excludeParamKeyPattern
   *                       which is a SQL-92 compliant regex pattern to include or exclude parameters. The paramKeyPattern
   *                       supports _ or % wildcards which represent one character and 0 or more characters respectively
   * @param filterSpec     The filter spec from <code>GetPartitionsRequest</code> which includes the filter mode
   *                       and the list of filter strings. The filter mode could be BY_NAMES, BY_VALUES or BY_EXPR
   *                       to filter by partition names, partition values or expression. The filter strings are provided
   *                       in the list of filters within the filterSpec. When more than one filters are provided in the list
   *                       they are logically AND together
   * @return List of matching partitions which which may be partially filled according to fieldList.
   * @throws MetaException         in case of errors
   * @throws NoSuchObjectException when table isn't found
   */
  List<Partition> getPartitionSpecsByFilterAndProjection(Table table,
                                                         GetProjectionsSpec projectionSpec, GetPartitionsFilterSpec filterSpec)
      throws MetaException, NoSuchObjectException;
  /**
   * Get partitions using an already parsed expression.
   * @param catName catalog name.
   * @param dbName database name
   * @param tblName table name
   * @param expr an already parsed Hive expression
   * @param defaultPartitionName default name of a partition
   * @param maxParts maximum number of partitions to return, or -1 for all
   * @param result list to place resulting partitions in
   * @return true if the result contains unknown partitions.
   * @throws TException error executing the expression
   */
  @Deprecated
  default boolean getPartitionsByExpr(String catName, String dbName, String tblName,
       byte[] expr, String defaultPartitionName, short maxParts, List<Partition> result)
       throws TException {
    return getPartitionsByExpr(catName, dbName, tblName, result, new GetPartitionsArgs
        .GetPartitionsArgsBuilder()
        .expr(expr).defaultPartName(defaultPartitionName).max(maxParts)
        .build());
  }

  /**
   * Get partitions using an already parsed expression.
   * @param catName catalog name.
   * @param dbName database name
   * @param tblName table name
   * @param args additional arguments for getting partitions
   * @return true if the result contains unknown partitions.
   * @throws TException error executing the expression
   */
  boolean getPartitionsByExpr(String catName, String dbName, String tblName,
      List<Partition> result, GetPartitionsArgs args)
      throws TException;

  /**
   * Get the number of partitions that match a provided SQL filter.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param filter filter from Hive's SQL where clause
   * @return number of matching partitions.
   * @throws MetaException error accessing the RDBMS or executing the filter
   * @throws NoSuchObjectException no such table
   */
  int getNumPartitionsByFilter(String catName, String dbName, String tblName, String filter)
    throws MetaException, NoSuchObjectException;

  /**
   * Get the number of partitions that match an already parsed expression.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param expr an already parsed Hive expression
   * @return number of matching partitions.
   * @throws MetaException error accessing the RDBMS or working with the expression.
   * @throws NoSuchObjectException no such table.
   */
  int getNumPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr)
      throws MetaException, NoSuchObjectException;

  /**
   * Get the number of partitions that match a given partial specification.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partVals A partial list of values for partitions in order of the table's partition keys.
   *                  Entries can be empty if you need to specify latter partitions.
   * @return number of matching partitions.
   * @throws MetaException error accessing the RDBMS or working with the specification.
   * @throws NoSuchObjectException no such table.
   */
  int getNumPartitionsByPs(String catName, String dbName, String tblName, List<String> partVals)
      throws MetaException, NoSuchObjectException;

  /**
   * Get partitions by name.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partNames list of partition names.  These are names not values, so they will include
   *                  both the key and the value.
   * @return list of matching partitions
   * @throws MetaException error accessing the RDBMS.
   * @throws NoSuchObjectException No such table.
   */
  default List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    return getPartitionsByNames(catName, dbName, tblName, new GetPartitionsArgs
        .GetPartitionsArgsBuilder().partNames(partNames).build());
  }

  /**
   * Get partitions by name.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param args additional arguments for getting partitions
   * @return list of matching partitions
   * @throws MetaException error accessing the RDBMS.
   * @throws NoSuchObjectException No such table.
   */
  List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
      GetPartitionsArgs args) throws MetaException, NoSuchObjectException;

  Table markPartitionForEvent(String catName, String dbName, String tblName, Map<String,String> partVals, PartitionEventType evtType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException;

  boolean isPartitionMarkedForEvent(String catName, String dbName, String tblName, Map<String, String> partName, PartitionEventType evtType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException;

  boolean addRole(String rowName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  boolean removeRole(String roleName) throws MetaException, NoSuchObjectException;

  boolean grantRole(Role role, String userName, PrincipalType principalType,
      String grantor, PrincipalType grantorType, boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  boolean revokeRole(Role role, String userName, PrincipalType principalType,
      boolean grantOption) throws MetaException, NoSuchObjectException;

  PrincipalPrivilegeSet getUserPrivilegeSet(String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException;

  /**
   * Get privileges for a database for a user.
   * @param catName catalog name
   * @param dbName database name
   * @param userName user name
   * @param groupNames list of groups the user is in
   * @return privileges for that user on indicated database
   * @throws InvalidObjectException no such database
   * @throws MetaException error accessing the RDBMS
   */
  PrincipalPrivilegeSet getDBPrivilegeSet (String catName, String dbName, String userName,
      List<String> groupNames)  throws InvalidObjectException, MetaException;

  /**
   * Get privileges for a connector for a user.
   * @param catName catalog name
   * @param connectorName connector name
   * @param userName user name
   * @param groupNames list of groups the user is in
   * @return privileges for that user on indicated connector
   * @throws InvalidObjectException no such database
   * @throws MetaException error accessing the RDBMS
   */
  PrincipalPrivilegeSet getConnectorPrivilegeSet (String catName, String connectorName, String userName,
      List<String> groupNames)  throws InvalidObjectException, MetaException;

  /**
   * Get privileges for a table for a user.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param userName user name
   * @param groupNames list of groups the user is in
   * @return privileges for that user on indicated table
   * @throws InvalidObjectException no such table
   * @throws MetaException error accessing the RDBMS
   */
  PrincipalPrivilegeSet getTablePrivilegeSet (String catName, String dbName, String tableName,
      String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  /**
   * Get privileges for a partition for a user.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param partition partition name
   * @param userName user name
   * @param groupNames list of groups the user is in
   * @return privileges for that user on indicated partition
   * @throws InvalidObjectException no such partition
   * @throws MetaException error accessing the RDBMS
   */
  PrincipalPrivilegeSet getPartitionPrivilegeSet (String catName, String dbName, String tableName,
      String partition, String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  /**
   * Get privileges for a column in a table or partition for a user.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param partitionName partition name, or null for table level column permissions
   * @param columnName column name
   * @param userName user name
   * @param groupNames list of groups the user is in
   * @return privileges for that user on indicated column in the table or partition
   * @throws InvalidObjectException no such table, partition, or column
   * @throws MetaException error accessing the RDBMS
   */
  PrincipalPrivilegeSet getColumnPrivilegeSet (String catName, String dbName, String tableName, String partitionName,
      String columnName, String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType);

  /**
   * For a given principal name and type, list the DB Grants
   * @param principalName principal name
   * @param principalType type
   * @param catName catalog name
   * @param dbName database name
   * @return list of privileges for that principal on the specified database.
   */
  List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String catName, String dbName);

  /**
   * For a given principal name and type, list the DC Grants
   * @param principalName principal name
   * @param principalType type
   * @param dcName data connector name
   * @return list of privileges for that principal on the specified data connector.
   */
  List<HiveObjectPrivilege> listPrincipalDCGrants(String principalName,
                                                  PrincipalType principalType, String dcName);

  /**
   * For a given principal name and type, list the Table Grants
   * @param principalName principal name
   * @param principalType type
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @return list of privileges for that principal on the specified database.
   */
  List<HiveObjectPrivilege> listAllTableGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName);

  /**
   * For a given principal name and type, list the Table Grants
   * @param principalName principal name
   * @param principalType type
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param partName partition name (not value)
   * @return list of privileges for that principal on the specified database.
   */
  List<HiveObjectPrivilege> listPrincipalPartitionGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, List<String> partValues, String partName);

  /**
   * For a given principal name and type, list the Table Grants
   * @param principalName principal name
   * @param principalType type
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param columnName column name
   * @return list of privileges for that principal on the specified database.
   */
  List<HiveObjectPrivilege> listPrincipalTableColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, String columnName);

  /**
   * For a given principal name and type, list the Table Grants
   * @param principalName principal name
   * @param principalType type
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param partName partition name (not value)
   * @param columnName column name
   * @return list of privileges for that principal on the specified database.
   */
  List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(
      String principalName, PrincipalType principalType, String catName, String dbName,
      String tableName, List<String> partValues, String partName, String columnName);

  boolean grantPrivileges (PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
  throws InvalidObjectException, MetaException, NoSuchObjectException;

  boolean refreshPrivileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
  throws InvalidObjectException, MetaException, NoSuchObjectException;

  org.apache.hadoop.hive.metastore.api.Role getRole(
      String roleName) throws NoSuchObjectException;

  List<String> listRoleNames();

  List<Role> listRoles(String principalName,
      PrincipalType principalType);

  List<RolePrincipalGrant> listRolesWithGrants(String principalName,
                                                      PrincipalType principalType);


  /**
   * Get the role to principal grant mapping for given role
   * @param roleName
   * @return
   */
  List<RolePrincipalGrant> listRoleMembers(String roleName);


  /**
   * Fetch a partition along with privilege information for a particular user.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partVals partition values
   * @param user_name user to get privilege information for.
   * @param group_names groups to get privilege information for.
   * @return a partition
   * @throws MetaException error accessing the RDBMS.
   * @throws NoSuchObjectException no such partition exists
   * @throws InvalidObjectException error fetching privilege information
   */
  Partition getPartitionWithAuth(String catName, String dbName, String tblName,
      List<String> partVals, String user_name, List<String> group_names)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  /**
   * Fetch some or all partitions for a table, along with privilege information for a particular
   * user.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param maxParts maximum number of partitions to fetch, -1 for all partitions.
   * @param userName user to get privilege information for.
   * @param groupNames groups to get privilege information for.
   * @return list of partitions.
   * @throws MetaException error access the RDBMS.
   * @throws NoSuchObjectException no such table exists
   * @throws InvalidObjectException error fetching privilege information.
   */
  @Deprecated
  default List<Partition> getPartitionsWithAuth(String catName, String dbName,
       String tblName, short maxParts, String userName, List<String> groupNames)
       throws MetaException, NoSuchObjectException, InvalidObjectException {
    return listPartitionsPsWithAuth(catName, dbName, tblName,
        new GetPartitionsArgs.GetPartitionsArgsBuilder()
            .max(maxParts).userName(userName).groupNames(groupNames)
            .build());
  }

  /**
   * Lists partition names that match a given partial specification
   * @param catName catalog name.
   * @param db_name
   *          The name of the database which has the partitions
   * @param tbl_name
   *          The name of the table which has the partitions
   * @param part_vals
   *          A partial list of values for partitions in order of the table's partition keys.
   *          Entries can be empty if you only want to specify latter partitions.
   * @param max_parts
   *          The maximum number of partitions to return
   * @return A list of partition names that match the partial spec.
   * @throws MetaException error accessing RDBMS
   * @throws NoSuchObjectException No such table exists
   */
  List<String> listPartitionNamesPs(String catName, String db_name, String tbl_name,
      List<String> part_vals, short max_parts)
      throws MetaException, NoSuchObjectException;

  /**
   * Lists partitions that match a given partial specification and sets their auth privileges.
   *   If userName and groupNames null, then no auth privileges are set.
   * @param catName catalog name.
   * @param db_name
   *          The name of the database which has the partitions
   * @param tbl_name
   *          The name of the table which has the partitions
   * @param part_vals
   *          A partial list of values for partitions in order of the table's partition keys
   *          Entries can be empty if you need to specify latter partitions.
   * @param max_parts
   *          The maximum number of partitions to return
   * @param userName
   *          The user name for the partition for authentication privileges
   * @param groupNames
   *          The groupNames for the partition for authentication privileges
   * @return A list of partitions that match the partial spec.
   * @throws MetaException error access RDBMS
   * @throws NoSuchObjectException No such table exists
   * @throws InvalidObjectException error access privilege information
   */
  @Deprecated
  default List<Partition> listPartitionsPsWithAuth(String catName, String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    return listPartitionsPsWithAuth(catName, db_name, tbl_name, new GetPartitionsArgs
        .GetPartitionsArgsBuilder()
        .part_vals(part_vals).max(max_parts).userName(userName).groupNames(groupNames)
        .build());
  }

  /**
   * Lists partitions that match a given partial specification and sets their auth privileges.
   *   If userName and groupNames null, then no auth privileges are set.
   * @param catName catalog name.
   * @param db_name
   *          The name of the database which has the partitions
   * @param tbl_name
   *          The name of the table which has the partitions
   * @param args additional arguments for getting partitions
   * @return A list of partitions that match the partial spec.
   * @throws MetaException error access RDBMS
   * @throws NoSuchObjectException No such table exists
   * @throws InvalidObjectException error access privilege information
   */
  List<Partition> listPartitionsPsWithAuth(String catName, String db_name, String tbl_name,
      GetPartitionsArgs args) throws MetaException, InvalidObjectException, NoSuchObjectException;

  /** Persists the given column statistics object to the metastore
   * @param colStats object to persist
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException No such table.
   * @throws MetaException error accessing the RDBMS.
   * @throws InvalidObjectException the stats object is invalid
   * @throws InvalidInputException unable to record the stats for the table
   */
  Map<String, String> updateTableColumnStatistics(ColumnStatistics colStats, String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /** Persists the given column statistics object to the metastore
   * @param statsObj object to persist
   * @param partVals partition values to persist the stats for
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException No such table.
   * @throws MetaException error accessing the RDBMS.
   * @throws InvalidObjectException the stats object is invalid
   * @throws InvalidInputException unable to record the stats for the table
   */
  Map<String, String> updatePartitionColumnStatistics(ColumnStatistics statsObj,
      List<String> partVals, String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  Map<String, String> updatePartitionColumnStatistics(Table table, MTable mTable,
      ColumnStatistics statsObj, List<String> partVals,
      String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /**
   * Returns the relevant column statistics for a given column in a given table in a given database
   * if such statistics exist.
   * @param catName catalog name.
   * @param dbName name of the database, defaults to current database
   * @param tableName name of the table
   * @param colName names of the columns for which statistics is requested
   * @return Relevant column statistics for the column for the given table
   * @throws NoSuchObjectException No such table
   * @throws MetaException error accessing the RDBMS
   *
   */
  List<ColumnStatistics> getTableColumnStatistics(String catName, String dbName, String tableName,
    List<String> colName) throws MetaException, NoSuchObjectException;

  /**
   * Returns the relevant column statistics for a given column in a given table in a given database
   * if such statistics exist.
   * @param catName catalog name.
   * @param dbName name of the database, defaults to current database
   * @param tableName name of the table
   * @param colName names of the columns for which statistics is requested
   * @param engine engine requesting the statistics
   * @return Relevant column statistics for the column for the given table
   * @throws NoSuchObjectException No such table
   * @throws MetaException error accessing the RDBMS
   *
   */
  ColumnStatistics getTableColumnStatistics(String catName, String dbName, String tableName,
    List<String> colName, String engine) throws MetaException, NoSuchObjectException;

  /**
   * Returns the relevant column statistics for a given column in a given table in a given database
   * if such statistics exist.
   * @param catName catalog name.
   * @param dbName name of the database, defaults to current database
   * @param tableName name of the table
   * @param colName names of the columns for which statistics is requested
   * @param engine engine requesting the statistics
   * @param writeIdList string format of valid writeId transaction list
   * @return Relevant column statistics for the column for the given table
   * @throws NoSuchObjectException No such table
   * @throws MetaException error accessing the RDBMS
   *
   */
  ColumnStatistics getTableColumnStatistics(
    String catName, String dbName, String tableName,
    List<String> colName, String engine, String writeIdList)
      throws MetaException, NoSuchObjectException;

  /**
   * Get statistics for a partition for a set of columns.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partNames list of partition names.  These are names so must be key1=val1[/key2=val2...]
   * @param colNames list of columns to get stats for
   * @return list of statistics objects
   * @throws MetaException error accessing the RDBMS
   * @throws NoSuchObjectException no such partition.
   */
  List<List<ColumnStatistics>> getPartitionColumnStatistics(
      String catName, String dbName, String tblName, List<String> partNames, List<String> colNames)
      throws MetaException, NoSuchObjectException;

  /**
   * Get statistics for a partition for a set of columns.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partNames list of partition names.  These are names so must be key1=val1[/key2=val2...]
   * @param colNames list of columns to get stats for
   * @param engine engine requesting the statistics
   * @return list of statistics objects
   * @throws MetaException error accessing the RDBMS
   * @throws NoSuchObjectException no such partition.
   */
  List<ColumnStatistics> getPartitionColumnStatistics(
     String catName, String dbName, String tblName, List<String> partNames, List<String> colNames,
     String engine) throws MetaException, NoSuchObjectException;

  /**
   * Get statistics for a partition for a set of columns.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partNames list of partition names.  These are names so must be key1=val1[/key2=val2...]
   * @param colNames list of columns to get stats for
   * @param engine engine requesting the statistics
   * @param writeIdList string format of valid writeId transaction list
   * @return list of statistics objects
   * @throws MetaException error accessing the RDBMS
   * @throws NoSuchObjectException no such partition.
   */
  List<ColumnStatistics> getPartitionColumnStatistics(
      String catName, String dbName, String tblName,
      List<String> partNames, List<String> colNames,
      String engine, String writeIdList)
      throws MetaException, NoSuchObjectException;

  /**
   * Deletes column statistics if present associated with a given db, table, partition and col. If
   * null is passed instead of a colName, stats when present for all columns associated
   * with a given db, table and partition are deleted.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name.
   * @param partName partition name.
   * @param partVals partition values.
   * @param colName column name.
   * @param engine engine for which we want to delete statistics
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException no such partition
   * @throws MetaException error access the RDBMS
   * @throws InvalidObjectException error dropping the stats
   * @throws InvalidInputException bad input, such as null table or database name.
   */
  boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName,
      String partName, List<String> partVals, String colName, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /**
   * Delete statistics for a single column or all columns in a table.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param colName column name.  Null to delete stats for all columns in the table.
   * @param engine engine for which we want to delete statistics
   * @return true if the statistics were deleted.
   * @throws NoSuchObjectException no such table or column.
   * @throws MetaException error access the RDBMS.
   * @throws InvalidObjectException error dropping the stats
   * @throws InvalidInputException bad inputs, such as null table name.
   */
  boolean deleteTableColumnStatistics(String catName, String dbName, String tableName,
    String colName, String engine)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  long cleanupEvents();

  boolean addToken(String tokenIdentifier, String delegationToken);

  boolean removeToken(String tokenIdentifier);

  String getToken(String tokenIdentifier);

  List<String> getAllTokenIdentifiers();

  int addMasterKey(String key) throws MetaException;

  void updateMasterKey(Integer seqNo, String key)
     throws NoSuchObjectException, MetaException;

  boolean removeMasterKey(Integer keySeq);

  String[] getMasterKeys();

  void verifySchema() throws MetaException;

  String getMetaStoreSchemaVersion() throws  MetaException;

  abstract void setMetaStoreSchemaVersion(String version, String comment) throws MetaException;

  /**
   * Drop a list of partitions.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name
   * @param partNames list of partition names.
   * @throws MetaException error access RDBMS or storage.
   * @throws NoSuchObjectException One or more of the partitions does not exist.
   */
  void dropPartitions(String catName, String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException;

  /**
   * List all DB grants for a given principal.
   * @param principalName principal name
   * @param principalType type
   * @return all DB grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalDBGrantsAll(
      String principalName, PrincipalType principalType);

  /**
   * List all DC grants for a given principal.
   * @param principalName principal name
   * @param principalType type
   * @return all DC grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalDCGrantsAll(
          String principalName, PrincipalType principalType);

  /**
   * List all Table grants for a given principal
   * @param principalName principal name
   * @param principalType type
   * @return all Table grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalTableGrantsAll(
      String principalName, PrincipalType principalType);

  /**
   * List all Partition grants for a given principal
   * @param principalName principal name
   * @param principalType type
   * @return all Partition grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(
      String principalName, PrincipalType principalType);

  /**
   * List all Table column grants for a given principal
   * @param principalName principal name
   * @param principalType type
   * @return all Table column grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(
      String principalName, PrincipalType principalType);

  /**
   * List all Partition column grants for a given principal
   * @param principalName principal name
   * @param principalType type
   * @return all Partition column grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(
      String principalName, PrincipalType principalType);

  List<HiveObjectPrivilege> listGlobalGrantsAll();

  /**
   * Find all the privileges for a given database.
   * @param catName catalog name
   * @param dbName database name
   * @return list of all privileges.
   */
  List<HiveObjectPrivilege> listDBGrantsAll(String catName, String dbName);

  /**
   * Find all the privileges for a given data connector.
   * @param dcName data connector name
   * @return list of all privileges.
   */
  List<HiveObjectPrivilege> listDCGrantsAll(String dcName);

  /**
   * Find all of the privileges for a given column in a given partition.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param partitionName partition name (not value)
   * @param columnName column name
   * @return all privileges on this column in this partition
   */
  List<HiveObjectPrivilege> listPartitionColumnGrantsAll(
      String catName, String dbName, String tableName, String partitionName, String columnName);

  /**
   * Find all of the privileges for a given table
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @return all privileges on this table
   */
  List<HiveObjectPrivilege> listTableGrantsAll(String catName, String dbName, String tableName);

  /**
   * Find all of the privileges for a given partition.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param partitionName partition name (not value)
   * @return all privileges on this partition
   */
  List<HiveObjectPrivilege> listPartitionGrantsAll(
      String catName, String dbName, String tableName, String partitionName);

  /**
   * Find all of the privileges for a given column in a given table.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param columnName column name
   * @return all privileges on this column in this table
   */
  List<HiveObjectPrivilege> listTableColumnGrantsAll(
      String catName, String dbName, String tableName, String columnName);

  /**
   * Register a user-defined function based on the function specification passed in.
   * @param func function to create
   * @throws InvalidObjectException incorrectly specified function
   * @throws MetaException error accessing the RDBMS
   */
  void createFunction(Function func)
      throws InvalidObjectException, MetaException;

  /**
   * Alter function based on new function specs.
   * @param dbName database name
   * @param funcName function name
   * @param newFunction new function specification
   * @throws InvalidObjectException no such function, or incorrectly specified new function
   * @throws MetaException incorrectly specified function
   */
  void alterFunction(String catName, String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException;

  /**
   * Drop a function definition.
   * @param dbName database name
   * @param funcName function name
   * @throws MetaException incorrectly specified function
   * @throws NoSuchObjectException no such function
   * @throws InvalidObjectException not sure when this is thrown
   * @throws InvalidInputException not sure when this is thrown
   */
  void dropFunction(String catName, String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  /**
   * Retrieve function by name.
   * @param dbName database name
   * @param funcName function name
   * @return the function
   * @throws MetaException incorrectly specified function
   */
  Function getFunction(String catName, String dbName, String funcName) throws MetaException;

  /**
   * Retrieve all functions.
   * @return all functions in a catalog
   * @throws MetaException incorrectly specified function
   */
  List<Function> getAllFunctions(String catName) throws MetaException;

  /**
   * Retrieve list of function names based on name pattern.
   * @param dbName database name
   * @param pattern pattern to match
   * @return functions that match the pattern
   * @throws MetaException incorrectly specified function
   */
  List<String> getFunctions(String catName, String dbName, String pattern) throws MetaException;

  /**
   * Get aggregated stats for a table or partition(s).
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partNames list of partition names.  These are the names of the partitions, not
   *                  values.
   * @param colNames list of column names
   * @param engine engine requesting the statistics
   * @return aggregated stats
   * @throws MetaException error accessing RDBMS
   * @throws NoSuchObjectException no such table or partition
   */
  AggrStats get_aggr_stats_for(String catName, String dbName, String tblName,
    List<String> partNames, List<String> colNames, String engine) throws MetaException, NoSuchObjectException;

  /**
   * Get aggregated stats for a table or partition(s).
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partNames list of partition names.  These are the names of the partitions, not
   *                  values.
   * @param colNames list of column names
   * @param engine engine requesting the statistics
   * @param writeIdList string format of valid writeId transaction list
   * @return aggregated stats
   * @throws MetaException error accessing RDBMS
   * @throws NoSuchObjectException no such table or partition
   */
  AggrStats get_aggr_stats_for(String catName, String dbName, String tblName,
    List<String> partNames, List<String> colNames,
    String engine, String writeIdList)
      throws MetaException, NoSuchObjectException;

  /**
   * Get column stats for all partitions of all tables in the database
   * @param catName catalog name
   * @param dbName database name
   * @return List of column stats objects for all partitions of all tables in the database
   * @throws MetaException error accessing RDBMS
   * @throws NoSuchObjectException no such database
   */
  List<ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String catName, String dbName)
      throws MetaException, NoSuchObjectException;

    /**
   * Get the next notification event.
   * @param rqst Request containing information on the last processed notification.
   * @return list of notifications, sorted by eventId
   */
  NotificationEventResponse getNextNotification(NotificationEventRequest rqst);


  /**
   * Add a notification entry.  This should only be called from inside the metastore
   * @param event the notification to add
   * @throws MetaException error accessing RDBMS
   */
  void addNotificationEvent(NotificationEvent event) throws MetaException;

  /**
   * Remove older notification events.
   *
   * @param olderThan Remove any events older or equal to a given number of seconds
   */
  void cleanNotificationEvents(int olderThan);

  /**
   * Get the last issued notification event id.  This is intended for use by the export command
   * so that users can determine the state of the system at the point of the export,
   * and determine which notification events happened before or after the export.
   * @return
   */
  CurrentNotificationEventId getCurrentNotificationEventId();

  /**
   * Get the number of events corresponding to given database with fromEventId.
   * This is intended for use by the repl commands to track the progress of incremental dump.
   * @return
   */
  NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst);

  /*
   * Flush any catalog objects held by the metastore implementation.  Note that this does not
   * flush statistics objects.  This should be called at the beginning of each query.
   */
  void flushCache();

  /**
   * @param fileIds List of file IDs from the filesystem.
   * @return File metadata buffers from file metadata cache. The array is fileIds-sized, and
   *         the entries (or nulls, if metadata is not in cache) correspond to fileIds in the list
   */
  ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException;

  /**
   * @param fileIds List of file IDs from the filesystem.
   * @param metadata Metadata buffers corresponding to fileIds in the list.
   * @param type The type; determines the class that can do additional processing for metadata.
   */
  void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata,
      FileMetadataExprType type) throws MetaException;

  /**
   * @return Whether file metadata cache is supported by this implementation.
   */
  boolean isFileMetadataSupported();

  /**
   * Gets file metadata from cache after applying a format-specific expression that can
   * produce additional information based on file metadata and also filter the file list.
   * @param fileIds List of file IDs from the filesystem.
   * @param expr Format-specific serialized expression applicable to the files' metadatas.
   * @param type Expression type; used to determine the class that handles the metadata.
   * @param metadatas Output parameter; fileIds-sized array to receive the metadatas
   *                  for corresponding files, if any.
   * @param exprResults Output parameter; fileIds-sized array to receive the format-specific
   *                    expression results for the corresponding files.
   * @param eliminated Output parameter; fileIds-sized array to receive the indication of whether
   *                   the corresponding files are entirely eliminated by the expression.
   */
  void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr,
      ByteBuffer[] metadatas, ByteBuffer[] exprResults, boolean[] eliminated)
          throws MetaException;

  /** Gets file metadata handler for the corresponding type. */
  FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type);

  /**
   * Gets total number of tables.
   */
  @InterfaceStability.Evolving
  int getTableCount() throws MetaException;

  /**
   * Gets total number of partitions.
   */
  @InterfaceStability.Evolving
  int getPartitionCount() throws MetaException;

  /**
   * Gets total number of databases.
   */
  @InterfaceStability.Evolving
  int getDatabaseCount() throws MetaException;

  /**
   * Get the primary associated with a table.  Strangely enough each SQLPrimaryKey is actually a
   * column in they key, not the key itself.  Thus the list.
   * @param catName catalog name
   * @param db_name database name
   * @param tbl_name table name
   * @return list of primary key columns or an empty list if the table does not have a primary key
   * @throws MetaException error accessing the RDBMS
   */
  @Deprecated
  List<SQLPrimaryKey> getPrimaryKeys(String catName, String db_name, String tbl_name)
      throws MetaException;

  /**
   * SQLPrimaryKey represents a single primary key column.
   * Since a table can have one or more primary keys ( in case of composite primary key ),
   * this method returns List&lt;SQLPrimaryKey&gt;
   * @param request primary key request
   * @return list of primary key columns or an empty list if the table does not have a primary key
   * @throws MetaException error accessing the RDBMS
   */
  List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request)
      throws MetaException;

  /**
   * Get the foreign keys for a table.  All foreign keys for a particular table can be fetched by
   * passing null for the last two arguments.
   * @param catName catalog name.
   * @param parent_db_name Database the table referred to is in.  This can be null to match all
   *                       databases.
   * @param parent_tbl_name Table that is referred to.  This can be null to match all tables.
   * @param foreign_db_name Database the table with the foreign key is in.
   * @param foreign_tbl_name Table with the foreign key.
   * @return List of all matching foreign key columns.  Note that if more than one foreign key
   * matches the arguments the results here will be all mixed together into a single list.
   * @throws MetaException error access the RDBMS.
   */
  @Deprecated
  List<SQLForeignKey> getForeignKeys(String catName, String parent_db_name,
    String parent_tbl_name, String foreign_db_name, String foreign_tbl_name)
    throws MetaException;

  /**
   * SQLForeignKey represents a single foreign key column.
   * Since a table can have one or more foreign keys ( in case of composite foreign key ),
   * this method returns List&lt;SQLForeignKey&gt;
   * @param request ForeignKeysRequest object
   * @return List of all matching foreign key columns.  Note that if more than one foreign key
   * matches the arguments the results here will be all mixed together into a single list.
   * @throws MetaException error access the RDBMS.
   */
  List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request)
      throws MetaException;

  /**
   * Get unique constraints associated with a table.
   * @param catName catalog name.
   * @param db_name database name.
   * @param tbl_name table name.
   * @return list of unique constraints
   * @throws MetaException error access the RDBMS.
   */
  @Deprecated
  List<SQLUniqueConstraint> getUniqueConstraints(String catName, String db_name,
    String tbl_name) throws MetaException;

  /**
   * SQLUniqueConstraint represents a single unique constraint column.
   * Since a table can have one or more unique constraint ( in case of composite unique constraint ),
   * this method returns List&lt;SQLUniqueConstraint&gt;
   * @param request UniqueConstraintsRequest object.
   * @return list of unique constraints
   * @throws MetaException error access the RDBMS.
   */
  List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request) throws MetaException;

  /**
   * Get not null constraints on a table.
   * @param catName catalog name.
   * @param db_name database name.
   * @param tbl_name table name.
   * @return list of not null constraints
   * @throws MetaException error accessing the RDBMS.
   */
  @Deprecated
  List<SQLNotNullConstraint> getNotNullConstraints(String catName, String db_name,
    String tbl_name) throws MetaException;

  /**
   * SQLNotNullConstraint represents a single not null constraint column.
   * Since a table can have one or more not null constraint ( in case of composite not null constraint ),
   * this method returns List&lt;SQLNotNullConstraint&gt;
   * @param request NotNullConstraintsRequest object.
   * @return list of not null constraints
   * @throws MetaException error accessing the RDBMS.
   */
  List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest request) throws MetaException;

  /**
   * Get default values for columns in a table.
   * @param catName catalog name
   * @param db_name database name
   * @param tbl_name table name
   * @return list of default values defined on the table.
   * @throws MetaException error accessing the RDBMS
   */
  @Deprecated
  List<SQLDefaultConstraint> getDefaultConstraints(String catName, String db_name,
                                                   String tbl_name) throws MetaException;

  /**
   * SQLDefaultConstraint represents a single default constraint column.
   * Since a table can have one or more default constraint ( in case of composite default constraint ),
   * this method returns List&lt;SQLDefaultConstraint&gt;
   * @param request DefaultConstraintsRequest object.
   * @return list of default values defined on the table.
   * @throws MetaException error accessing the RDBMS
   */
  List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest request) throws MetaException;

  /**
   * Get check constraints for columns in a table.
   * @param catName catalog name.
   * @param db_name database name
   * @param tbl_name table name
   * @return ccheck constraints for this table
   * @throws MetaException error accessing the RDBMS
   */
  @Deprecated
  List<SQLCheckConstraint> getCheckConstraints(String catName, String db_name,
                                                   String tbl_name) throws MetaException;

  /**
   * SQLCheckConstraint represents a single check constraint column.
   * Since a table can have one or more check constraint ( in case of composite check constraint ),
   * this method returns List&lt;SQLCheckConstraint&gt;
   * @param request CheckConstraintsRequest object.
   * @return ccheck constraints for this table
   * @throws MetaException error accessing the RDBMS
   */
  List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest request) throws MetaException;

  /**
   * Get all constraints of the table
   * @param catName catalog name
   * @param dbName database name
   * @param tblName table name
   * @return all constraints for this table
   * @throws MetaException error accessing the RDBMS
   */
  @Deprecated
  SQLAllTableConstraints getAllTableConstraints(String catName, String dbName, String tblName)
      throws MetaException, NoSuchObjectException;

  /**
   * Get table constraints
   * @param request AllTableConstraintsRequest object
   * @return all constraints for this table
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  SQLAllTableConstraints getAllTableConstraints(AllTableConstraintsRequest request)
      throws MetaException, NoSuchObjectException;

  /**
   * Create a table with constraints
   * @param tbl table definition
   * @param constraints wrapper of all table constraints
   * @return list of constraint names
   * @throws InvalidObjectException one of the provided objects is malformed.
   * @throws MetaException error accessing the RDBMS
   */
  SQLAllTableConstraints createTableWithConstraints(Table tbl, SQLAllTableConstraints constraints) throws InvalidObjectException, MetaException;

  /**
   * Drop a constraint, any constraint.  I have no idea why add and get each have separate
   * methods for each constraint type but drop has only one.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param constraintName name of the constraint
   * @throws NoSuchObjectException no constraint of this name exists
   */
  default void dropConstraint(String catName, String dbName, String tableName,
                              String constraintName) throws NoSuchObjectException {
    dropConstraint(catName, dbName, tableName, constraintName, false);
  }

  /**
   * Drop a constraint, any constraint.  I have no idea why add and get each have separate
   * methods for each constraint type but drop has only one.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param constraintName name of the constraint
   * @param missingOk if true, it is not an error if there is no constraint of this name.  If
   *                  false and there is no constraint of this name an exception will be thrown.
   * @throws NoSuchObjectException no constraint of this name exists and missingOk = false
   */
  void dropConstraint(String catName, String dbName, String tableName, String constraintName,
                      boolean missingOk) throws NoSuchObjectException;

  /**
   * Add a primary key to a table.
   * @param pks Columns in the primary key.
   * @return the name of the constraint, as a list of strings.
   * @throws InvalidObjectException The SQLPrimaryKeys list is malformed
   * @throws MetaException error accessing the RDMBS
   */
  List<SQLPrimaryKey> addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException;

  /**
   * Add a foreign key to a table.
   * @param fks foreign key specification
   * @return foreign key name.
   * @throws InvalidObjectException the specification is malformed.
   * @throws MetaException error accessing the RDBMS.
   */
  List<SQLForeignKey> addForeignKeys(List<SQLForeignKey> fks) throws InvalidObjectException, MetaException;

  /**
   * Add unique constraints to a table.
   * @param uks unique constraints specification
   * @return unique constraint names.
   * @throws InvalidObjectException the specification is malformed.
   * @throws MetaException error accessing the RDBMS.
   */
  List<SQLUniqueConstraint> addUniqueConstraints(List<SQLUniqueConstraint> uks) throws InvalidObjectException, MetaException;

  /**
   * Add not null constraints to a table.
   * @param nns not null constraint specifications
   * @return constraint names.
   * @throws InvalidObjectException the specification is malformed.
   * @throws MetaException error accessing the RDBMS.
   */
  List<SQLNotNullConstraint> addNotNullConstraints(List<SQLNotNullConstraint> nns) throws InvalidObjectException, MetaException;

  /**
   * Add default values to a table definition.
   * @param dv list of default values
   * @return constraint names
   * @throws InvalidObjectException the specification is malformed.
   * @throws MetaException error accessing the RDBMS.
   */
  List<SQLDefaultConstraint> addDefaultConstraints(List<SQLDefaultConstraint> dv)
      throws InvalidObjectException, MetaException;

  /**
   * Add check constraints to a table.
   * @param cc check constraints to add
   * @return list of constraint names
   * @throws InvalidObjectException the specification is malformed
   * @throws MetaException error accessing the RDBMS
   */
  List<SQLCheckConstraint> addCheckConstraints(List<SQLCheckConstraint> cc) throws InvalidObjectException, MetaException;

  /**
   * Gets the unique id of the backing datastore for the metadata.
   * @return
   * @throws MetaException
   */
  String getMetastoreDbUuid() throws MetaException;

  void createResourcePlan(WMResourcePlan resourcePlan, String copyFrom, int defaultPoolSize)
      throws AlreadyExistsException, MetaException, InvalidObjectException, NoSuchObjectException;

  WMFullResourcePlan getResourcePlan(String name, String string) throws NoSuchObjectException, MetaException;

  List<WMResourcePlan> getAllResourcePlans(String string) throws MetaException;

  WMFullResourcePlan alterResourcePlan(String name, String ns, WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean canDeactivate, boolean isReplace)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException;

  WMFullResourcePlan getActiveResourcePlan(String ns) throws MetaException;

  WMValidateResourcePlanResponse validateResourcePlan(String name, String ns)
      throws NoSuchObjectException, InvalidObjectException, MetaException;

  void dropResourcePlan(String name, String ns) throws NoSuchObjectException, MetaException;

  void createWMTrigger(WMTrigger trigger)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException;

  void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidOperationException, MetaException;

  void dropWMTrigger(String resourcePlanName, String triggerName, String ns)
      throws NoSuchObjectException, InvalidOperationException, MetaException;

  List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException;

  void createPool(WMPool pool) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException;

  void alterPool(WMNullablePool pool, String poolPath) throws AlreadyExistsException,
      NoSuchObjectException, InvalidOperationException, MetaException;

  void dropWMPool(String resourcePlanName, String poolPath, String ns)
      throws NoSuchObjectException, InvalidOperationException, MetaException;

  void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException;

  void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, InvalidOperationException, MetaException;

  void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath, String ns)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException;

  void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath, String ns)
      throws NoSuchObjectException, InvalidOperationException, MetaException;

  /**
   * Create a new ISchema.
   * @param schema schema to create
   * @throws AlreadyExistsException there's already a schema with this name
   * @throws MetaException general database exception
   */
  void createISchema(ISchema schema) throws AlreadyExistsException, MetaException,
      NoSuchObjectException;

  /**
   * Alter an existing ISchema.  This assumes the caller has already checked that such a schema.
   * exists.
   * @param schemaName name of the schema
   * @param newSchema new schema object
   * @throws NoSuchObjectException no function with this name exists
   * @throws MetaException general database exception
   */
  void alterISchema(ISchemaName schemaName, ISchema newSchema) throws NoSuchObjectException, MetaException;

  /**
   * Get an ISchema by name.
   * @param schemaName schema descriptor
   * @return ISchema
   * @throws MetaException general database exception
   */
  ISchema getISchema(ISchemaName schemaName) throws MetaException;

  /**
   * Drop an ISchema.
   * This does not check whether there are valid versions of the schema in
   * existence, it assumes the caller has already done that.
   * @param schemaName schema descriptor
   * @throws NoSuchObjectException no schema of this name exists
   * @throws MetaException general database exception
   */
  void dropISchema(ISchemaName schemaName) throws NoSuchObjectException, MetaException;

  /**
   * Create a new version of an existing schema.
   * @param schemaVersion version number
   * @throws AlreadyExistsException a version of the schema with the same version number already
   * exists.
   * @throws InvalidObjectException the passed in SchemaVersion object has problems.
   * @throws NoSuchObjectException no schema with the passed in name exists.
   * @throws MetaException general database exception
   */
  void addSchemaVersion(SchemaVersion schemaVersion)
      throws AlreadyExistsException, InvalidObjectException, NoSuchObjectException, MetaException;

  /**
   * Alter a schema version.
   * Note that the Thrift interface only supports changing the serde
   * mapping and states.  This method does not guarantee it will check anymore than that.  This
   * method does not understand the state transitions and just assumes that the new state it is
   * passed is reasonable.
   * @param version version descriptor for the schema
   * @param newVersion altered SchemaVersion
   * @throws NoSuchObjectException no such version of the named schema exists
   * @throws MetaException general database exception
   */
  void alterSchemaVersion(SchemaVersionDescriptor version, SchemaVersion newVersion)
      throws NoSuchObjectException, MetaException;

  /**
   * Get a specific schema version.
   * @param version version descriptor for the schema
   * @return the SchemaVersion
   * @throws MetaException general database exception
   */
  SchemaVersion getSchemaVersion(SchemaVersionDescriptor version) throws MetaException;

  /**
   * Get the latest version of a schema.
   * @param schemaName name of the schema
   * @return latest version of the schema
   * @throws MetaException general database exception
   */
  SchemaVersion getLatestSchemaVersion(ISchemaName schemaName) throws MetaException;

  /**
   * Get all of the versions of a schema.
   * @param schemaName name of the schema
   * @return all versions of the schema
   * @throws MetaException general database exception
   */
  List<SchemaVersion> getAllSchemaVersion(ISchemaName schemaName) throws MetaException;

  /**
   * Find all SchemaVersion objects that match a query.
   * The query will select all SchemaVersions
   * that are equal to all of the non-null passed in arguments.  That is, if arguments
   * colName='name', colNamespace=null, type='string' are passed in, then all schemas that have
   * a column with colName 'name' and type 'string' will be returned.
   * @param colName column name.  Null is ok, which will cause this field to not be used in the
   *                query.
   * @param colNamespace column namespace.   Null is ok, which will cause this field to not be
   *                     used in the query.
   * @param type column type.   Null is ok, which will cause this field to not be used in the
   *             query.
   * @return List of all SchemaVersions that match.  Note that there is no expectation that these
   * SchemaVersions derive from the same ISchema.  The list will be empty if there are no
   * matching SchemaVersions.
   * @throws MetaException general database exception
   */
  List<SchemaVersion> getSchemaVersionsByColumns(String colName, String colNamespace, String type)
      throws MetaException;

  /**
   * Drop a version of the schema.
   * @param version version descriptor for the schema
   * @throws NoSuchObjectException no such version of the named schema exists
   * @throws MetaException general database exception
   */
  void dropSchemaVersion(SchemaVersionDescriptor version) throws NoSuchObjectException, MetaException;

  /**
   * Get serde information.
   * @param serDeName name of the SerDe
   * @return the SerDe, or null if there is no such serde
   * @throws NoSuchObjectException no serde with this name exists
   * @throws MetaException general database exception
   */
  SerDeInfo getSerDeInfo(String serDeName) throws NoSuchObjectException, MetaException;

  /**
   * Add a serde.
   * @param serde serde to add
   * @throws AlreadyExistsException a serde of this name already exists
   * @throws MetaException general database exception
   */
  void addSerde(SerDeInfo serde) throws AlreadyExistsException, MetaException;

  /** Adds a RuntimeStat for persistence. */
  void addRuntimeStat(RuntimeStat stat) throws MetaException;

  /** Reads runtime statistic entries. */
  List<RuntimeStat> getRuntimeStats(int maxEntries, int maxCreateTime) throws MetaException;

  /** Removes outdated statistics. */
  int deleteRuntimeStats(int maxRetainSecs) throws MetaException;

  List<TableName> getTableNamesWithStats() throws MetaException, NoSuchObjectException;

  List<TableName> getAllTableNamesForStats() throws MetaException, NoSuchObjectException;

  Map<String, List<String>> getPartitionColsWithStats(String catName, String dbName,
      String tableName) throws MetaException, NoSuchObjectException;

  /**
   * Remove older notification events.
   * @param olderThan Remove any events older or equal to a given number of seconds
   */
  void cleanWriteNotificationEvents(int olderThan);

  /**
   * Get all write events for a specific transaction .
   * @param txnId get all the events done by this transaction
   * @param dbName the name of db for which dump is being taken
   * @param tableName the name of the table for which the dump is being taken
   */
  List<WriteEventInfo> getAllWriteEventInfo(long txnId, String dbName, String tableName) throws MetaException;

  /**
   * Checking if table is part of a materialized view.
   * @param catName catalog the table is in
   * @param dbName database the table is in
   * @param tblName table name
   * @return list of materialized views that uses the table
   */
  List<String> isPartOfMaterializedView(String catName, String dbName, String tblName);

  /**
   * Returns details about a scheduled query by name.
   *
   * @throws NoSuchObjectException if an object by the given name dosen't exists.
   */
  ScheduledQuery getScheduledQuery(ScheduledQueryKey scheduleKey) throws MetaException, NoSuchObjectException;

  /**
   * Carries out maintenance of scheduled queries (insert/update/drop).
   */
  void scheduledQueryMaintenance(ScheduledQueryMaintenanceRequest request)
      throws MetaException, NoSuchObjectException, AlreadyExistsException, InvalidInputException;

  /**
   * Checks whenever a query is available for execution.
   *
   * @return optionally a scheduled query to be processed.
   */
  ScheduledQueryPollResponse scheduledQueryPoll(ScheduledQueryPollRequest request) throws MetaException;

  /**
   * Registers the progress a scheduled query being executed.
   */
  void scheduledQueryProgress(ScheduledQueryProgressInfo info)
      throws MetaException, NoSuchObjectException, InvalidOperationException;

  /**
   * Add the replication metrics and progress info.
   * @param replicationMetricList
   */
  void addReplicationMetrics(ReplicationMetricList replicationMetricList);

  /**
   * Gets the replication metrics and progress info.
   * @param replicationMetricsRequest
   */
  ReplicationMetricList getReplicationMetrics(GetReplicationMetricsRequest replicationMetricsRequest);

  Map<String, Map<String, String>> updatePartitionColumnStatisticsInBatch(
          Map<String, ColumnStatistics> partColStatsMap,
          Table tbl, List<TransactionalMetaStoreEventListener> listeners,
          String validWriteIds, long writeId)
          throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  int deleteReplicationMetrics(int maxRetainSecs);

  int deleteScheduledExecutions(int maxRetainSecs);

  int markScheduledExecutionsTimedOut(int timeoutSecs) throws InvalidOperationException, MetaException;

  void deleteAllPartitionColumnStatistics(TableName tn, String writeIdList);

  void createOrUpdateStoredProcedure(StoredProcedure proc) throws NoSuchObjectException, MetaException;

  StoredProcedure getStoredProcedure(String catName, String db, String name) throws MetaException;

  void dropStoredProcedure(String catName, String dbName, String funcName) throws MetaException;

  List<String> getAllStoredProcedures(ListStoredProcedureRequest request);

  void addPackage(AddPackageRequest request) throws MetaException, NoSuchObjectException;
  Package findPackage(GetPackageRequest request);
  List<String> listPackages(ListPackageRequest request);
  void dropPackage(DropPackageRequest request);
  public MTable ensureGetMTable(String catName, String dbName, String tblName) throws NoSuchObjectException;

  /** Persistent Property Management. */
  default PropertyStore getPropertyStore() {
    return null;
  }

}
