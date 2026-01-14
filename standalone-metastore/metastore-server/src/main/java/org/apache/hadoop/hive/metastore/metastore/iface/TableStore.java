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

package org.apache.hadoop.hive.metastore.metastore.iface;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.model.MPartition;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.metastore.RawStoreAware;
import org.apache.hadoop.hive.metastore.metastore.MetaDescriptor;
import org.apache.hadoop.hive.metastore.metastore.impl.TableStoreImpl;
import org.apache.thrift.TException;

@MetaDescriptor(alias = "table", defaultImpl = TableStoreImpl.class)
public interface TableStore extends RawStoreAware {

  void createTable(Table tbl) throws InvalidObjectException,
      MetaException;
  /**
   * Drop a table.
   * @param table the table to be dropped
   * @return true if the table was dropped
   * @throws MetaException something went wrong, usually in the RDBMS or storage
   * @throws NoSuchObjectException No table of this name
   * @throws InvalidObjectException Don't think this is ever actually thrown
   * @throws InvalidInputException Don't think this is ever actually thrown
   */
  boolean dropTable(TableName table) throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  /**
   * Drop all partitions from the table, and return the partition's location that not a child of baseLocationToNotShow,
   * when the baseLocationToNotShow is not null.
   * @param table the table to drop partitions from
   * @param baseLocationToNotShow Partition locations which are child of this path are omitted
   * @param message postgres of this drop
   * @return list of partition locations outside baseLocationToNotShow
   * @throws MetaException something went wrong, usually in the RDBMS or storage
   * @throws InvalidInputException unable to drop all partitions due to the invalid input
   */
  List<String> dropAllPartitionsAndGetLocations(TableName table, String baseLocationToNotShow, AtomicReference<String> message)
      throws MetaException, InvalidInputException, NoSuchObjectException, InvalidObjectException;

  /**
   * Get a table object.
   * @param table the table to be got
   * @param writeIdList string format of valid writeId transaction list
   * @return table object, or null if no such table exists (wow it would be nice if we either
   * consistently returned null or consistently threw NoSuchObjectException).
   * @throws MetaException something went wrong in the RDBMS
   */
  Table getTable(TableName table,
      String writeIdList, long tableId) throws MetaException;

  /**
   * Add a list of partitions to a table.
   * @param table the table this partitions added to .
   * @param parts list of partitions to be added.
   * @return true if the operation succeeded.
   * @throws InvalidObjectException never throws this AFAICT
   * @throws MetaException the partitions don't belong to the indicated table or error writing to
   * the RDBMS.
   */
  boolean addPartitions(TableName table, List<Partition> parts)
      throws InvalidObjectException, MetaException;

  /**
   * Get a partition.
   * @param table table name.
   * @param part_vals partition values for this table.
   * @param writeIdList string format of valid writeId transaction list
   * @return the partition.
   * @throws MetaException error reading from RDBMS.
   * @throws NoSuchObjectException no partition matching this specification exists.
   */
  Partition getPartition(TableName table,
      List<String> part_vals,
      String writeIdList)
      throws MetaException, NoSuchObjectException;

  /**
   * Get some or all partitions for a table.
   * @param table table name
   * @param args additional arguments for getting partitions
   * @return list of partitions
   * @throws MetaException error access the RDBMS.
   * @throws NoSuchObjectException no such table exists
   */
  List<Partition> getPartitions(TableName table,
      GetPartitionsArgs args) throws MetaException, NoSuchObjectException;

  /**
   * Get the location for every partition of a given table. If a partition location is a child of
   * baseLocationToNotShow then the partitionName is returned, but the only null location is
   * returned.
   * @param tableName table name.
   * @param baseLocationToNotShow Partition locations which are child of this path are omitted, and
   *     null value returned instead.
   * @param max The maximum number of partition locations returned, or -1 for all
   * @return The map of the partitionName, location pairs
   */
  Map<String, String> getPartitionLocations(TableName tableName, String baseLocationToNotShow, int max);

  /**
   * Alter a table.
   * @param tableName name of the table.
   * @param newTable New table object.  Which parts of the table can be altered are
   *                 implementation specific.
   * @return
   * @throws InvalidObjectException The new table object is invalid.
   * @throws MetaException something went wrong, usually in the RDBMS or storage.
   */
  Table alterTable(TableName tableName, Table newTable,
      String queryValidWriteIds)
      throws InvalidObjectException, MetaException;


  boolean dropPartitions(TableName tableName, List<String> partNames)
      throws MetaException, NoSuchObjectException;

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
      GetProjectionsSpec projectionSpec, String tablePattern) throws MetaException,
      UnknownDBException;

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
   * Get partition names with a filter. This is a portion of the SQL where clause.
   * @param tableName table name
   * @param args additional arguments for getting partition names
   * @return list of partition names matching the criteria
   * @throws MetaException Error accessing the RDBMS or processing the filter.
   * @throws NoSuchObjectException no such table.
   */
  List<String> listPartitionNamesByFilter(TableName tableName,
      GetPartitionsArgs args) throws MetaException, NoSuchObjectException;

  /**
   * Get a partial or complete list of names for partitions of a table.
   * @param tableName the table
   * @param defaultPartName default partition name.
   * @param exprBytes expression for filtering resulting list, serialized from ExprNodeDesc.
   * @param order ordered the resulting list.
   * @param maxParts maximum number of partitions to retrieve, -1 for all.
   * @return list of partition names.
   * @throws MetaException there was an error accessing the RDBMS
   */
  List<String> listPartitionNames(TableName tableName,
      String defaultPartName, byte[] exprBytes, String order,
      int maxParts) throws MetaException, NoSuchObjectException;

  /**
   * Get partitions using an already parsed expression.
   * @param tableName the table
   * @param args additional arguments for getting partitions
   * @return true if the result contains unknown partitions.
   * @throws TException error executing the expression
   */
  boolean getPartitionsByExpr(TableName tableName,
      List<Partition> result, GetPartitionsArgs args)
      throws TException;

  /**
   * Get partitions by name.
   * @param tableName the table.
   * @param args additional arguments for getting partitions
   * @return list of matching partitions
   * @throws MetaException error accessing the RDBMS.
   * @throws NoSuchObjectException No such table.
   */
  List<Partition> getPartitionsByNames(TableName tableName,
      GetPartitionsArgs args) throws MetaException, NoSuchObjectException;

  /**
   * Alter a partition.
   * @param tableName the table.
   * @param part_vals partition values that describe the partition.
   * @param new_part new partition object.  This should be a complete copy of the old with
   *                 changes values, not just the parts to update.
   * @return
   * @throws InvalidObjectException No such partition.
   * @throws MetaException error accessing the RDBMS.
   */
  Partition alterPartition(TableName tableName, List<String> part_vals,
      Partition new_part, String queryValidWriteIds)
      throws InvalidObjectException, MetaException;

  /**
   * Alter a set of partitions.
   * @param tableName table name.
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
  List<Partition> alterPartitions(TableName tableName,
      List<List<String>> part_vals_list, List<Partition> new_parts, long writeId,
      String queryValidWriteIds)
      throws InvalidObjectException, MetaException;


  /**
   * Get partitions with a filter.  This is a portion of the SQL where clause.
   * @param tableName table name
   * @param args additional arguments for getting partitions
   * @return list of partition objects matching the criteria
   * @throws MetaException Error accessing the RDBMS or processing the filter.
   * @throws NoSuchObjectException no such table.
   */
  List<Partition> getPartitionsByFilter(
      TableName tableName, GetPartitionsArgs args)
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
   * Fetch a partition along with privilege information for a particular user.
   * @param tableName table name.
   * @param partVals partition values
   * @param user_name user to get privilege information for.
   * @param group_names groups to get privilege information for.
   * @return a partition
   * @throws MetaException error accessing the RDBMS.
   * @throws NoSuchObjectException no such partition exists
   * @throws InvalidObjectException error fetching privilege information
   */
  Partition getPartitionWithAuth(TableName tableName,
      List<String> partVals, String user_name, List<String> group_names)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  /**
   * Lists partition names that match a given partial specification
   * @param tableName
   *          The name of the table which has the partitions
   * @param partVals
   *          A partial list of values for partitions in order of the table's partition keys.
   *          Entries can be empty if you only want to specify latter partitions.
   * @param maxParts
   *          The maximum number of partitions to return
   * @return A list of partition names that match the partial spec.
   * @throws MetaException error accessing RDBMS
   * @throws NoSuchObjectException No such table exists
   */
  List<String> listPartitionNamesPs(TableName tableName, List<String> partVals, short maxParts)
      throws MetaException, NoSuchObjectException;

  /**
   * Lists partitions that match a given partial specification and sets their auth privileges.
   *   If userName and groupNames null, then no auth privileges are set.
   * @param tableName
   *          The name of the table which has the partitions
   * @param args additional arguments for getting partitions
   * @return A list of partitions that match the partial spec.
   * @throws MetaException error access RDBMS
   * @throws NoSuchObjectException No such table exists
   * @throws InvalidObjectException error access privilege information
   */
  List<Partition> listPartitionsPsWithAuth(TableName tableName,
      GetPartitionsArgs args) throws MetaException, InvalidObjectException, NoSuchObjectException;

  /**
   * Get the number of partitions that match a provided SQL filter.
   * @param tableName table name.
   * @param filter filter from Hive's SQL where clause
   * @return number of matching partitions.
   * @throws MetaException error accessing the RDBMS or executing the filter
   * @throws NoSuchObjectException no such table
   */
  int getNumPartitionsByFilter(TableName tableName, String filter)
      throws MetaException, NoSuchObjectException;

  /**
   * Get the number of partitions that match a given partial specification.
   * @param tableName table name.
   * @param partVals A partial list of values for partitions in order of the table's partition keys.
   *                  Entries can be empty if you need to specify latter partitions.
   * @return number of matching partitions.
   * @throws MetaException error accessing the RDBMS or working with the specification.
   * @throws NoSuchObjectException no such table.
   */
  int getNumPartitionsByPs(TableName tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException;

  /**
   * Get a list of partition values as one big struct.
   * @param tableName table name.
   * @param cols partition key columns
   * @param applyDistinct whether to apply distinct to the list
   * @param filter filter to apply to the partition names
   * @param ascending whether to put in ascending order
   * @param order whether to order
   * @param maxParts maximum number of parts to return, or -1 for all
   * @return struct with all of the partition value information
   * @throws MetaException error access the RDBMS
   */
  PartitionValuesResponse listPartitionValues(TableName tableName,
      List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending,
      List<FieldSchema> order, long maxParts) throws MetaException;

  /**
   * Update creation metadata for a materialized view.
   * @param tableName table name.
   * @param cm new creation metadata
   * @throws MetaException error accessing the RDBMS.
   */
  void updateCreationMetadata(TableName tableName, CreationMetadata cm)
      throws MetaException;

  /**
   * Retrieve all materialized views.
   * @return all materialized views in a catalog
   * @throws MetaException error querying the RDBMS
   */
  List<Table> getAllMaterializedViewObjectsForRewriting(String catName) throws MetaException;

  MTable ensureGetMTable(TableName tableName) throws NoSuchObjectException;

  /**
   * Checking if table is part of a materialized view.
   * @param tableName table name
   * @return list of materialized views that uses the table
   */
  List<String> isPartOfMaterializedView(TableName tableName);

  Table markPartitionForEvent(TableName tableName, Map<String,String> partVals, PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException;

  boolean isPartitionMarkedForEvent(TableName tableName, Map<String, String> partName, PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException;

  int getObjectCount(String fieldName, String objName);

  /**
   * Updates a given table parameter with expected value.
   *
   * @return the number of rows updated
   */
  long updateParameterWithExpectedValue(Table table, String key, String expectedValue, String newValue)
      throws MetaException, NoSuchObjectException;

  MPartition ensureGetMPartition(TableName tableName, List<String> partVals) throws MetaException;
}
