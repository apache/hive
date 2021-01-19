/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.impala.plan;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.SlotRef;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsStorageDescriptor;
import org.apache.impala.catalog.HiveStorageDescriptorFactory;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.catalog.RowFormat;
import org.apache.impala.catalog.SqlConstraints;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TColumnDescriptor;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.ListMap;
import org.apache.impala.util.MetaStoreUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Specifies the location and format for the results of a query. This is primarily used
 * by external frontends that want to convey results through files rather than streaming
 * the results from a coordinator. This is a mutally exclusive feature from result
 * spooling which spools results to the local filesystem to mitigate the effect of slow
 * "streaming" clients which otherwise could cause backends to hold resources that then in
 * turn lowers total throughput. External frontends are responsible for managing the
 * resulting files (caching, cleaning up files, etc).
 */
public class ImpalaResultLocation implements FeFsTable {
  private final static Logger LOG = LoggerFactory.getLogger(ImpalaResultLocation.class);
  // Dummy table and database names for methods that require a table, db name
  public final static String RESULT_TABLE = "_result_table_";
  public final static String RESULT_DB = "_result_db_";
  public final static HdfsFileFormat DEFAULT_FILE_FORMAT = HdfsFileFormat.TEXT;

  // Used to generate expected schema
  private final List<Expr> outputExprs_;
  // The format of the returned rows
  private final HdfsFileFormat fileFormat_;
  // Path where results will be written
  private String baseDir_;

  public ImpalaResultLocation(List<Expr> outputExprs, String baseDir) {
    this(outputExprs, baseDir, DEFAULT_FILE_FORMAT);
  }

  public ImpalaResultLocation(List<Expr> outputExprs, String baseDir, String outputFormatClassName) {
    this(outputExprs, baseDir, HdfsFileFormat.fromJavaClassName(outputFormatClassName));
  }

  public ImpalaResultLocation(List<Expr> outputExprs, String baseDir,
      HdfsFileFormat fileFormat) {
    super();
    outputExprs_ = outputExprs;
    baseDir_ = baseDir;
    fileFormat_ = fileFormat;
  }

  /**
   * @return the set of file formats that the partitions in this table use.
   * This API is only used by the TableSink to write out partitions. It
   * should not be used for scanning.
   */
  public Set<HdfsFileFormat> getFileFormats() {
    return ImmutableSet.of(fileFormat_);
  }

  public int parseSkipHeaderLineCount(StringBuilder error) {
    return 0;
  }

  /**
   * @return the full name of this table (e.g. "my_db.my_table")
   */
  public String getFullName() {
    return RESULT_DB + "." + RESULT_TABLE;
  }

  /**
   * @return the table name in structured form
   */
  public TableName getTableName() {
    return new TableName(RESULT_DB, RESULT_TABLE);
  }

  /**
   * @return the Thrift table descriptor for this table
   */
  public TTableDescriptor toThriftDescriptor(int tableId, Set<Long> referencedPartitions) {
    List<TColumnDescriptor> columnDescs = new ArrayList<>();
    List<String> columnNames = new ArrayList<>();

    int literalCount = 0;
    // Iterate over passed in outputExprs and create corresponding TColumnDescriptors
    for (Expr expr : outputExprs_) {
      if (expr instanceof SlotRef) {
        SlotRef slot = (SlotRef) expr;
        columnDescs.add(new TColumnDescriptor(slot.getDesc().getLabel(), slot.getType().toThrift()));
        columnNames.add(slot.getDesc().getLabel());
      } else {
        // Generically handle other expression types. We generally only care about about the return
        // types. LiteralExpr, ImpalaFunctionCallExpr, ImpalaCaseExpr, ImpalaBinaryCompExpr are
        // some types of Exprs that can occur here.
        // TODO: Investigate column naming (can we generate better names for the columns for
        // different Expr types).
        columnDescs.add(new TColumnDescriptor("_c" + literalCount,  expr.getType().toThrift()));
        columnNames.add("_c" + literalCount);
        literalCount++;
      }
    }

    // Create a storage descriptor with the desired result row format
    HdfsStorageDescriptor hdfsSd;
    try {
      StorageDescriptor sd = HiveStorageDescriptorFactory.createSd(fileFormat_.toThrift(),
          RowFormat.DEFAULT_ROW_FORMAT);
      hdfsSd = HdfsStorageDescriptor.fromStorageDescriptor(RESULT_TABLE, sd);
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create HdfsStorageDescriptor");
    }

    // Create the required thrift structures
    THdfsPartition thriftHdfsPart = new THdfsPartition(new ArrayList<>());
    thriftHdfsPart.setLineDelim(hdfsSd.getLineDelim());
    thriftHdfsPart.setFieldDelim(hdfsSd.getFieldDelim());
    thriftHdfsPart.setCollectionDelim(hdfsSd.getCollectionDelim());
    thriftHdfsPart.setMapKeyDelim(hdfsSd.getMapKeyDelim());
    thriftHdfsPart.setEscapeChar(hdfsSd.getEscapeChar());
    thriftHdfsPart.setFileFormat(hdfsSd.getFileFormat().toThrift());
    thriftHdfsPart.setBlockSize(hdfsSd.getBlockSize());
    Map<Long, THdfsPartition> idToPartition = new HashMap<>();
    THdfsTable hdfsTable = new THdfsTable(baseDir_, columnNames,
        MetaStoreUtil.DEFAULT_NULL_PARTITION_KEY_VALUE,
        FeFsTable.DEFAULT_NULL_COLUMN_VALUE, idToPartition, thriftHdfsPart);
    // TableId must be 0 due to results table being the root sink
    TTableDescriptor tableDesc = new TTableDescriptor(/* tableId */ 0,
        TTableType.HDFS_TABLE, columnDescs, /* numClusteringCols */ 0,
        RESULT_DB, RESULT_TABLE)
      .setHdfsTable(hdfsTable);
    return tableDesc;
  }

  /** @see CatalogObject#isLoaded() */
  public boolean isLoaded() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the metastore.api.Table object this Table was created from. Returns null
   * if the derived Table object was not created from a metastore Table (ex. InlineViews).
   */
  public Table getMetaStoreTable() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the Hive StorageHandler class name that should be used for this table,
   * or null if no storage handler is needed.
   */
  public String getStorageHandlerClassName() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the type of catalog object -- either TABLE or VIEW.
   */
  public TCatalogObjectType getCatalogObjectType() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the short name of this table (e.g. "my_table")
   */
  public String getName() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the columns in this table
   */
  public List<Column> getColumns() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return an unmodifiable list of all columns, but with partition columns at the end of
   * the list rather than the beginning. This is equivalent to the order in
   * which Hive enumerates columns.
   */
  public List<Column> getColumnsInHiveOrder() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return a list of the column names ordered by position.
   */
  public List<String> getColumnNames() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the list of primary keys for this table.
   */
  public List<SQLPrimaryKey> getPrimaryKeys() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the list of foreign keys for this table.
   */
  public List<SQLForeignKey> getForeignKeys() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return an unmodifiable list of all partition columns.
   */
  public List<Column> getClusteringColumns() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return an unmodifiable list of all columns excluding any partition columns.
   */
  public List<Column> getNonClusteringColumns() {
    throw new IllegalStateException("Not implemented");
  }

  public int getNumClusteringCols() {
    throw new IllegalStateException("Not implemented");
  }

  public boolean isClusteringColumn(Column c) {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * Case-insensitive lookup.
   *
   * @return null if the column with 'name' is not found.
   */
  public Column getColumn(String name) {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the type of this table (array of struct) that mirrors the columns.
   */
  public ArrayType getType() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the database that that contains this table
   */
  public FeDb getDb() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the estimated number of rows in this table (or -1 if unknown)
   */
  public long getNumRows() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the stats for this table
   */
  public TTableStats getTTableStats() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the write id for this table
   */
  public long getWriteId() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the valid write id list for this table
   */
  public ValidWriteIdList getValidWriteIds() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the owner user for this table. If the table is not loaded or the owner is
   * missing returns null.
   */
  public String getOwnerUser() {
    throw new IllegalStateException("Not implemented");
  }

  public ListMap<TNetworkAddress> getHostIndex() {
    throw new IllegalStateException("Not implemented");
  }

  public TResultSet getTableStats() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return all partitions of this table
   */
  public Collection<? extends PrunablePartition> getPartitions() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return identifiers for all partitions in this table
   */
  public Set<Long> getPartitionIds() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * Returns the map from partition identifier to prunable partition.
   */
  public Map<Long, ? extends PrunablePartition> getPartitionMap() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @param the index of the target partitioning column
   * @return a map from value to a set of partitions for which column 'col'
   * has that value.
   */
  public TreeMap<LiteralExpr, Set<Long>> getPartitionValueMap(int col) {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the set of partitions which have a null value for column
   * index 'colIdx'.
   */
  public Set<Long> getNullPartitionIds(int colIdx) {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * Returns the full partition objects for the given partition IDs, which must
   * have been obtained by prior calls to the above methods.
   * @throws IllegalArgumentException if any partition ID does not exist
   */
  public List<? extends FeFsPartition> loadPartitions(Collection<Long> ids) {
    throw new IllegalStateException("Not implemented");
  }

  public String getNullPartitionKeyValue() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the base HDFS directory where files of this table are stored.
   */
  public String getHdfsBaseDir() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the FsType where files of this table are stored.
   */
  public FileSystemUtil.FsType getFsType() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return the total number of bytes stored for this table.
   */
  public long getTotalHdfsBytes() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return true if this table's schema as stored in the HMS has been overridden
   * by an Avro schema.
   */
  public boolean usesAvroSchemaOverride() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * Return true if the table's base directory may be written to, in order to
   * create new partitions, or insert into the default partition in the case of
   * an unpartitioned table.
   */
  public boolean hasWriteAccessToBaseDir() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * Return some location found without write access for this table, useful
   * in error messages about insufficient permissions to insert into a table.
   *
   * In case multiple locations are missing write access, the particular
   * location returned is implementation-defined.
   *
   * Returns null if all partitions have write access.
   */
  public String getFirstLocationWithoutWriteAccess() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return true if the table and all its partitions reside at locations which
   * support caching (e.g. HDFS).
   */
  public boolean isCacheable() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return true if the table resides at a location which supports caching
   * (e.g. HDFS).
   */
  public boolean isLocationCacheable() {
    throw new IllegalStateException("Not implemented");
  }

  /**
   * @return true if this table is marked as cached
   */
  public boolean isMarkedCached() {
    throw new IllegalStateException("Not implemented");
  }

  /*
   * Returns the storage location (HDFS path) of this table.
   */
  public String getLocation() {
    throw new IllegalStateException("Not implemented");
  }

  public SqlConstraints getSqlConstraints() {
    throw new IllegalStateException("Not implemented");
  }
}
