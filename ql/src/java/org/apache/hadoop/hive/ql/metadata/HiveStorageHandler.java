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

package org.apache.hadoop.hive.ql.metadata;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.common.type.SnapshotContext;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Context.Operation;
import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.create.like.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.parse.AlterTableSnapshotRefSpec;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec;
import org.apache.hadoop.hive.ql.parse.DeleteSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.StorageFormat.StorageHandlerTypes;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.UpdateSemanticAnalyzer;
import org.apache.hadoop.hive.ql.plan.PartitionAwareOptimizationCtx;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MergeTaskProperties;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.hive.ql.stats.Partish;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.OutputFormat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * HiveStorageHandler defines a pluggable interface for adding
 * new storage handlers to Hive.  A storage handler consists of
 * a bundle of the following:
 *
 *<ul>
 *<li>input format
 *<li>output format
 *<li>serde
 *<li>metadata hooks for keeping an external catalog in sync
 * with Hive's metastore
 *<li>rules for setting up the configuration properties on
 * map/reduce jobs which access tables stored by this handler
 *</ul>
 *
 * Storage handler classes are plugged in using the STORED BY 'classname'
 * clause in CREATE TABLE.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface HiveStorageHandler extends Configurable {

  List<AlterTableType> DEFAULT_ALLOWED_ALTER_OPS = ImmutableList.of(
      AlterTableType.ADDPROPS, AlterTableType.DROPPROPS, AlterTableType.ADDCOLS);

  /**
   * @return Class providing an implementation of {@link InputFormat}
   */
  Class<? extends InputFormat> getInputFormatClass();

  /**
   * @return Class providing an implementation of {@link OutputFormat}
   */
  Class<? extends OutputFormat> getOutputFormatClass();

  /**
   * @return Class providing an implementation of {@link AbstractSerDe}
   */
  Class<? extends AbstractSerDe> getSerDeClass();

  /**
   * @return metadata hook implementation, or null if this
   * storage handler does not need any metadata notifications
   */
  HiveMetaHook getMetaHook();

  /**
   * Returns the implementation specific authorization provider
   *
   * @return authorization provider
   * @throws HiveException
   */
  HiveAuthorizationProvider getAuthorizationProvider() throws HiveException;

  /**
   * This method is called to allow the StorageHandlers the chance
   * to populate the JobContext.getConfiguration() with properties that
   * maybe be needed by the handler's bundled artifacts (ie InputFormat, SerDe, etc).
   * Key value pairs passed into jobProperties are guaranteed to be set in the job's
   * configuration object. User's can retrieve "context" information from tableDesc.
   * User's should avoid mutating tableDesc and only make changes in jobProperties.
   * This method is expected to be idempotent such that a job called with the
   * same tableDesc values should return the same key-value pairs in jobProperties.
   * Any external state set by this method should remain the same if this method is
   * called again. It is up to the user to determine how best guarantee this invariant.
   *
   * This method in particular is to create a configuration for input.
   * @param tableDesc descriptor for the table being accessed
   * @param jobProperties receives properties copied or transformed
   * from the table properties
   */
  void configureInputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties);

  /**
   * This method is called to allow the StorageHandlers the chance to
   * populate secret keys into the job's credentials.
   */
  void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> secrets);

  /**
   * This method is called to allow the StorageHandlers the chance
   * to populate the JobContext.getConfiguration() with properties that
   * maybe be needed by the handler's bundled artifacts (ie InputFormat, SerDe, etc).
   * Key value pairs passed into jobProperties are guaranteed to be set in the job's
   * configuration object. User's can retrieve "context" information from tableDesc.
   * User's should avoid mutating tableDesc and only make changes in jobProperties.
   * This method is expected to be idempotent such that a job called with the
   * same tableDesc values should return the same key-value pairs in jobProperties.
   * Any external state set by this method should remain the same if this method is
   * called again. It is up to the user to determine how best guarantee this invariant.
   *
   * This method in particular is to create a configuration for output.
   * @param tableDesc descriptor for the table being accessed
   * @param jobProperties receives properties copied or transformed
   * from the table properties
   */
  void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> jobProperties);

  /**
   * Configures properties for a job based on the definition of the
   * source or target table it accesses.
   *
   * @param tableDesc descriptor for the table being accessed
   * @param jobProperties receives properties copied or transformed from the table properties
   *
   * @deprecated since 4.0.1, will be removed in 5.0.0,
   * use {@link #configureInputJobProperties} and {@link #configureOutputJobProperties} instead.
   */
  @Deprecated
  void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties);

  /**
   * Called just before submitting MapReduce job.
   *
   * @param tableDesc descriptor for the table being accessed
   * @param jobConf jobConf for MapReduce job
   */
  void configureJobConf(TableDesc tableDesc, JobConf jobConf);

  /**
   * Used to fetch runtime information about storage handler during DESCRIBE EXTENDED statement
   *
   * @param table table definition
   * @return StorageHandlerInfo containing runtime information about storage handler
   * OR `null` if the storage handler choose to not provide any runtime information.
   */
  default StorageHandlerInfo getStorageHandlerInfo(Table table) throws MetaException {
    return null;
  }
  
  default StorageHandlerTypes getType() {
    return StorageHandlerTypes.DEFAULT;
  }

  default LockType getLockType(WriteEntity writeEntity){
    return LockType.EXCLUSIVE;
  }

  /**
   * Test if the storage handler allows the push-down of join filter predicate to prune further the splits.
   *
   * @param table The table to filter.
   * @param syntheticFilterPredicate Join filter predicate.
   * @return true if supports dynamic split pruning for the given predicate.
   */

  default boolean addDynamicSplitPruningEdge(org.apache.hadoop.hive.ql.metadata.Table table,
      ExprNodeDesc syntheticFilterPredicate) {
    return false;
  }

  /**
   * Used to add additional operator specific information from storage handler during DESCRIBE EXTENDED statement.
   *
   * @param operatorDesc operatorDesc
   * @param initialProps Map containing initial operator properties
   * @return Map&lt;String, String&gt; containing additional operator specific information from storage handler
   * OR `initialProps` if the storage handler choose to not provide any such information.
   */
  default Map<String, String> getOperatorDescProperties(OperatorDesc operatorDesc, Map<String, String> initialProps) {
    return initialProps;
  }

  /**
   * Returns basic statistics (numRows, numFiles, totalSize) calculated by the underlying storage handler
   * implementation.
   * @param partish table/partition wrapper object
   * @return map of basic statistics
   */
  default Map<String, String> getBasicStatistics(Partish partish) {
    return null;
  }

  /**
   * Compute basic statistics (numRows, numFiles, totalSize) for the given table/partition.
   * @param partish table/partition wrapper object
   * @return map of basic statistics
   */
  default Map<String, String> computeBasicStatistics(Partish partish) {
    return null;
  }

  /**
   * Check if the storage handler can provide basic statistics.
   * @return true if the storage handler can supply the basic statistics
   */
  default boolean canProvideBasicStatistics() {
    return false;
  }
  
  /**
   * Check if the storage handler can provide partition statistics.
   * @return true if the storage handler can supply the partition statistics
   */
  default boolean canProvidePartitionStatistics(org.apache.hadoop.hive.ql.metadata.Table table) {
    return false;
  }

  /**
   * Returns column statistics (upper/lower bounds, number of Null/NaN values, NDVs, histogram).
   * @param table table object
   * @param colNames list of column names            
   * @return list of ColumnStatisticsObj objects
   */
  default List<ColumnStatisticsObj> getColStatistics(org.apache.hadoop.hive.ql.metadata.Table table, 
        List<String> colNames) {
    return null;
  }

  /**
   * Returns column statistics (upper/lower bounds, number of Null/NaN values, NDVs, histogram).
   * @param table table object
   *
   * @deprecated since 4.1.0, will be removed in 5.0.0,
   * use {@link #getColStatistics(org.apache.hadoop.hive.ql.metadata.Table, List<String>)} instead.
   */
  @Deprecated
  default List<ColumnStatisticsObj> getColStatistics(org.apache.hadoop.hive.ql.metadata.Table table) {
    return getColStatistics(table, null);
  }

  /**
   * Returns an aggregated column statistics for the supplied partition list
   * @param table table object
   * @param colNames list of column names
   * @param partNames list of partition names
   * @return AggrStats object
  */ 
  default AggrStats getAggrColStatsFor(org.apache.hadoop.hive.ql.metadata.Table table, List<String> colNames, 
        List<String> partNames) throws MetaException {
    return null;
  }

  /**
   * Set column stats for non-native tables
   * @param table table object
   * @param colStats list of ColumnStatistics objects
   * @return true if operation is successful                 
   */
  default boolean setColStatistics(org.apache.hadoop.hive.ql.metadata.Table table, List<ColumnStatistics> colStats) {
    return false;
  }

  /**
   * Check if the storage handler can provide column statistics.
   * @param table table object
   * @return true if the storage handler can supply the column statistics
   */
  default boolean canProvideColStatistics(org.apache.hadoop.hive.ql.metadata.Table table) {
    return false;
  }

  /**
   * Check if the storage handler can set col statistics.
   * @param table table object
   * @return true if the storage handler can set the col statistics
   */
  default boolean canSetColStatistics(org.apache.hadoop.hive.ql.metadata.Table table) {
    return false;
  }

  /**
   * Check if the storage handler can answer a few queries like count(1) purely using statistics.
   * @param partish table/partition wrapper object
   * @return true if the storage handler can answer query using statistics
   */
  default boolean canComputeQueryUsingStats(Partish partish) {
    return false;
  }

  /**
   * Check if the storage handler can answer a few queries like count(1) purely using statistics.
   * @param table table wrapper object
   *
   * @deprecated since 4.0.1, will be removed in 5.0.0,
   * use {@link #canComputeQueryUsingStats(Partish)} instead.
   */
  @Deprecated
  default boolean canComputeQueryUsingStats(org.apache.hadoop.hive.ql.metadata.Table table) {
    return canComputeQueryUsingStats(Partish.buildFor(table));
  }
  
  /**
   *
   * Gets the storage format descriptor to be used for temp table for LOAD data.
   * @param table table object
   * @return StorageFormatDescriptor if the storage handler can support load data
   */
  default StorageFormatDescriptor getStorageFormatDescriptor(Table table) throws SemanticException {
    return null;
  }

  /**
   * Checks whether the table supports appending data files to the table.
   * @param table the table
   * @param withPartClause whether a partition is specified
   * @return true if the table can append files directly to the table
   * @throws SemanticException in case of any error.
   */
  default boolean supportsAppendData(Table table, boolean withPartClause) throws SemanticException {
    return false;
  }

  /**
   * Appends files to the table
   * @param tbl the table object.
   * @param fromURI the source of files.
   * @param isOverwrite whether to overwrite the existing table data.
   * @param partitionSpec the partition spec.
   * @throws SemanticException in case of any error
   */
  default void appendFiles(Table tbl, URI fromURI, boolean isOverwrite, Map<String, String> partitionSpec)
      throws SemanticException {
    throw new SemanticException(ErrorMsg.LOAD_INTO_NON_NATIVE.getMsg());
  }

  /**
   * Check if CTAS and CMV operations should behave in a direct-insert manner (i.e. no move task).
   * <p>
   * Please note that the atomicity of the operation will suffer in this case, i.e. the created table might become
   * exposed, depending on the implementation, before the CTAS or CMV operations finishes.
   * Rollback (e.g. dropping the table) is also the responsibility of the storage handler in case of failures.
   *
   * @return whether direct insert CTAS or CMV is required
   */
  default boolean directInsert() {
    return false;
  }

  /**
   * Check if partition columns should be removed and added to the list of regular columns in HMS.
   * This can be useful for non-native tables where the table format/layout differs from the standard Hive table layout,
   * e.g. Iceberg tables. For these table formats, the partition column values are stored in the data files along with
   * regular column values, therefore the object inspectors should include the partition columns as well.
   * Any partitioning scheme provided via the standard HiveQL syntax will be honored but stored in someplace
   * other than HMS, depending on the storage handler implementation.
   *
   * @return whether the table has built-in partitioning support
   */
  default boolean supportsPartitioning() {
    return false;
  }

  /**
   * Retains storage handler specific properties during CTLT.
   * @param tbl        the table
   * @param desc       the table descriptor
   * @param origParams the original table properties
   */
  default void setTableParametersForCTLT(org.apache.hadoop.hive.ql.metadata.Table tbl, CreateTableLikeDesc desc,
      Map<String, String> origParams) {
  }

  /**
   * Sets tables physical location at create table as select.
   * Some storage handlers requires specifying the location of tables others generates it internally.
   */
  default void setTableLocationForCTAS(CreateTableDesc desc, String location) {
  }

  /**
   * Extract the native properties of the table which aren't stored in the HMS
   * @param table the table
   * @return map with native table level properties
   */
  default Map<String, String> getNativeProperties(org.apache.hadoop.hive.ql.metadata.Table table) {
    return new HashMap<>();
  }

  /**
   * Returns whether the data should be overwritten for the specific operation.
   * @param mTable the table.
   * @param operation operation type.
   * @return if the data should be overwritten for the specified operation.
   */
  default boolean shouldOverwrite(org.apache.hadoop.hive.ql.metadata.Table mTable, Context.Operation operation) {
    return false;
  }

  /**
   * Adds specific configurations to session for create table command.
   * @param tblProps table properties
   * @param hiveConf configuration
   */
  default void addResourcesForCreateTable(Map<String, String> tblProps, HiveConf hiveConf) {
  }

  enum AcidSupportType {
    NONE,
    WITH_TRANSACTIONS,
    WITHOUT_TRANSACTIONS
  }

  /**
   * Specifies whether the table supports ACID operations or not (DELETE, UPDATE and MERGE statements).
   *
   * Possible return values:
   * <ul>
   *   <li>AcidSupportType.NONE - ACID operations are not supported</li>
   *   <li>AcidSupportType.WITH_TRANSACTIONS - ACID operations are supported, and must use a valid HiveTxnManager to wrap
   *   the operation in a transaction, like in the case of standard Hive ACID tables</li>
   *   <li>AcidSupportType.WITHOUT_TRANSACTIONS - ACID operations are supported, and there is no need for a HiveTxnManager
   *   to open/close transactions for the operation, i.e. org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager
   *   can be used</li>
   * </ul>
   *
   * @return the table's ACID support type
   */
  default AcidSupportType supportsAcidOperations() {
    return AcidSupportType.NONE;
  }

  /**
   * Specifies which additional virtual columns should be added to the virtual column registry during compilation
   * for tables that support ACID operations.
   *
   * Should only return a non-empty list if
   * {@link HiveStorageHandler#supportsAcidOperations()} returns something
   * other NONE.
   *
   * @return the list of ACID virtual columns
   */
  default List<VirtualColumn> acidVirtualColumns() {
    return Collections.emptyList();
  }

  /**
   * {@link UpdateSemanticAnalyzer} rewrites UPDATE and
   * {@link DeleteSemanticAnalyzer} rewrites DELETE queries into INSERT queries.
   * - DELETE FROM T WHERE A = 32 is rewritten into
   * INSERT INTO T SELECT &lt;selectCols&gt; FROM T WHERE A = 32 SORT BY &lt;sortCols&gt;.
   * - UPDATE T SET B=12 WHERE A = 32 is rewritten into
   * INSERT INTO T SELECT &lt;selectCols&gt;, &lt;newValues&gt; FROM T WHERE A = 32 SORT BY &lt;sortCols&gt;.
   *
   * This method specifies which columns should be injected into the &lt;selectCols&gt; part of the rewritten query.
   *
   * Should only return a non-empty list if
   * {@link HiveStorageHandler#supportsAcidOperations()} returns something
   * other NONE.
   *
   * @param table the table which is being deleted/updated/merged into
   * @param operation the operation type we are executing
   * @return the list of columns that should be projected in the rewritten ACID query
   */
  default List<FieldSchema> acidSelectColumns(org.apache.hadoop.hive.ql.metadata.Table table, Operation operation) {
    return Collections.emptyList();
  }

  default FieldSchema getRowId() {
    throw new UnsupportedOperationException();
  }

  /**
   * {@link UpdateSemanticAnalyzer} rewrites UPDATE and
   * {@link DeleteSemanticAnalyzer} rewrites DELETE queries into INSERT
   * queries. E.g. DELETE FROM T WHERE A = 32 is rewritten into
   * INSERT INTO T SELECT &lt;selectCols&gt; FROM T WHERE A = 32 SORT BY &lt;sortCols&gt;.
   *
   * This method specifies which columns should be injected into the &lt;sortCols&gt; part of the rewritten query.
   *
   * Should only return a non-empty list if
   * {@link HiveStorageHandler#supportsAcidOperations()} returns something
   * other NONE.
   *
   * @param table the table which is being deleted/updated/merged into
   * @param operation the operation type we are executing
   * @return the list of columns that should be used as sort columns in the rewritten ACID query
   */
  default List<FieldSchema> acidSortColumns(org.apache.hadoop.hive.ql.metadata.Table table, Operation operation) {
    return Collections.emptyList();
  }

  /**
   * Check if the underlying storage handler implementation supports sort columns.
   * @return true if the storage handler can support it
   */
  default boolean supportsSortColumns() {
    return false;
  }

  /**
   * Collect the columns that are used to sort the content of the data files
   * @param table the table which is being sorted
   * @return the list of columns that are used during data sorting
   */
  default List<FieldSchema> sortColumns(org.apache.hadoop.hive.ql.metadata.Table table) {
    Preconditions.checkState(supportsSortColumns(), "Should only be called for table formats where data sorting " +
        "is supported");
    return Collections.emptyList();
  }

  /**
   * Check if the underlying storage handler implementation support partition transformations.
   * @return true if the storage handler can support it
   */
  default boolean supportsPartitionTransform() {
    return false;
  }

  /**
   * Return a list of partition transform specifications. This method should be overwritten in case
   * {@link HiveStorageHandler#supportsPartitionTransform()} returns true.
   * @param table the HMS table, must be non-null
   * @return partition transform specification, can be null.
   */
  default List<TransformSpec> getPartitionTransformSpec(org.apache.hadoop.hive.ql.metadata.Table table) {
    return null;
  }

  default Map<Integer,List<TransformSpec>> getPartitionTransformSpecs(org.apache.hadoop.hive.ql.metadata.Table table) {
    return null;
  }

  /**
   * Creates a DynamicPartitionCtx instance that will be set up by the storage handler itself. Useful for non-native
   * tables where partitions are not handled by Hive, and sorting is required in a custom way before writing the table.
   * @param conf job conf
   * @param table the HMS table
   * @return the created DP context object, null if DP context / sorting is not required
   * @throws SemanticException
   */
  default DynamicPartitionCtx createDPContext(
          HiveConf conf, org.apache.hadoop.hive.ql.metadata.Table table, Operation writeOperation)
      throws SemanticException {
    Preconditions.checkState(supportsPartitioning(), "Should only be called for table formats where partitioning " +
        "is not handled by Hive but the table format itself. See supportsPartitioning() method.");
    return null;
  }

  /**
   * Checks whether the table supports Partition-Aware Optimization.
   * @param table the table
   * @return true if the table has partition definitions which potentially optimize query plans
   * @throws SemanticException in case of any error.
   */
  default boolean supportsPartitionAwareOptimization(org.apache.hadoop.hive.ql.metadata.Table table)
      throws SemanticException {
    return false;
  }

  /**
   * Creates a PartitionAwareOptimizationCtx that retains partition definitions applicable to query plan optimization.
   * Hive can apply special optimization such as Bucket Map Join based on the storage layouts provided by this context.
   * @param table the table
   * @return the created PartitionAwareOptimizationCtx, null if this table doesn't support Partition-Aware Optimization
   * @throws SemanticException in case of any error.
   */
  default PartitionAwareOptimizationCtx createPartitionAwareOptimizationContext(
      org.apache.hadoop.hive.ql.metadata.Table table) throws SemanticException {
    return null;
  }

  /**
   * Get file format property key, if the file format is configured through a table property.
   * @return table property key, can be null
   */
  default String getFileFormatPropertyKey() {
    return null;
  }

  /**
   * Checks if we should keep the {@link org.apache.hadoop.hive.ql.exec.MoveTask} and use the
   * {@link #storageHandlerCommit(Properties, boolean)} method for committing inserts instead of
   * {@link org.apache.hadoop.hive.metastore.DefaultHiveMetaHook#commitInsertTable(Table, boolean)}.
   * @return Returns true if we should use the {@link #storageHandlerCommit(Properties, boolean)} method
   */
  default boolean commitInMoveTask() {
    return false;
  }

  /**
   * Commits the inserts for the non-native tables. Used in the {@link org.apache.hadoop.hive.ql.exec.MoveTask}.
   * @param commitProperties Commit properties which are needed for the handler based commit
   * @param operation the operation type
   * @throws HiveException If there is an error during commit
   */
  default void storageHandlerCommit(Properties commitProperties, Operation operation)
      throws HiveException {
    storageHandlerCommit(commitProperties, operation, null);
  }

  /**
   * Commits the inserts for the non-native tables. Used in the {@link org.apache.hadoop.hive.ql.exec.MoveTask}.
   * @param commitProperties Commit properties which are needed for the handler based commit
   * @param operation the operation type
   * @param executorService an ExecutorService to be used by the StorageHandler (optional)
   * @throws HiveException If there is an error during commit
   */
  default void storageHandlerCommit(Properties commitProperties, Operation operation, ExecutorService executorService)
      throws HiveException {
    throw new UnsupportedOperationException();
  }

  /**
   * Commits the inserts for the non-native tables.
   * @param commitProperties Commit properties which are needed for the handler based commit
   *
   * @deprecated since 4.0.1, will be removed in 5.0.0,
   * use {@link #storageHandlerCommit(Properties, Operation)} instead.
   */
  @Deprecated
  default void storageHandlerCommit(Properties commitProperties, boolean overwrite) throws HiveException {
    storageHandlerCommit(commitProperties, overwrite ? Operation.IOW : Operation.OTHER);
  }

  /**
   * Checks whether a certain ALTER TABLE operation is supported by the storage handler implementation.
   *
   * @param opType The alter operation type (e.g. RENAME_COLUMNS)
   * @return whether the operation is supported by the storage handler
   */
  default boolean isAllowedAlterOperation(AlterTableType opType) {
    return DEFAULT_ALLOWED_ALTER_OPS.contains(opType);
  }

  /**
   * Returns an OutputCommitter that belongs to this storage handler if any.
   */
  default OutputCommitter getOutputCommitter() {
    return null;
  }

  /**
   * Check if the underlying storage handler implementation supports truncate operation
   * for non native tables.
   * @return true if the storage handler can support it
   * @return
   */
  default boolean supportsTruncateOnNonNativeTables() {
    return false;
  }

  /**
   * Should return true if the StorageHandler is able to handle time travel.
   * @return True if time travel is allowed
   */
  default boolean isTimeTravelAllowed() {
    return false;
  }

  /**
   * Introduced by HIVE-25457 for iceberg to query metadata table.
   * @return true if the storage handler can support it
   *
   * @deprecated since 4.0.1, will be removed in 5.0.0,
   * use {@link #isTableMetaRefSupported()} instead.
   */
  @Deprecated
  default boolean isMetadataTableSupported() {
    return isTableMetaRefSupported();
  }

  /**
   * Check whether the table supports metadata references which mainly include branch, tag and metadata tables.
   * @return true if the storage handler can support it
   */
  default boolean isTableMetaRefSupported() {
    return false;
  }

  default boolean isValidMetadataTable(String metaTableName) {
    return false;
  }

  default org.apache.hadoop.hive.ql.metadata.Table checkAndSetTableMetaRef(
      org.apache.hadoop.hive.ql.metadata.Table hmsTable, String tableMetaRef) throws SemanticException {
    return null;
  }

  /**
   * Constructs a URI for authorization purposes using the HMS table object
   * @param table The HMS table object
   * @return the URI for authorization
   */
  default URI getURIForAuth(Table table) throws URISyntaxException {
    Map<String, String> tableProperties = HiveCustomStorageHandlerUtils.getTableProperties(table);
    return new URI(this.getClass().getSimpleName().toLowerCase() + "://" +
        HiveCustomStorageHandlerUtils.getTablePropsForCustomStorageHandler(tableProperties));
  }

  /**
   * Validates whether the sink operation is permitted for the specific storage handler, based
   * on information contained in the sinkDesc.
   * @param sinkDesc The sink descriptor
   * @throws SemanticException if the sink operation is not allowed
   */
  default void validateSinkDesc(FileSinkDesc sinkDesc) throws SemanticException {
  }

  /**
   * Execute an operation on storage handler level
   * @param executeSpec operation specification
   */
  default void executeOperation(org.apache.hadoop.hive.ql.metadata.Table table, AlterTableExecuteSpec executeSpec) {
  }

  default void alterTableSnapshotRefOperation(org.apache.hadoop.hive.ql.metadata.Table table,
      AlterTableSnapshotRefSpec alterTableSnapshotRefSpec) {
  }

  /**
   * Gets whether this storage handler supports snapshots.
   * @return true means snapshots are supported false otherwise
   */
  default boolean areSnapshotsSupported() {
    return false;
  }

  /**
   * Gets whether this storage handler supports row lineage.
   * @return true means row lineage is supported false otherwise
   */
  default boolean supportsRowLineage(Map<String, String> table) {
    return false;
  }

  /**
   * Query the most recent unique snapshot's context of the passed table.
   * @param table - {@link org.apache.hadoop.hive.ql.metadata.Table} which snapshot context should be returned.
   * @return {@link SnapshotContext} wraps the snapshotId or null if no snapshot present.
   */
  default SnapshotContext getCurrentSnapshotContext(org.apache.hadoop.hive.ql.metadata.Table table) {
    return null;
  }
  
  default void validateCurrentSnapshot(TableDesc tableDesc) {
  }

  /**
   * Return snapshot metadata of table snapshots which are newer than the specified.
   * The specified snapshot is excluded.
   * @param hmsTable table metadata stored in Hive Metastore
   * @param since the snapshot preceding the oldest snapshot which should be checked.
   *              The value null means all should be checked.
   * @return Iterable of {@link SnapshotContext}.
   */
  default Iterable<SnapshotContext> getSnapshotContexts(
      org.apache.hadoop.hive.ql.metadata.Table hmsTable, SnapshotContext since) {
    return Collections.emptyList();
  }

  /**
   * Alter table operations can rely on this to customize the EnvironmentContext to be used during the alter table
   * invocation (both on client and server side of HMS)
   * @param alterTableDesc the alter table desc (e.g.: AlterTableSetPropertiesDesc) containing the work to do
   * @param environmentContext an existing EnvironmentContext created prior, now to be filled/amended
   */
  default void prepareAlterTableEnvironmentContext(AbstractAlterTableDesc alterTableDesc,
      EnvironmentContext environmentContext) {
  }

  /**
   * Check the operation type of all snapshots which are newer than the specified. The specified snapshot is excluded.
   *
   * @param hmsTable table metadata stored in Hive Metastore
   * @param since the snapshot preceding the oldest snapshot which should be checked.
   *              The value null means all should be checked.
   * @return null if table is empty, true if all snapshots are {@link SnapshotContext.WriteOperationType#APPEND}s,
   * false otherwise.
   *
   * @deprecated since 4.0.1, will be removed in 5.0.0,
   * use {@link HiveStorageHandler#getSnapshotContexts(org.apache.hadoop.hive.ql.metadata.Table, SnapshotContext)} instead
   * and check {@link SnapshotContext.WriteOperationType#APPEND}.equals({@link SnapshotContext#getOperation()})
   */
  @Deprecated
  default Boolean hasAppendsOnly(org.apache.hadoop.hive.ql.metadata.Table hmsTable, SnapshotContext since) {
    return null;
  }

  /**
   * Returns partitions names for the table.
   *
   * @deprecated since 4.0.1, will be removed in 5.0.0,
   * use {@link #getPartitionNames(org.apache.hadoop.hive.ql.metadata.Table)} instead.
   */
  @Deprecated
  default List<String> showPartitions(DDLOperationContext context,
      org.apache.hadoop.hive.ql.metadata.Table tbl) throws UnsupportedOperationException, HiveException {
    return getPartitionNames(tbl);
  }

  /**
   * Validates that the provided partitionSpec is a valid according to the current table partitioning.
   * @param hmsTable {@link org.apache.hadoop.hive.ql.metadata.Table} table metadata stored in Hive Metastore
   * @param partitionSpec Map of Strings {@link java.util.Map} partition specification
   */
  default void validatePartSpec(org.apache.hadoop.hive.ql.metadata.Table hmsTable, Map<String, String> partitionSpec)
      throws SemanticException {
    validatePartSpec(hmsTable, partitionSpec, Context.RewritePolicy.DEFAULT);
  }

  /**
   * Validates that the provided partitionSpec is a valid according to the current table partitioning.
   * @param hmsTable {@link org.apache.hadoop.hive.ql.metadata.Table} table metadata stored in Hive Metastore
   * @param partitionSpec Map of Strings {@link java.util.Map} partition specification
   * @param policy {@link org.apache.hadoop.hive.ql.Context.RewritePolicy} compaction rewrite policy
   */
  default void validatePartSpec(org.apache.hadoop.hive.ql.metadata.Table hmsTable, Map<String, String> partitionSpec,
      Context.RewritePolicy policy) throws SemanticException {
    throw new UnsupportedOperationException("Storage handler does not support validation of partition values");
  }

  default boolean canUseTruncate(org.apache.hadoop.hive.ql.metadata.Table hmsTable, Map<String, String> partitionSpec)
      throws SemanticException {
    return true;
  }

  /**
   * Returns partitions names for the current table spec that correspond to the provided partition spec.
   * @param table {@link org.apache.hadoop.hive.ql.metadata.Table} table metadata stored in Hive Metastore
   * @param partitionSpec Map of Strings {@link java.util.Map} partition specification
   * @return List of partition names
   */
  default List<String> getPartitionNames(org.apache.hadoop.hive.ql.metadata.Table table,
      Map<String, String> partitionSpec) throws SemanticException {
    throw new UnsupportedOperationException("Storage handler does not support getting partition names");
  }

  /**
   * Returns partitions names for the current table spec.
   * @param table {@link org.apache.hadoop.hive.ql.metadata.Table} table metadata stored in Hive Metastore
   * @return List of partition names
   */
  default List<String> getPartitionNames(org.apache.hadoop.hive.ql.metadata.Table table) throws SemanticException {
    return getPartitionNames(table, Maps.newHashMap());
  }
  
  default ColumnInfo getColumnInfo(org.apache.hadoop.hive.ql.metadata.Table hmsTable, String colName)
      throws SemanticException {
    throw new UnsupportedOperationException("Storage handler does not support getting column type " +
            "for a specific column.");
  }

  default boolean canPerformMetadataDelete(org.apache.hadoop.hive.ql.metadata.Table hmsTable, String branchName,
    SearchArgument searchArgument) {
    return false;
  }

  default List<FieldSchema> getPartitionKeys(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    throw new UnsupportedOperationException("Storage handler does not support getting partition keys for a table.");
  }

  /**
   * Returns a list of partitions which contain any files whose content falls under the provided filter condition.
   * @param table {@link org.apache.hadoop.hive.ql.metadata.Table} table metadata stored in Hive Metastore
   * @param filter Iceberg filter expression
   * @param latestSpecOnly when True, returns partitions with the current spec only; 
   *                       False - older specs only; 
   *                       Null - any spec
   * @return List of Partitions {@link org.apache.hadoop.hive.ql.metadata.Partition}
   */
  default List<Partition> getPartitionsByExpr(org.apache.hadoop.hive.ql.metadata.Table table,
        ExprNodeDesc filter, Boolean latestSpecOnly) throws SemanticException {
    throw new UnsupportedOperationException("Storage handler does not support getting partitions " +
        "by generic expressions");
  }

  default List<Partition> getPartitionsByExpr(org.apache.hadoop.hive.ql.metadata.Table table, ExprNodeDesc filter)
        throws SemanticException {
    return getPartitionsByExpr(table, filter, null);
  }

  /**
   * Returns partition based on table and partition specification.
   * @param table {@link org.apache.hadoop.hive.ql.metadata.Table} table metadata stored in Hive Metastore
   * @param partitionSpec Map of Strings {@link java.util.Map} partition specification
   * @return Partition {@link org.apache.hadoop.hive.ql.metadata.Partition} 
   * @throws SemanticException {@link org.apache.hadoop.hive.ql.parse.SemanticException} 
   */
  default Partition getPartition(org.apache.hadoop.hive.ql.metadata.Table table, Map<String, String> partitionSpec)
        throws SemanticException {
    return getPartition(table, partitionSpec, Context.RewritePolicy.DEFAULT);
  }

  /**
   * Returns partition based on table and partition specification.
   * @param table {@link org.apache.hadoop.hive.ql.metadata.Table} table metadata stored in Hive Metastore
   * @param partitionSpec Map of Strings {@link java.util.Map} partition specification
   * @param policy {@link org.apache.hadoop.hive.ql.Context.RewritePolicy} compaction rewrite policy
   * @return Partition {@link org.apache.hadoop.hive.ql.metadata.Partition}
   * @throws SemanticException {@link org.apache.hadoop.hive.ql.parse.SemanticException}
   */
  default Partition getPartition(org.apache.hadoop.hive.ql.metadata.Table table, Map<String, String> partitionSpec,
        Context.RewritePolicy policy) throws SemanticException {
    throw new UnsupportedOperationException("Storage handler does not support getting partition for a table.");
  }

  /**
   * Returns a list of partitions based on table and partial partition specification.
   * @param table {@link org.apache.hadoop.hive.ql.metadata.Table} table metadata stored in Hive Metastore
   * @param partitionSpec Map of Strings {@link java.util.Map} partition specification
   * @param latestSpecOnly when True, returns partitions with the current spec only, else - any spec
   * @return List of Partitions {@link org.apache.hadoop.hive.ql.metadata.Partition}
   * @throws SemanticException {@link org.apache.hadoop.hive.ql.parse.SemanticException}
   */
  default List<Partition> getPartitions(org.apache.hadoop.hive.ql.metadata.Table table,
        Map<String, String> partitionSpec, boolean latestSpecOnly) throws SemanticException {
    throw new UnsupportedOperationException("Storage handler does not support getting partitions for a table.");
  }

  default List<Partition> getPartitions(org.apache.hadoop.hive.ql.metadata.Table table,
        Map<String, String> partitionSpec) throws SemanticException {
    return getPartitions(table, partitionSpec, false);
  }

  default List<Partition> getPartitions(org.apache.hadoop.hive.ql.metadata.Table table) throws SemanticException {
    return getPartitions(table, Collections.emptyMap());
  }

  default boolean isPartitioned(org.apache.hadoop.hive.ql.metadata.Table table) {
    return false;
  }

  default boolean hasUndergonePartitionEvolution(org.apache.hadoop.hive.ql.metadata.Table table) {
    throw new UnsupportedOperationException("Storage handler does not support checking if table " +
        "undergone partition evolution.");
  }

  /**
   * Returns true if the table contain any records matching the filter expression, false otherwise
   * @param hmsTable {@link org.apache.hadoop.hive.ql.metadata.Table} table metadata stored in Hive Metastore
   * @param filter Iceberg filter expression
   * @return boolean 
   */
  default boolean hasDataMatchingFilterExpr(org.apache.hadoop.hive.ql.metadata.Table hmsTable, ExprNodeDesc filter) {
    throw new UnsupportedOperationException("Storage handler does not support checking if filter has any match");
  }

  default boolean supportsMergeFiles() {
    return false;
  }

  default List<FileStatus> getMergeTaskInputFiles(Properties properties) throws IOException {
    throw new UnsupportedOperationException("Storage handler does not support getting merge input files " +
            "for a table.");
  }

  default MergeTaskProperties getMergeTaskProperties(Properties properties) {
    throw new UnsupportedOperationException("Storage handler does not support getting merge input files " +
            "for a table.");
  }

  default void setMergeTaskDeleteProperties(TableDesc tableDesc) {
    throw new UnsupportedOperationException("Storage handler does not support getting custom delete merge schema.");
  }

  default boolean supportsDefaultColumnValues(Map<String, String> tblProps) {
    return false;
  }
}
