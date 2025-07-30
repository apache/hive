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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.SnapshotContext;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Context.Operation;
import org.apache.hadoop.hive.ql.Context.RewritePolicy;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.create.like.CreateTableLikeDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.properties.AlterTableSetPropertiesDesc;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.StorageFormatDescriptor;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.AlterTableExecuteSpec;
import org.apache.hadoop.hive.ql.parse.AlterTableSnapshotRefSpec;
import org.apache.hadoop.hive.ql.parse.PartitionTransform;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.StorageFormat;
import org.apache.hadoop.hive.ql.parse.StorageFormat.StorageHandlerTypes;
import org.apache.hadoop.hive.ql.parse.TransformSpec;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hadoop.hive.ql.plan.MergeTaskProperties;
import org.apache.hadoop.hive.ql.plan.PartitionAwareOptimizationCtx;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveCustomStorageHandlerUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.hadoop.hive.ql.stats.Partish;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.FindFiles;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.data.PartitionStatsHandler;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.hive.HiveTableOperations;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.hive.actions.HiveIcebergDeleteOrphanFiles;
import org.apache.iceberg.mr.hive.plan.IcebergBucketFunction;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinReader;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.FluentIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.SerializationUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.ql.metadata.VirtualColumn.FILE_PATH;
import static org.apache.hadoop.hive.ql.metadata.VirtualColumn.PARTITION_HASH;
import static org.apache.hadoop.hive.ql.metadata.VirtualColumn.PARTITION_PROJECTION;
import static org.apache.hadoop.hive.ql.metadata.VirtualColumn.PARTITION_SPEC_ID;
import static org.apache.hadoop.hive.ql.metadata.VirtualColumn.ROW_POSITION;
import static org.apache.iceberg.SnapshotSummary.ADDED_RECORDS_PROP;
import static org.apache.iceberg.SnapshotSummary.DELETED_RECORDS_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DATA_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_EQ_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_FILE_SIZE_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_POS_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;
import static org.apache.iceberg.TableProperties.DELETE_MODE;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.MERGE_MODE;
import static org.apache.iceberg.TableProperties.UPDATE_MODE;

public class HiveIcebergStorageHandler implements HiveStoragePredicateHandler, HiveStorageHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergStorageHandler.class);

  private static final String ICEBERG_URI_PREFIX = "iceberg://";
  private static final Splitter TABLE_NAME_SPLITTER = Splitter.on("..");
  private static final String TABLE_NAME_SEPARATOR = "..";
  // Column index for partition metadata table
  public static final String COPY_ON_WRITE = "copy-on-write";
  public static final String MERGE_ON_READ = "merge-on-read";
  public static final String STATS = "/stats/snap-";

  public static final String TABLE_DEFAULT_LOCATION = "TABLE_DEFAULT_LOCATION";

  private static final List<VirtualColumn> ACID_VIRTUAL_COLS = ImmutableList.of(
      PARTITION_SPEC_ID, PARTITION_HASH, FILE_PATH, ROW_POSITION, PARTITION_PROJECTION);

  private static final List<FieldSchema> ACID_VIRTUAL_COLS_AS_FIELD_SCHEMA = schema(ACID_VIRTUAL_COLS);

  private static final List<FieldSchema> POSITION_DELETE_ORDERING =
      orderBy(PARTITION_SPEC_ID, PARTITION_HASH, FILE_PATH, ROW_POSITION);

  private static final List<FieldSchema> EMPTY_ORDERING = ImmutableList.of();

  private Configuration conf;

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveIcebergInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveIcebergOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return HiveIcebergSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    // Make sure to always return a new instance here, as HiveIcebergMetaHook might hold state relevant for the
    // operation.
    return new HiveIcebergMetaHook(conf);
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() {
    return null;
  }

  @Override
  public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> map) {
    overlayTableProperties(conf, tableDesc, map);
    // Until the vectorized reader can handle delete files, let's fall back to non-vector mode for V2 tables
    fallbackToNonVectorizedModeBasedOnProperties(tableDesc.getProperties());

    boolean allowDataFilesWithinTableLocationOnly =
        conf.getBoolean(ConfVars.HIVE_ICEBERG_ALLOW_DATAFILES_IN_TABLE_LOCATION_ONLY.varname,
            ConfVars.HIVE_ICEBERG_ALLOW_DATAFILES_IN_TABLE_LOCATION_ONLY.defaultBoolVal);

    map.put(ConfVars.HIVE_ICEBERG_ALLOW_DATAFILES_IN_TABLE_LOCATION_ONLY.varname,
        String.valueOf(allowDataFilesWithinTableLocationOnly));
  }

  @Override
  public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> map) {
    overlayTableProperties(conf, tableDesc, map);
    // Until the vectorized reader can handle delete files, let's fall back to non-vector mode for V2 tables
    fallbackToNonVectorizedModeBasedOnProperties(tableDesc.getProperties());
    // For Tez, setting the committer here is enough to make sure it'll be part of the jobConf
    map.put("mapred.output.committer.class", HiveIcebergNoJobCommitter.class.getName());
    // For MR, the jobConf is set only in configureJobConf, so we're setting the write key here to detect it over there
    String opType = getOperationType();
    map.put(InputFormatConfig.OPERATION_TYPE_PREFIX + tableDesc.getTableName(), opType);
    // Putting the key into the table props as well, so that projection pushdown can be determined on a
    // table-level and skipped only for output tables in HiveIcebergSerde. Properties from the map will be present in
    // the serde config for all tables in the query, not just the output tables, so we can't rely on that in the serde.
    tableDesc.getProperties().put(InputFormatConfig.OPERATION_TYPE_PREFIX + tableDesc.getTableName(), opType);
  }

  /**
   * Committer with no-op job commit. We can pass this into the Tez AM to take care of task commits/aborts, as well
   * as aborting jobs reliably if an execution error occurred. However, we want to execute job commits on the
   * HS2-side during the MoveTask, so we will use the full-featured HiveIcebergOutputCommitter there.
   */
  static class HiveIcebergNoJobCommitter extends HiveIcebergOutputCommitter {
    @Override
    public void commitJob(JobContext originalContext) {
      // do nothing
    }
  }

  @Override
  public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> map) {

  }

  // Override annotation commented out, since this interface method has been introduced only in Hive 3
  // @Override
  public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> secrets) {

  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    setCommonJobConf(jobConf);
    if (tableDesc != null && tableDesc.getProperties() != null &&
        tableDesc.getProperties().get(InputFormatConfig.OPERATION_TYPE_PREFIX + tableDesc.getTableName()) != null) {
      String tableName = tableDesc.getTableName();
      String opKey = InputFormatConfig.OPERATION_TYPE_PREFIX + tableName;
      // set operation type into job conf too
      jobConf.set(opKey, tableDesc.getProperties().getProperty(opKey));
      Preconditions.checkArgument(!tableName.contains(TABLE_NAME_SEPARATOR),
          "Can not handle table " + tableName + ". Its name contains '" + TABLE_NAME_SEPARATOR + "'");
      if (HiveCustomStorageHandlerUtils.getWriteOperation(tableDesc.getProperties()::getProperty, tableName) != null) {
        HiveCustomStorageHandlerUtils.setWriteOperation(jobConf, tableName,
            Operation.valueOf(tableDesc.getProperties().getProperty(
                HiveCustomStorageHandlerUtils.WRITE_OPERATION_CONFIG_PREFIX + tableName)));
      }
      boolean isMergeTaskEnabled = Boolean.parseBoolean(tableDesc.getProperty(
              HiveCustomStorageHandlerUtils.MERGE_TASK_ENABLED + tableName));
      if (isMergeTaskEnabled) {
        HiveCustomStorageHandlerUtils.setMergeTaskEnabled(jobConf, tableName, true);
      }
      String tables = jobConf.get(InputFormatConfig.OUTPUT_TABLES);
      tables = tables == null ? tableName : tables + TABLE_NAME_SEPARATOR + tableName;
      jobConf.set(InputFormatConfig.OUTPUT_TABLES, tables);

      String catalogName = tableDesc.getProperties().getProperty(InputFormatConfig.CATALOG_NAME);
      if (catalogName != null) {
        jobConf.set(InputFormatConfig.TABLE_CATALOG_PREFIX + tableName, catalogName);
      }
    }
    try {
      if (!jobConf.getBoolean(ConfVars.HIVE_IN_TEST_IDE.varname, false)) {
        // For running unit test this won't work as maven surefire CP is different than what we have on a cluster:
        // it places the current projects' classes and test-classes to top instead of jars made from these...
        Utilities.addDependencyJars(jobConf, HiveIcebergStorageHandler.class);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean directInsert() {
    return true;
  }

  @Override
  public boolean supportsPartitioning() {
    return true;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public String toString() {
    return this.getClass().getName();
  }

  /**
   * @param jobConf Job configuration for InputFormat to access
   * @param deserializer Deserializer
   * @param exprNodeDesc Filter expression extracted by Hive
   * @return Entire filter to take advantage of Hive's pruning as well as Iceberg's pruning.
   */
  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc exprNodeDesc) {
    DecomposedPredicate predicate = new DecomposedPredicate();
    predicate.residualPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    ExprNodeDesc pushedPredicate = exprNodeDesc.clone();

    List<ExprNodeDesc> subExprNodes = pushedPredicate.getChildren();
    Set<String> skipList = Stream.of(FILE_PATH, PARTITION_SPEC_ID, PARTITION_HASH)
        .map(VirtualColumn::getName).collect(Collectors.toSet());

    if (subExprNodes.removeIf(nodeDesc -> nodeDesc.getCols() != null &&
          nodeDesc.getCols().stream().anyMatch(skipList::contains))) {
      if (subExprNodes.size() == 1) {
        pushedPredicate = (subExprNodes.getFirst() instanceof ExprNodeGenericFuncDesc) ?
            subExprNodes.getFirst() : null;
      } else if (subExprNodes.isEmpty()) {
        pushedPredicate = null;
      }
    }
    predicate.pushedPredicate = (ExprNodeGenericFuncDesc) pushedPredicate;

    if (pushedPredicate != null) {
      SessionStateUtil.setConflictDetectionFilter(conf, jobConf.get(Catalogs.NAME), pushedPredicate);
    }
    return predicate;
  }


  @Override
  public boolean canProvideBasicStatistics() {
    return true;
  }

  @Override
  public boolean canProvidePartitionStatistics(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    if (!getStatsSource().equals(HiveMetaHook.ICEBERG)) {
      return false;
    }
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    Snapshot snapshot = IcebergTableUtil.getTableSnapshot(table, hmsTable);
    if (snapshot != null) {
      return IcebergTableUtil.getPartitionStatsFile(table, snapshot.snapshotId()) != null;
    }
    return false;
  }

  @Override
  public StorageFormatDescriptor getStorageFormatDescriptor(org.apache.hadoop.hive.metastore.api.Table table)
      throws SemanticException {
    if (table.getParameters() != null) {
      String format = table.getParameters().getOrDefault(TableProperties.DEFAULT_FILE_FORMAT, IOConstants.PARQUET);
      return StorageFormat.getDescriptor(format, TableProperties.DEFAULT_FILE_FORMAT);
    }
    return null;
  }

  public boolean supportsAppendData(org.apache.hadoop.hive.metastore.api.Table table, boolean withPartClause)
      throws SemanticException {
    Table icebergTbl = IcebergTableUtil.getTable(conf, table);
    if (icebergTbl.spec().isUnpartitioned()) {
      return true;
    }
    // If it is a table which has undergone partition evolution, return false;
    if (IcebergTableUtil.hasUndergonePartitionEvolution(icebergTbl)) {
      if (withPartClause) {
        throw new SemanticException("Can not Load into an iceberg table, which has undergone partition evolution " +
            "using the PARTITION clause");
      }
      return false;
    }
    return withPartClause;
  }

  public void appendFiles(org.apache.hadoop.hive.metastore.api.Table table, URI fromURI, boolean isOverwrite,
      Map<String, String> partitionSpec)
      throws SemanticException {
    Table icebergTbl = IcebergTableUtil.getTable(conf, table);
    String format = table.getParameters().get(TableProperties.DEFAULT_FILE_FORMAT);
    HiveTableUtil.appendFiles(fromURI, format, icebergTbl, isOverwrite, partitionSpec, conf);
  }

  @Override
  public Map<String, String> getBasicStatistics(Partish partish) {
    return getBasicStatistics(partish, false);
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private Map<String, String> getBasicStatistics(Partish partish, boolean quickStats) {
    Map<String, String> stats = Maps.newHashMap();

    org.apache.hadoop.hive.ql.metadata.Table hmsTable = partish.getTable();
    // For write queries where rows got modified, don't fetch from cache as values could have changed.
    Table table = getTable(hmsTable);
    Snapshot snapshot = IcebergTableUtil.getTableSnapshot(table, hmsTable);

    if (snapshot == null) {
      stats.put(StatsSetupConst.NUM_FILES, "0");
      stats.put(StatsSetupConst.ROW_COUNT, "0");
      stats.put(StatsSetupConst.TOTAL_SIZE, "0");

    } else if (!HiveMetaHook.ICEBERG.equals(getStatsSource()) && !quickStats) {
      stats = partish.getPartParameters();

    } else {
      Map<String, String> summary = getPartishSummary(partish, table, snapshot);
      if (summary != null) {
        if (summary.containsKey(TOTAL_DATA_FILES_PROP)) {
          stats.put(StatsSetupConst.NUM_FILES, summary.get(TOTAL_DATA_FILES_PROP));
        }
        if (summary.containsKey(TOTAL_RECORDS_PROP) && !quickStats) {
          long totalRecords = Long.parseLong(summary.get(TOTAL_RECORDS_PROP));
          if (summary.containsKey(TOTAL_EQ_DELETES_PROP) &&
              summary.containsKey(TOTAL_POS_DELETES_PROP)) {

            long totalEqDeletes = Long.parseLong(summary.get(TOTAL_EQ_DELETES_PROP));
            long totalPosDeletes = Long.parseLong(summary.get(TOTAL_POS_DELETES_PROP));

            long actualRecords = totalRecords - (totalEqDeletes > 0 ? 0 : totalPosDeletes);
            totalRecords = actualRecords > 0 ? actualRecords : totalRecords;
            // actualRecords maybe -ve in edge cases
          }
          stats.put(StatsSetupConst.ROW_COUNT, String.valueOf(totalRecords));
        }
        if (summary.containsKey(TOTAL_FILE_SIZE_PROP)) {
          stats.put(StatsSetupConst.TOTAL_SIZE, summary.get(TOTAL_FILE_SIZE_PROP));
        }
      }
    }
    return stats;
  }

  @Override
  public Map<String, String> computeBasicStatistics(Partish partish) {
    Map<String, String> stats;
    if (!getStatsSource().equals(HiveMetaHook.ICEBERG)) {
      stats = partish.getPartParameters();

      if (!StatsSetupConst.areBasicStatsUptoDate(stats)) {
        // populate quick-stats
        stats = getBasicStatistics(partish, true);
      }
      return stats;
    }
    org.apache.hadoop.hive.ql.metadata.Table hmsTable = partish.getTable();
    // For write queries where rows got modified, don't fetch from cache as values could have changed.
    Table table = getTable(hmsTable);
    Snapshot snapshot = IcebergTableUtil.getTableSnapshot(table, hmsTable);

    if (snapshot != null && table.spec().isPartitioned()) {
      PartitionStatisticsFile statsFile = IcebergTableUtil.getPartitionStatsFile(table, snapshot.snapshotId());
      if (statsFile == null) {
        try {
          statsFile = PartitionStatsHandler.computeAndWriteStatsFile(table);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }

        table.updatePartitionStatistics()
            .setPartitionStatistics(statsFile)
            .commit();
      }
    }
    return getBasicStatistics(partish);
  }

  private static Map<String, String> getPartishSummary(Partish partish, Table table, Snapshot snapshot) {
    if (partish.getPartition() != null) {
      PartitionStatisticsFile statsFile = IcebergTableUtil.getPartitionStatsFile(table, snapshot.snapshotId());
      if (statsFile != null) {
        Types.StructType partitionType = Partitioning.partitionType(table);
        Schema recordSchema = PartitionStatsHandler.schema(partitionType);

        try (CloseableIterable<PartitionStats> recordIterator = PartitionStatsHandler.readPartitionStatsFile(
            recordSchema, table.io().newInputFile(statsFile.path()))) {
          PartitionStats partitionStats = Iterables.tryFind(recordIterator, stats -> {
            PartitionSpec spec = table.specs().get(stats.specId());
            PartitionData data  = IcebergTableUtil.toPartitionData(stats.partition(), partitionType,
                spec.partitionType());
            return spec.partitionToPath(data).equals(partish.getPartition().getName());
          }).orNull();

          if (partitionStats != null) {
            Map<String, String> stats = ImmutableMap.of(
                TOTAL_DATA_FILES_PROP, String.valueOf(partitionStats.dataFileCount()),
                TOTAL_RECORDS_PROP, String.valueOf(partitionStats.dataRecordCount()),
                TOTAL_EQ_DELETES_PROP, String.valueOf(partitionStats.equalityDeleteRecordCount()),
                TOTAL_POS_DELETES_PROP, String.valueOf(partitionStats.positionDeleteRecordCount()),
                TOTAL_FILE_SIZE_PROP, String.valueOf(partitionStats.totalDataFileSizeInBytes())
            );
            return stats;
          } else {
            LOG.warn("Partition {} not found in stats file: {}",
                partish.getPartition().getName(), statsFile.path());
            return null;
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      } else {
        LOG.warn("Partition stats file not found for snapshot: {}", snapshot.snapshotId());
        return null;
      }
    }
    return snapshot.summary();
  }

  private Table getTable(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    boolean skipCache = SessionStateUtil.getQueryState(conf)
        .map(queryState -> queryState.getNumModifiedRows() > 0)
        .orElse(true);
    return IcebergTableUtil.getTable(conf, hmsTable.getTTable(), skipCache);
  }

  @Override
  public boolean canSetColStatistics(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    return HiveMetaHook.ICEBERG.equals(getStatsSource());
  }

  @Override
  public boolean setColStatistics(org.apache.hadoop.hive.ql.metadata.Table hmsTable, List<ColumnStatistics> colStats) {
    Table tbl = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    return writeColStats(colStats, tbl);
  }

  private boolean writeColStats(List<ColumnStatistics> colStats, Table tbl) {
    try {
      if (!shouldRewriteColStats(tbl)) {
        checkAndMergeColStats(colStats, tbl);
      }
      StatisticsFile statisticsFile;
      String statsPath = tbl.location() + STATS + UUID.randomUUID();

      try (PuffinWriter puffinWriter = Puffin.write(tbl.io().newOutputFile(statsPath))
          .createdBy(Constants.HIVE_ENGINE).build()) {
        long snapshotId = tbl.currentSnapshot().snapshotId();
        long snapshotSequenceNumber = tbl.currentSnapshot().sequenceNumber();

        colStats.forEach(statsObj -> {
          byte[] serializeColStats = SerializationUtils.serialize(statsObj);
          puffinWriter.add(
            new Blob(
              ColumnStatisticsObj.class.getSimpleName(),
              ImmutableList.of(1),
              snapshotId,
              snapshotSequenceNumber,
              ByteBuffer.wrap(serializeColStats),
              PuffinCompressionCodec.NONE,
              ImmutableMap.of("partition",
                  String.valueOf(statsObj.getStatsDesc().getPartName()))
            ));
        });
        puffinWriter.finish();

        statisticsFile =
            new GenericStatisticsFile(
                snapshotId,
                statsPath,
                puffinWriter.fileSize(),
                puffinWriter.footerSize(),
                puffinWriter.writtenBlobsMetadata().stream()
                    .map(GenericBlobMetadata::from)
                    .collect(ImmutableList.toImmutableList())
            );
      } catch (IOException e) {
        LOG.warn("Unable to write stats to puffin file {}", e.getMessage());
        return false;
      }
      tbl.updateStatistics()
          .setStatistics(statisticsFile)
          .commit();
      return true;

    } catch (Exception e) {
      LOG.warn("Unable to invalidate or merge stats: {}", e.getMessage());
    }
    return false;
  }

  @Override
  public boolean canProvideColStatistics(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    Snapshot snapshot = IcebergTableUtil.getTableSnapshot(table, hmsTable);
    if (snapshot != null) {
      return canSetColStatistics(hmsTable) && canProvideColStats(table, snapshot.snapshotId());
    }
    return false;
  }

  private boolean canProvideColStats(Table table, long snapshotId) {
    return IcebergTableUtil.getColStatsPath(table, snapshotId).isPresent();
  }

  @Override
  public List<ColumnStatisticsObj> getColStatistics(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    Snapshot snapshot = IcebergTableUtil.getTableSnapshot(table, hmsTable);

    ColumnStatistics emptyStats = new ColumnStatistics();
    if (snapshot != null) {
      return IcebergTableUtil.getColStatsPath(table, snapshot.snapshotId())
        .map(statsPath -> readColStats(table, statsPath, null).getFirst())
        .orElse(emptyStats).getStatsObj();
    }
    return emptyStats.getStatsObj();
  }

  @Override
  public AggrStats getAggrColStatsFor(org.apache.hadoop.hive.ql.metadata.Table hmsTable, List<String> colNames,
        List<String> partNames) throws MetaException {
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());

    Snapshot snapshot = IcebergTableUtil.getTableSnapshot(table, hmsTable);
    if (snapshot == null) {
      return new AggrStats(Collections.emptyList(), 0);
    }

    boolean useDensityFunctionForNDVEstimation = MetastoreConf.getBoolVar(getConf(),
        MetastoreConf.ConfVars.STATS_NDV_DENSITY_FUNCTION);
    double ndvTuner = MetastoreConf.getDoubleVar(getConf(), MetastoreConf.ConfVars.STATS_NDV_TUNER);

    List<ColumnStatistics> partStats = IcebergTableUtil.getColStatsPath(table, snapshot.snapshotId())
        .map(statsPath -> readColStats(table, statsPath, Sets.newHashSet(partNames)))
        .orElse(Collections.emptyList());

    partStats.forEach(colStats ->
        colStats.getStatsObj().removeIf(statsObj -> !colNames.contains(statsObj.getColName())));

    List<ColumnStatisticsObj> colStatsList = MetaStoreServerUtils.aggrPartitionStats(partStats,
        MetaStoreUtils.getDefaultCatalog(conf), hmsTable.getDbName(), hmsTable.getTableName(),
        partNames, colNames,
        partStats.size() == partNames.size(),
        useDensityFunctionForNDVEstimation, ndvTuner);

    return new AggrStats(colStatsList, partStats.size());
  }

  private List<ColumnStatistics> readColStats(Table table, Path statsPath, Set<String> partNames) {
    List<ColumnStatistics> colStats = Lists.newArrayList();

    try (PuffinReader reader = Puffin.read(table.io().newInputFile(statsPath.toString())).build()) {
      List<BlobMetadata> blobMetadata = reader.fileMetadata().blobs();

      if (partNames != null) {
        blobMetadata = blobMetadata.stream()
            .filter(metadata -> partNames.contains(metadata.properties().get("partition")))
            .collect(Collectors.toList());
      }
      Iterator<ByteBuffer> it = Iterables.transform(reader.readAll(blobMetadata), Pair::second).iterator();
      LOG.info("Using col stats from : {}", statsPath);

      while (it.hasNext()) {
        byte[] byteBuffer = ByteBuffers.toByteArray(it.next());
        colStats.add(SerializationUtils.deserialize(byteBuffer));
      }
    } catch (Exception e) {
      LOG.warn(" Unable to read col stats: ", e);
    }
    return colStats;
  }

  @Override
  public boolean canComputeQueryUsingStats(Partish partish) {
    org.apache.hadoop.hive.ql.metadata.Table hmsTable = partish.getTable();
    if (hmsTable.getMetaTable() != null) {
      return false;
    }
    if (!getStatsSource().equals(HiveMetaHook.ICEBERG) && StatsSetupConst.areBasicStatsUptoDate(
          partish.getPartParameters())) {
      return true;
    }
    Table table = getTable(hmsTable);
    Snapshot snapshot = IcebergTableUtil.getTableSnapshot(table, hmsTable);
    if (snapshot != null) {
      Map<String, String> summary = getPartishSummary(partish, table, snapshot);
      if (summary != null && summary.containsKey(TOTAL_EQ_DELETES_PROP) &&
          summary.containsKey(TOTAL_POS_DELETES_PROP)) {

        long totalEqDeletes = Long.parseLong(summary.get(TOTAL_EQ_DELETES_PROP));
        long totalPosDeletes = Long.parseLong(summary.get(TOTAL_POS_DELETES_PROP));
        return totalEqDeletes + totalPosDeletes == 0;
      }
    }
    return false;
  }

  private String getStatsSource() {
    return HiveConf.getVar(conf, ConfVars.HIVE_ICEBERG_STATS_SOURCE, HiveMetaHook.ICEBERG)
        .toUpperCase();
  }

  private boolean shouldRewriteColStats(Table tbl) {
    return SessionStateUtil.getQueryState(conf).map(QueryState::getHiveOperation)
              .filter(opType -> HiveOperation.ANALYZE_TABLE == opType).isPresent() ||
          IcebergTableUtil.getColStatsPath(tbl).isPresent();
  }

  private void checkAndMergeColStats(List<ColumnStatistics> statsNew, Table tbl) throws InvalidObjectException {
    Long previousSnapshotId = tbl.currentSnapshot().parentId();
    if (previousSnapshotId != null && canProvideColStats(tbl, previousSnapshotId)) {
      List<ColumnStatistics> statsOld = IcebergTableUtil.getColStatsPath(tbl, previousSnapshotId)
          .map(statsPath -> readColStats(tbl, statsPath, null))
          .orElse(Collections.emptyList());

      boolean isTblLevel = statsNew.getFirst().getStatsDesc().isIsTblLevel();
      Map<String, ColumnStatistics> oldStatsMap = Maps.newHashMap();

      if (!isTblLevel) {
        for (ColumnStatistics statsObjOld : statsOld) {
          oldStatsMap.put(statsObjOld.getStatsDesc().getPartName(), statsObjOld);
        }
      }
      for (ColumnStatistics statsObjNew : statsNew) {
        String partitionKey = statsObjNew.getStatsDesc().getPartName();
        ColumnStatistics statsObjOld = isTblLevel ?
            statsOld.getFirst() : oldStatsMap.get(partitionKey);

        if (statsObjOld != null && statsObjOld.getStatsObjSize() != 0 && !statsObjNew.getStatsObj().isEmpty()) {
          MetaStoreServerUtils.mergeColStats(statsObjNew, statsObjOld);
          if (!isTblLevel) {
            oldStatsMap.remove(partitionKey);
          }
        }
      }
      if (!isTblLevel) {
        statsNew.addAll(oldStatsMap.values());
      }
    }
  }

  /**
   * Iceberg's optimistic concurrency control fails to provide means for IOW and Insert operations isolation.
   * Use `hive.txn.ext.locking.enabled` config to create Hive locks in order to guarantee data consistency.
   */
  @Override
  public LockType getLockType(WriteEntity writeEntity) {
    org.apache.hadoop.hive.ql.metadata.Table hmsTable = writeEntity.getTable();
    boolean sharedWrite = !HiveConf.getBoolVar(conf, ConfVars.TXN_WRITE_X_LOCK);
    // Materialized views stored by Iceberg and the MV metadata is stored in HMS doesn't need write locking because
    // the locking is done by DbTxnManager.acquireMaterializationRebuildLock()
    if (TableType.MATERIALIZED_VIEW == writeEntity.getTable().getTableType()) {
      return LockType.SHARED_READ;
    }
    if (HiveTableOperations.hiveLockEnabled(hmsTable.getParameters(), conf)) {
      throw new RuntimeException("Hive locking on table `" + hmsTable.getFullTableName() +
          "`cannot be enabled when `engine.hive.lock-enabled`=`true`. " +
          "Disable `engine.hive.lock-enabled` to use Hive locking");
    }
    return switch (writeEntity.getWriteType()) {
      case INSERT_OVERWRITE ->
        LockType.EXCL_WRITE;
      case UPDATE, DELETE -> sharedWrite ?
        LockType.SHARED_WRITE : LockType.EXCL_WRITE;
      default -> LockType.SHARED_WRITE;
    };
  }

  @Override
  public boolean supportsPartitionTransform() {
    return true;
  }

  @Override
  public List<TransformSpec> getPartitionTransformSpec(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    return table.spec().fields().stream()
      .filter(f -> !f.transform().isVoid())
      .map(f -> {
        TransformSpec spec = IcebergTableUtil.getTransformSpec(
            table, f.transform().toString(), f.sourceId());
        spec.setFieldName(f.name());
        return spec;
      })
      .collect(Collectors.toList());
  }

  @Override
  public Map<Integer, List<TransformSpec>> getPartitionTransformSpecs(
      org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    return table.specs().entrySet().stream().flatMap(e ->
      e.getValue().fields().stream()
        .filter(f -> !f.transform().isVoid())
        .map(f -> {
          TransformSpec spec = IcebergTableUtil.getTransformSpec(
              table, f.transform().toString(), f.sourceId());
          spec.setFieldName(f.name());
          return Pair.of(e.getKey(), spec);
        }))
      .collect(Collectors.groupingBy(
          Pair::first, Collectors.mapping(Pair::second, Collectors.toList())));
  }

  private List<TransformSpec> getSortTransformSpec(Table table) {
    return table.sortOrder().fields().stream().map(s ->
        IcebergTableUtil.getTransformSpec(table, s.transform().toString(), s.sourceId())
      )
      .collect(Collectors.toList());
  }

  @Override
  public DynamicPartitionCtx createDPContext(
          HiveConf hiveConf, org.apache.hadoop.hive.ql.metadata.Table hmsTable, Operation writeOperation)
      throws SemanticException {
    // delete records are already clustered by partition spec id and the hash of the partition struct
    // there is no need to do any additional sorting based on partition columns
    if (writeOperation == Operation.DELETE && !shouldOverwrite(hmsTable, writeOperation)) {
      return null;
    }

    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table table = IcebergTableUtil.getTable(conf, tableDesc.getProperties());

    DynamicPartitionCtx dpCtx = new DynamicPartitionCtx(Maps.newLinkedHashMap(),
        hiveConf.getVar(ConfVars.DEFAULT_PARTITION_NAME),
        hiveConf.getIntVar(ConfVars.DYNAMIC_PARTITION_MAX_PARTS_PER_NODE));

    if (table.spec().isPartitioned() &&
          hiveConf.getIntVar(ConfVars.HIVE_OPT_SORT_DYNAMIC_PARTITION_THRESHOLD) >= 0) {
      addCustomSortExpr(table, hmsTable, writeOperation, dpCtx, getPartitionTransformSpec(hmsTable));
    }

    SortOrder sortOrder = table.sortOrder();
    if (sortOrder.isSorted()) {
      List<Integer> customSortPositions = Lists.newLinkedList();
      List<Integer> customSortOrder = Lists.newLinkedList();
      dpCtx.setCustomSortOrder(customSortOrder);
      List<Integer> customSortNullOrder = Lists.newLinkedList();
      dpCtx.setCustomSortNullOrder(customSortNullOrder);
      for (SortField sortField : sortOrder.fields()) {
        int pos = 0;
        for (Types.NestedField field : table.schema().columns()) {
          if (sortField.sourceId() == field.fieldId()) {
            customSortPositions.add(pos);
            customSortOrder.add(sortField.direction() == SortDirection.ASC ? 1 : 0);

            NullOrdering nullOrdering = NullOrdering.NULLS_LAST;
            if (sortField.nullOrder() == NullOrder.NULLS_FIRST) {
              nullOrdering = NullOrdering.NULLS_FIRST;
            }
            customSortNullOrder.add(nullOrdering.getCode());
            break;
          }
          pos++;
        }
      }

      addCustomSortExpr(table, hmsTable, writeOperation, dpCtx, getSortTransformSpec(table));
    }

    return dpCtx;
  }

  private void addCustomSortExpr(Table table,  org.apache.hadoop.hive.ql.metadata.Table hmsTable,
      Operation writeOperation, DynamicPartitionCtx dpCtx,
      List<TransformSpec> transformSpecs) {
    Map<String, Integer> fieldOrderMap = Maps.newHashMap();
    List<Types.NestedField> fields = table.schema().columns();
    for (int i = 0; i < fields.size(); ++i) {
      fieldOrderMap.put(fields.get(i).name(), i);
    }

    int offset = (shouldOverwrite(hmsTable, writeOperation) ?
        ACID_VIRTUAL_COLS_AS_FIELD_SCHEMA : acidSelectColumns(hmsTable, writeOperation)).size();

    dpCtx.addCustomSortExpressions(transformSpecs.stream().map(spec ->
        IcebergTransformSortFunctionUtil.getCustomSortExprs(spec, fieldOrderMap.get(spec.getColumnName()) + offset)
    ).collect(Collectors.toList()));
  }

  @Override
  public boolean supportsPartitionAwareOptimization(org.apache.hadoop.hive.ql.metadata.Table table) {
    if (hasUndergonePartitionEvolution(table)) {
      // Don't support complex cases yet
      return false;
    }
    final List<TransformSpec> specs = getPartitionTransformSpec(table);
    // Currently, we support the only bucket transform
    return specs.stream().anyMatch(IcebergTableUtil::isBucket);
  }

  @Override
  public PartitionAwareOptimizationCtx createPartitionAwareOptimizationContext(
      org.apache.hadoop.hive.ql.metadata.Table table) {
    // Currently, we support the only bucket transform
    final List<String> bucketColumnNames = Lists.newArrayList();
    final List<Integer> numBuckets = Lists.newArrayList();
    getPartitionTransformSpec(table).stream().filter(IcebergTableUtil::isBucket).forEach(spec -> {
      bucketColumnNames.add(spec.getColumnName());
      numBuckets.add(spec.getTransformParam());
    });

    if (bucketColumnNames.isEmpty()) {
      return null;
    }
    final IcebergBucketFunction bucketFunction = new IcebergBucketFunction(bucketColumnNames, numBuckets);
    return new PartitionAwareOptimizationCtx(bucketFunction);
  }

  @Override
  public String getFileFormatPropertyKey() {
    return TableProperties.DEFAULT_FILE_FORMAT;
  }

  @Override
  public boolean commitInMoveTask() {
    return true;
  }

  @Override
  public void storageHandlerCommit(Properties commitProperties, Operation operation, ExecutorService executorService)
      throws HiveException {
    String tableName = commitProperties.getProperty(Catalogs.NAME);
    String location = commitProperties.getProperty(Catalogs.LOCATION);
    String snapshotRef = commitProperties.getProperty(Catalogs.SNAPSHOT_REF);
    Configuration configuration = SessionState.getSessionConf();
    if (location != null) {
      HiveTableUtil.cleanupTableObjectFile(location, configuration);
    }
    List<JobContext> jobContextList = HiveIcebergOutputCommitter
            .generateJobContext(configuration, tableName, snapshotRef);
    if (jobContextList.isEmpty()) {
      return;
    }
    HiveIcebergOutputCommitter committer = getOutputCommitter();
    committer.setWorkerPool(executorService);
    try {
      committer.commitJobs(jobContextList, operation);
    } catch (Throwable e) {
      String ids = jobContextList
          .stream().map(jobContext -> jobContext.getJobID().toString()).collect(Collectors.joining(", "));
      // Aborting the job if the commit has failed
      LOG.error("Error while trying to commit job: {}, starting rollback changes for table: {}",
          ids, tableName, e);
      try {
        committer.abortJobs(jobContextList);
      } catch (IOException ioe) {
        LOG.error("Error while trying to abort failed job. There might be uncleaned data files.", ioe);
        // no throwing here because the original exception should be propagated
      }
      throw new HiveException(
          "Error committing job: " + ids + " for table: " + tableName, e);
    }
  }


  @Override
  public HiveIcebergOutputCommitter getOutputCommitter() {
    return new HiveIcebergOutputCommitter();
  }

  @Override
  public boolean isAllowedAlterOperation(AlterTableType opType) {
    return HiveIcebergMetaHook.SUPPORTED_ALTER_OPS.contains(opType);
  }

  @Override
  public boolean supportsTruncateOnNonNativeTables() {
    return true;
  }

  @Override
  public boolean isTimeTravelAllowed() {
    return true;
  }

  @Override
  public boolean isTableMetaRefSupported() {
    return true;
  }

  @Override
  public void executeOperation(org.apache.hadoop.hive.ql.metadata.Table hmsTable, AlterTableExecuteSpec executeSpec) {
    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table icebergTable = IcebergTableUtil.getTable(conf, tableDesc.getProperties());
    switch (executeSpec.getOperationType()) {
      case ROLLBACK:
        LOG.info("Executing rollback operation on iceberg table. If you would like to revert rollback you could " +
              "try altering the metadata location to the current metadata location by executing the following query:" +
              "ALTER TABLE {}.{} SET TBLPROPERTIES('metadata_location'='{}'). This operation is supported for Hive " +
              "Catalog tables.", hmsTable.getDbName(), hmsTable.getTableName(),
            ((BaseTable) icebergTable).operations().current().metadataFileLocation());
        AlterTableExecuteSpec.RollbackSpec rollbackSpec =
            (AlterTableExecuteSpec.RollbackSpec) executeSpec.getOperationParams();
        IcebergTableUtil.rollback(icebergTable, rollbackSpec.getRollbackType(), rollbackSpec.getParam());
        break;
      case EXPIRE_SNAPSHOT:
        LOG.info("Executing expire snapshots operation on iceberg table {}.{}", hmsTable.getDbName(),
            hmsTable.getTableName());
        AlterTableExecuteSpec.ExpireSnapshotsSpec expireSnapshotsSpec =
            (AlterTableExecuteSpec.ExpireSnapshotsSpec) executeSpec.getOperationParams();
        int numThreads = conf.getInt(ConfVars.HIVE_ICEBERG_EXPIRE_SNAPSHOT_NUMTHREADS.varname,
            ConfVars.HIVE_ICEBERG_EXPIRE_SNAPSHOT_NUMTHREADS.defaultIntVal);
        expireSnapshot(icebergTable, expireSnapshotsSpec, numThreads);
        break;
      case SET_CURRENT_SNAPSHOT:
        AlterTableExecuteSpec.SetCurrentSnapshotSpec setSnapshotVersionSpec =
            (AlterTableExecuteSpec.SetCurrentSnapshotSpec) executeSpec.getOperationParams();
        LOG.debug("Executing set current snapshot operation on iceberg table {}.{} to version {}", hmsTable.getDbName(),
            hmsTable.getTableName(), setSnapshotVersionSpec.getSnapshotIdOrRefName());
        IcebergTableUtil.setCurrentSnapshot(icebergTable, setSnapshotVersionSpec.getSnapshotIdOrRefName());
        break;
      case FAST_FORWARD:
        AlterTableExecuteSpec.FastForwardSpec fastForwardSpec =
            (AlterTableExecuteSpec.FastForwardSpec) executeSpec.getOperationParams();
        IcebergTableUtil.fastForwardBranch(icebergTable, fastForwardSpec.getSourceBranch(),
            fastForwardSpec.getTargetBranch());
        break;
      case CHERRY_PICK:
        AlterTableExecuteSpec.CherryPickSpec cherryPickSpec =
            (AlterTableExecuteSpec.CherryPickSpec) executeSpec.getOperationParams();
        IcebergTableUtil.cherryPick(icebergTable, cherryPickSpec.getSnapshotId());
        break;
      case DELETE_METADATA:
        AlterTableExecuteSpec.DeleteMetadataSpec deleteMetadataSpec =
            (AlterTableExecuteSpec.DeleteMetadataSpec) executeSpec.getOperationParams();
        IcebergTableUtil.performMetadataDelete(icebergTable, deleteMetadataSpec.getBranchName(),
            deleteMetadataSpec.getSarg());
        break;
      case DELETE_ORPHAN_FILES:
        int numDeleteThreads = conf.getInt(ConfVars.HIVE_ICEBERG_EXPIRE_SNAPSHOT_NUMTHREADS.varname,
            ConfVars.HIVE_ICEBERG_EXPIRE_SNAPSHOT_NUMTHREADS.defaultIntVal);
        AlterTableExecuteSpec.DeleteOrphanFilesDesc deleteOrphanFilesSpec =
            (AlterTableExecuteSpec.DeleteOrphanFilesDesc) executeSpec.getOperationParams();
        deleteOrphanFiles(icebergTable, deleteOrphanFilesSpec.getTimestampMillis(), numDeleteThreads);
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Operation type %s is not supported", executeSpec.getOperationType().name()));
    }
  }

  private void deleteOrphanFiles(Table icebergTable, long timestampMillis, int numThreads) {
    ExecutorService deleteExecutorService = null;
    try {
      if (numThreads > 0) {
        LOG.info("Executing delete orphan files on iceberg table {} with {} threads", icebergTable.name(), numThreads);
        deleteExecutorService = IcebergTableUtil.newDeleteThreadPool(icebergTable.name(),
            numThreads);
      }

      HiveIcebergDeleteOrphanFiles deleteOrphanFiles = new HiveIcebergDeleteOrphanFiles(conf, icebergTable);
      deleteOrphanFiles.olderThan(timestampMillis);
      if (deleteExecutorService != null) {
        deleteOrphanFiles.executeDeleteWith(deleteExecutorService);
      }
      DeleteOrphanFiles.Result result = deleteOrphanFiles.execute();
      LOG.debug("Cleaned files {} for {}", result.orphanFileLocations(), icebergTable);
    } finally {
      if (deleteExecutorService != null) {
        deleteExecutorService.shutdown();
      }
    }
  }

  private void expireSnapshot(Table icebergTable, AlterTableExecuteSpec.ExpireSnapshotsSpec expireSnapshotsSpec,
      int numThreads) {
    ExecutorService deleteExecutorService = null;
    try {
      if (numThreads > 0) {
        LOG.info("Executing expire snapshots on iceberg table {} with {} threads", icebergTable.name(), numThreads);
        deleteExecutorService = IcebergTableUtil.newDeleteThreadPool(icebergTable.name(), numThreads);
      }
      if (expireSnapshotsSpec == null) {
        expireSnapshotWithDefaultParams(icebergTable, deleteExecutorService);
      } else if (expireSnapshotsSpec.isExpireByTimestampRange()) {
        expireSnapshotByTimestampRange(icebergTable, expireSnapshotsSpec.getFromTimestampMillis(),
            expireSnapshotsSpec.getTimestampMillis(), deleteExecutorService);
      } else if (expireSnapshotsSpec.isExpireByIds()) {
        expireSnapshotByIds(icebergTable, expireSnapshotsSpec.getIdsToExpire(), deleteExecutorService);
      } else if (expireSnapshotsSpec.isExpireByRetainLast()) {
        expireSnapshotRetainLast(icebergTable, expireSnapshotsSpec.getNumRetainLast(), deleteExecutorService);
      } else {
        expireSnapshotOlderThanTimestamp(icebergTable, expireSnapshotsSpec.getTimestampMillis(), deleteExecutorService);
      }
    } finally {
      if (deleteExecutorService != null) {
        deleteExecutorService.shutdown();
      }
    }
  }

  private void expireSnapshotWithDefaultParams(Table icebergTable, ExecutorService deleteExecutorService) {
    ExpireSnapshots expireSnapshots = icebergTable.expireSnapshots();
    if (deleteExecutorService != null) {
      expireSnapshots.executeDeleteWith(deleteExecutorService);
    }
    expireSnapshots.commit();
  }

  private void expireSnapshotRetainLast(Table icebergTable, int numRetainLast, ExecutorService deleteExecutorService) {
    ExpireSnapshots expireSnapshots = icebergTable.expireSnapshots();
    expireSnapshots.retainLast(numRetainLast);
    if (deleteExecutorService != null) {
      expireSnapshots.executeDeleteWith(deleteExecutorService);
    }
    expireSnapshots.commit();
  }

  private void expireSnapshotByTimestampRange(Table icebergTable, Long fromTimestamp, Long toTimestamp,
      ExecutorService deleteExecutorService) {
    ExpireSnapshots expireSnapshots = icebergTable.expireSnapshots();
    for (Snapshot snapshot : icebergTable.snapshots()) {
      if (snapshot.timestampMillis() >= fromTimestamp && snapshot.timestampMillis() <= toTimestamp) {
        expireSnapshots.expireSnapshotId(snapshot.snapshotId());
        LOG.debug("Expiring snapshot on {} with id: {} and timestamp: {}", icebergTable.name(), snapshot.snapshotId(),
            snapshot.timestampMillis());
      }
    }
    LOG.info("Expiring snapshot on {} within time range {} -> {}", icebergTable.name(), fromTimestamp, toTimestamp);
    if (deleteExecutorService != null) {
      expireSnapshots.executeDeleteWith(deleteExecutorService);
    }
    expireSnapshots.commit();
  }

  private void expireSnapshotOlderThanTimestamp(Table icebergTable, Long timestamp,
      ExecutorService deleteExecutorService) {
    ExpireSnapshots expireSnapshots = icebergTable.expireSnapshots().expireOlderThan(timestamp);
    if (deleteExecutorService != null) {
      expireSnapshots.executeDeleteWith(deleteExecutorService);
    }
    expireSnapshots.commit();
  }

  private void expireSnapshotByIds(Table icebergTable, String[] idsToExpire,
      ExecutorService deleteExecutorService) {
    if (idsToExpire.length != 0) {
      ExpireSnapshots expireSnapshots = icebergTable.expireSnapshots();
      for (String id : idsToExpire) {
        expireSnapshots.expireSnapshotId(Long.parseLong(id));
      }
      LOG.info("Expiring snapshot on {} for snapshot Ids: {}", icebergTable.name(), Arrays.toString(idsToExpire));
      if (deleteExecutorService != null) {
        expireSnapshots = expireSnapshots.executeDeleteWith(deleteExecutorService);
      }
      expireSnapshots.commit();
    }
  }

  @Override
  public void alterTableSnapshotRefOperation(org.apache.hadoop.hive.ql.metadata.Table hmsTable,
      AlterTableSnapshotRefSpec alterTableSnapshotRefSpec) {
    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table icebergTable = IcebergTableUtil.getTable(conf, tableDesc.getProperties());

    switch (alterTableSnapshotRefSpec.getOperationType()) {
      case CREATE_BRANCH:
        AlterTableSnapshotRefSpec.CreateSnapshotRefSpec createBranchSpec =
            (AlterTableSnapshotRefSpec.CreateSnapshotRefSpec) alterTableSnapshotRefSpec.getOperationParams();
        IcebergSnapshotRefExec.createBranch(icebergTable, createBranchSpec);
        break;
      case CREATE_TAG:
        if (icebergTable.currentSnapshot() == null) {
          throw new UnsupportedOperationException(
              String.format("Cannot alter %s on iceberg table %s.%s which has no snapshot",
                  alterTableSnapshotRefSpec.getOperationType().getName(), hmsTable.getDbName(),
                  hmsTable.getTableName()));
        }
        AlterTableSnapshotRefSpec.CreateSnapshotRefSpec createTagSpec =
            (AlterTableSnapshotRefSpec.CreateSnapshotRefSpec) alterTableSnapshotRefSpec.getOperationParams();
        IcebergSnapshotRefExec.createTag(icebergTable, createTagSpec);
        break;
      case DROP_BRANCH:
        AlterTableSnapshotRefSpec.DropSnapshotRefSpec dropBranchSpec =
            (AlterTableSnapshotRefSpec.DropSnapshotRefSpec) alterTableSnapshotRefSpec.getOperationParams();
        IcebergSnapshotRefExec.dropBranch(icebergTable, dropBranchSpec);
        break;
      case RENAME_BRANCH:
        AlterTableSnapshotRefSpec.RenameSnapshotrefSpec renameSnapshotrefSpec =
            (AlterTableSnapshotRefSpec.RenameSnapshotrefSpec) alterTableSnapshotRefSpec.getOperationParams();
        IcebergSnapshotRefExec.renameBranch(icebergTable, renameSnapshotrefSpec);
        break;
      case REPLACE_SNAPSHOTREF:
        AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec replaceSnapshotrefSpec =
            (AlterTableSnapshotRefSpec.ReplaceSnapshotrefSpec) alterTableSnapshotRefSpec.getOperationParams();
        if (replaceSnapshotrefSpec.isReplaceBranch()) {
          IcebergSnapshotRefExec.replaceBranch(icebergTable, replaceSnapshotrefSpec);
        } else {
          IcebergSnapshotRefExec.replaceTag(icebergTable, replaceSnapshotrefSpec);
        }
        break;
      case DROP_TAG:
        AlterTableSnapshotRefSpec.DropSnapshotRefSpec dropTagSpec =
            (AlterTableSnapshotRefSpec.DropSnapshotRefSpec) alterTableSnapshotRefSpec.getOperationParams();
        IcebergSnapshotRefExec.dropTag(icebergTable, dropTagSpec);
        break;
      default:
        throw new UnsupportedOperationException(String.format(
            "Operation type %s is not supported", alterTableSnapshotRefSpec.getOperationType().getName()));
    }
  }

  @Override
  public boolean isValidMetadataTable(String metaTableName) {
    return Optional.ofNullable(metaTableName)
        .map(MetadataTableType::from)
        .filter(type -> type != MetadataTableType.POSITION_DELETES)
        .isPresent();
  }

  @Override
  public org.apache.hadoop.hive.ql.metadata.Table checkAndSetTableMetaRef(
      org.apache.hadoop.hive.ql.metadata.Table hmsTable, String tableMetaRef) throws SemanticException {
    String refName = HiveUtils.getTableSnapshotRef(tableMetaRef);
    if (refName != null) {
      Table tbl = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
      if (tbl.snapshot(refName) != null) {
        hmsTable.setSnapshotRef(tableMetaRef);
        return hmsTable;
      }
      throw new SemanticException(String.format("Cannot use snapshotRef (does not exist): %s", refName));
    }
    if (isValidMetadataTable(tableMetaRef)) {
      hmsTable.setMetaTable(tableMetaRef);
      return hmsTable;
    }
    throw new SemanticException(ErrorMsg.INVALID_METADATA_TABLE_NAME, tableMetaRef);
  }

  @Override
  public URI getURIForAuth(org.apache.hadoop.hive.metastore.api.Table hmsTable) throws URISyntaxException {
    String dbName = hmsTable.getDbName();
    String tableName = hmsTable.getTableName();
    StringBuilder authURI =
        new StringBuilder(ICEBERG_URI_PREFIX).append(encodeString(dbName)).append("/").append(encodeString(tableName))
            .append("?snapshot=");
    // If metadata location is provided we should use that location for auth, since during create if the
    // metadata_location is explicitly provided we register a table using that path.
    Optional<String> metadataLocation =
        SessionStateUtil.getProperty(conf, BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    if (metadataLocation.isPresent()) {
      authURI.append(getPathForAuth(metadataLocation.get()));
    } else {
      Optional<String> locationProperty =
          SessionStateUtil.getProperty(conf, hive_metastoreConstants.META_TABLE_LOCATION);
      if (locationProperty.isPresent()) {
        // this property is set during the create operation before the hive table was created
        // we are returning a dummy iceberg metadata file
        authURI.append(getPathForAuth(locationProperty.get()))
            .append(encodeString("/metadata/dummy.metadata.json"));
      } else {
        Table table = IcebergTableUtil.getTable(conf, hmsTable);
        authURI.append(getPathForAuth(((BaseTable) table).operations().current().metadataFileLocation(),
            hmsTable.getSd().getLocation()));
      }
    }
    LOG.debug("Iceberg storage handler authorization URI {}", authURI);
    return new URI(authURI.toString());
  }

  @VisibleForTesting
  static String encodeString(String rawString) {
    if (rawString == null) {
      return null;
    }
    return HiveConf.EncoderDecoderFactory.URL_ENCODER_DECODER.encode(rawString);
  }

  String getPathForAuth(String locationProperty) {
    return getPathForAuth(locationProperty,
        SessionStateUtil.getProperty(conf, SessionStateUtil.DEFAULT_TABLE_LOCATION).orElse(null));
  }

  String getPathForAuth(String locationProperty, String defaultTableLocation) {
    boolean maskDefaultLocation = conf.getBoolean(ConfVars.HIVE_ICEBERG_MASK_DEFAULT_LOCATION.varname,
        ConfVars.HIVE_ICEBERG_MASK_DEFAULT_LOCATION.defaultBoolVal);
    String location = URI.create(locationProperty).getPath();
    if (!maskDefaultLocation || defaultTableLocation == null ||
        !arePathsInSameFs(locationProperty, defaultTableLocation)) {
      return encodeString(location);
    }
    try {
      Path locationPath = new Path(location);
      Path defaultLocationPath = locationPath.toUri().getScheme() != null ?
          FileUtils.makeQualified(new Path(defaultTableLocation), conf) :
          Path.getPathWithoutSchemeAndAuthority(new Path(defaultTableLocation));
      return encodeString(location.replaceFirst(defaultLocationPath.toString(), TABLE_DEFAULT_LOCATION));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean arePathsInSameFs(String locationProperty, String defaultTableLocation) {
    try {
      return FileUtils.equalsFileSystem(new Path(locationProperty).getFileSystem(conf),
          new Path(defaultTableLocation).getFileSystem(conf));
    } catch (IOException e) {
      LOG.debug("Unable to get FileSystem for path {} and {}", locationProperty, defaultTableLocation);
      return false;
    }
  }

  @Override
  public void validateSinkDesc(FileSinkDesc sinkDesc) throws SemanticException {
    HiveStorageHandler.super.validateSinkDesc(sinkDesc);
    if (sinkDesc.getInsertOverwrite()) {
      Table table = IcebergTableUtil.getTable(conf, sinkDesc.getTableInfo().getProperties());
      if (table.currentSnapshot() != null &&
          Long.parseLong(table.currentSnapshot().summary().get(TOTAL_RECORDS_PROP)) == 0) {
        // If the table is empty we don't have any danger that some data can get lost.
        return;
      }
      if (RewritePolicy.fromString(conf.get(ConfVars.REWRITE_POLICY.varname)) != RewritePolicy.DEFAULT) {
        // Table rewriting has special logic as part of IOW that handles the case when table had a partition evolution
        return;
      }
      if (IcebergTableUtil.isBucketed(table)) {
        throw new SemanticException("Cannot perform insert overwrite query on bucket partitioned Iceberg table.");
      }
      if (IcebergTableUtil.hasUndergonePartitionEvolution(table)) {
        throw new SemanticException(
            "Cannot perform insert overwrite query on Iceberg table where partition evolution happened. In order " +
            "to successfully carry out any insert overwrite operation on this table, the data has to be rewritten " +
            "conforming to the latest spec. ");
      }
    }
  }

  @Override
  public AcidSupportType supportsAcidOperations() {
    return AcidSupportType.WITHOUT_TRANSACTIONS;
  }

  @Override
  public List<VirtualColumn> acidVirtualColumns() {
    return ACID_VIRTUAL_COLS;
  }

  @Override
  public List<FieldSchema> acidSelectColumns(org.apache.hadoop.hive.ql.metadata.Table table, Operation operation) {
    return switch (operation) {
      case DELETE ->
        // TODO: make it configurable whether we want to include the table columns in the select query.
        // It might make delete writes faster if we don't have to write out the row object
        ListUtils.union(ACID_VIRTUAL_COLS_AS_FIELD_SCHEMA, table.getCols());
      case UPDATE -> shouldOverwrite(table, operation) ?
        ACID_VIRTUAL_COLS_AS_FIELD_SCHEMA :
        ListUtils.union(ACID_VIRTUAL_COLS_AS_FIELD_SCHEMA, table.getCols());
      case MERGE ->
        ACID_VIRTUAL_COLS_AS_FIELD_SCHEMA;
      default -> ImmutableList.of();
    };
  }

  @Override
  public FieldSchema getRowId() {
    VirtualColumn rowId = ROW_POSITION;
    return new FieldSchema(rowId.getName(), rowId.getTypeInfo().getTypeName(), "");
  }

  @Override
  public List<FieldSchema> acidSortColumns(org.apache.hadoop.hive.ql.metadata.Table table, Operation operation) {
    return switch (operation) {
      case DELETE -> IcebergTableUtil.isFanoutEnabled(table.getParameters()) ?
        EMPTY_ORDERING : POSITION_DELETE_ORDERING;
      case MERGE ->
        POSITION_DELETE_ORDERING;
      default ->
        // For update operations we use the same sort order defined by
        // {@link #createDPContext(HiveConf, org.apache.hadoop.hive.ql.metadata.Table)}
        EMPTY_ORDERING;
    };
  }

  @Override
  public boolean supportsSortColumns() {
    return true;
  }

  @Override
  public List<FieldSchema> sortColumns(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table table = IcebergTableUtil.getTable(conf, tableDesc.getProperties());
    if (table.sortOrder().isUnsorted()) {
      return Collections.emptyList();
    }

    Schema schema = table.schema();
    List<FieldSchema> hiveSchema = HiveSchemaUtil.convert(schema);
    Map<String, String> colNameToColType = hiveSchema.stream()
        .collect(Collectors.toMap(FieldSchema::getName, FieldSchema::getType));
    return table.sortOrder().fields().stream().map(s -> new FieldSchema(schema.findColumnName(s.sourceId()),
        colNameToColType.get(schema.findColumnName(s.sourceId())),
        String.format("Transform: %s, Sort direction: %s, Null sort order: %s",
        s.transform().toString(), s.direction().name(), s.nullOrder().name()))).collect(Collectors.toList());
  }

  private void setCommonJobConf(JobConf jobConf) {
    jobConf.set("tez.mrreader.config.update.properties", "hive.io.file.readcolumn.names,hive.io.file.readcolumn.ids");
  }

  public StorageHandlerTypes getType() {
    return StorageHandlerTypes.ICEBERG;
  }

  public boolean addDynamicSplitPruningEdge(org.apache.hadoop.hive.ql.metadata.Table table,
                                            ExprNodeDesc syntheticFilterPredicate) {
    try {
      Collection<String> partitionColumns = ((HiveIcebergSerDe) table.getDeserializer()).partitionColumns();
      if (!partitionColumns.isEmpty()) {
        // The filter predicate contains ExprNodeDynamicListDesc object(s) for places where we will substitute
        // dynamic values later during execution. For Example:
        // GenericUDFIn(Column[ss_sold_date_sk], RS[5] <-- This is an ExprNodeDynamicListDesc)
        //
        // We would like to check if we will be able to convert these expressions to Iceberg filters when the
        // actual values will be available, so in this check we replace the ExprNodeDynamicListDesc with dummy
        // values and check whether the conversion will be possible or not.
        ExprNodeDesc clone = syntheticFilterPredicate.clone();

        String filterColumn = collectColumnAndReplaceDummyValues(clone, null);

        // If the filter is for a partition column then it could be worthwhile to try dynamic partition pruning
        if (partitionColumns.contains(filterColumn)) {
          // Check if we can convert the expression to a valid Iceberg filter
          SearchArgument sarg = ConvertAstToSearchArg.create(conf, (ExprNodeGenericFuncDesc) clone);
          HiveIcebergFilterFactory.generateFilterExpression(sarg);
          LOG.debug("Found Iceberg partition column to prune with predicate {}", syntheticFilterPredicate);
          return true;
        }
      }
    } catch (UnsupportedOperationException uoe) {
      // If we can not convert the filter, we do not prune
      LOG.debug("Unsupported predicate {}", syntheticFilterPredicate, uoe);
    }

    // There is nothing to prune, or we could not use the filter
    LOG.debug("Not found Iceberg partition columns to prune with predicate {}", syntheticFilterPredicate);
    return false;
  }

  /**
   * Returns the Table serialized to the configuration based on the table name.
   * If configuration is missing from the FileIO of the table, it will be populated with the input config.
   *
   * @param config The configuration used to get the data from
   * @param name The name of the table we need as returned by TableDesc.getTableName()
   * @return The Table
   */
  public static Table table(Configuration config, String name) {
    Table table = SerializationUtil.deserializeFromBase64(config.get(InputFormatConfig.SERIALIZED_TABLE_PREFIX + name));
    if (table == null &&
            config.getBoolean(hive_metastoreConstants.TABLE_IS_CTAS, false) &&
            StringUtils.isNotBlank(config.get(InputFormatConfig.TABLE_LOCATION))) {
      table = HiveTableUtil.readTableObjectFromFile(config);
    }
    checkAndSetIoConfig(config, table);
    return table;
  }

  /**
   * If enabled, it populates the FileIO's hadoop configuration with the input config object.
   * This might be necessary when the table object was serialized without the FileIO config.
   *
   * @param config Configuration to set for FileIO, if enabled
   * @param table The Iceberg table object
   */
  public static void checkAndSetIoConfig(Configuration config, Table table) {
    if (table != null && config.getBoolean(InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
          InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT) && table.io() instanceof HadoopConfigurable) {
      ((HadoopConfigurable) table.io()).setConf(config);
    }
  }

  /**
   * If enabled, it ensures that the FileIO's hadoop configuration will not be serialized.
   * This might be desirable for decreasing the overall size of serialized table objects.
   *
   * Note: Skipping FileIO config serialization in this fashion might in turn necessitate calling
   * {@link #checkAndSetIoConfig(Configuration, Table)} on the deserializer-side to enable subsequent use of the FileIO.
   *
   * @param config Configuration to set for FileIO in a transient manner, if enabled
   * @param table The Iceberg table object
   */
  public static void checkAndSkipIoConfigSerialization(Configuration config, Table table) {
    if (table != null && config.getBoolean(InputFormatConfig.CONFIG_SERIALIZATION_DISABLED,
          InputFormatConfig.CONFIG_SERIALIZATION_DISABLED_DEFAULT) && table.io() instanceof HadoopConfigurable) {
      ((HadoopConfigurable) table.io()).serializeConfWith(conf -> new NonSerializingConfig(config)::get);
    }
  }

  /**
   * Returns the names of the output tables stored in the configuration.
   * @param config The configuration used to get the data from
   * @return The collection of the table names as returned by TableDesc.getTableName()
   */
  public static Set<String> outputTables(Configuration config) {
    return Sets.newHashSet(TABLE_NAME_SPLITTER.split(config.get(InputFormatConfig.OUTPUT_TABLES)));
  }

  /**
   * Returns the catalog name serialized to the configuration.
   * @param config The configuration used to get the data from
   * @param name The name of the table we neeed as returned by TableDesc.getTableName()
   * @return catalog name
   */
  public static String catalogName(Configuration config, String name) {
    return config.get(InputFormatConfig.TABLE_CATALOG_PREFIX + name);
  }

  /**
   * Returns the Table Schema serialized to the configuration.
   * @param config The configuration used to get the data from
   * @return The Table Schema object
   */
  public static Schema schema(Configuration config) {
    return SchemaParser.fromJson(config.get(InputFormatConfig.TABLE_SCHEMA));
  }

  /**
   * Stores the serializable table data in the configuration.
   * Currently the following is handled:
   * <ul>
   *   <li>- Table - in case the table is serializable</li>
   *   <li>- Location</li>
   *   <li>- Schema</li>
   *   <li>- Partition specification</li>
   *   <li>- FileIO for handling table files</li>
   *   <li>- Location provider used for file generation</li>
   *   <li>- Encryption manager for encryption handling</li>
   * </ul>
   * @param configuration The configuration storing the catalog information
   * @param tableDesc The table which we want to store to the configuration
   * @param map The map of the configuration properties which we append with the serialized data
   */
  @VisibleForTesting
  static void overlayTableProperties(Configuration configuration, TableDesc tableDesc, Map<String, String> map) {
    Properties props = tableDesc.getProperties();

    Maps.fromProperties(props).entrySet().stream()
        .filter(entry -> !map.containsKey(entry.getKey())) // map overrides tableDesc properties
        .forEach(entry -> map.put(entry.getKey(), entry.getValue()));

    String location;
    Schema schema;
    PartitionSpec spec;
    try {
      Table table = IcebergTableUtil.getTable(configuration, props);
      location = table.location();
      schema = table.schema();
      spec = table.spec();

      // serialize table object into config
      Table serializableTable = SerializableTable.copyOf(table);
      // set table format-version and write-mode information from tableDesc
      List<String> writeConfigList = ImmutableList.of(
          FORMAT_VERSION, DELETE_MODE, UPDATE_MODE, MERGE_MODE);
      if (IcebergTableUtil.isV2TableOrAbove(props::getProperty)) {
        writeConfigList.forEach(cfg -> serializableTable.properties().computeIfAbsent(cfg, props::getProperty));
      }
      checkAndSkipIoConfigSerialization(configuration, serializableTable);
      map.put(InputFormatConfig.SERIALIZED_TABLE_PREFIX + tableDesc.getTableName(),
          SerializationUtil.serializeToBase64(serializableTable));
    } catch (NoSuchTableException ex) {
      if (!HiveTableUtil.isCtas(props)) {
        throw ex;
      }

      if (!Catalogs.hiveCatalog(configuration, props)) {
        throw new UnsupportedOperationException(HiveIcebergSerDe.CTAS_EXCEPTION_MSG);
      }

      location = map.get(hive_metastoreConstants.META_TABLE_LOCATION);

      map.put(InputFormatConfig.SERIALIZED_TABLE_PREFIX + tableDesc.getTableName(),
              SerializationUtil.serializeToBase64(null));

      try {
        AbstractSerDe serDe = tableDesc.getDeserializer(configuration);
        HiveIcebergSerDe icebergSerDe = (HiveIcebergSerDe) serDe;
        schema = icebergSerDe.getTableSchema();
        spec = IcebergTableUtil.spec(configuration, icebergSerDe.getTableSchema());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    map.put(InputFormatConfig.TABLE_IDENTIFIER, props.getProperty(Catalogs.NAME));
    if (StringUtils.isNotBlank(location)) {
      map.put(InputFormatConfig.TABLE_LOCATION, location);
    }
    String schemaJson = SchemaParser.toJson(schema);
    map.put(InputFormatConfig.TABLE_SCHEMA, schemaJson);
    // save schema into table props as well to avoid repeatedly hitting the HMS during serde initializations
    // this is an exception to the interface documentation, but it's a safe operation to add this property
    props.put(InputFormatConfig.TABLE_SCHEMA, schemaJson);
    if (spec == null) {
      spec = PartitionSpec.unpartitioned();
    }
    props.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(spec));

    // We need to remove this otherwise the job.xml will be invalid as column comments are separated with '\0' and
    // the serialization utils fail to serialize this character
    map.remove("columns.comments");
  }

  @Override
  public void validateCurrentSnapshot(TableDesc tableDesc) {
    if (conf.getBoolean(ConfigProperties.LOCK_HIVE_ENABLED, TableProperties.HIVE_LOCK_ENABLED_DEFAULT) ||
        !HiveConf.getBoolVar(conf, ConfVars.HIVE_TXN_EXT_LOCKING_ENABLED)) {
      return;
    }
    Table table = IcebergTableUtil.getTable(conf, tableDesc.getProperties());
    if (table.currentSnapshot() != null || table instanceof BaseTable) {
      TableMetadata currentMetadata = ((BaseTable) table).operations().current();
      if (currentMetadata.propertyAsBoolean(TableProperties.HIVE_LOCK_ENABLED, false)) {
        return;
      }
      TableMetadata newMetadata = ((BaseTable) table).operations().refresh();
      ValidationException.check(
          Objects.equals(newMetadata.metadataFileLocation(), currentMetadata.metadataFileLocation()),
          "Table snapshot is outdated: %s", tableDesc.getTableName()
      );
    }
  }

  /**
   * Recursively replaces the ExprNodeDynamicListDesc nodes by a dummy ExprNodeConstantDesc so we can test if we can
   * convert the predicate to an Iceberg predicate when pruning the partitions later. Also collects the column names
   * in the filter.
   * <p>
   * Please make sure that it is ok to change the input node (clone if needed)
   * @param node The node we are traversing
   * @param foundColumn The column we already found
   */
  private String collectColumnAndReplaceDummyValues(ExprNodeDesc node, String foundColumn) {
    String column = foundColumn;
    List<ExprNodeDesc> children = node.getChildren();
    if (children != null && !children.isEmpty()) {
      ListIterator<ExprNodeDesc> iterator = children.listIterator();
      while (iterator.hasNext()) {
        ExprNodeDesc child = iterator.next();
        if (child instanceof ExprNodeDynamicListDesc) {
          Object dummy = switch (((PrimitiveTypeInfo) child.getTypeInfo()).getPrimitiveCategory()) {
            case INT, SHORT -> 1;
            case LONG -> 1L;
            case TIMESTAMP, TIMESTAMPLOCALTZ -> new Timestamp();
            case CHAR, VARCHAR, STRING -> "1";
            case DOUBLE, FLOAT, DECIMAL -> 1.1;
            case DATE -> new Date();
            case BOOLEAN -> true;
            default -> throw new UnsupportedOperationException("Not supported primitive type in partition pruning: " +
                child.getTypeInfo());
          };
          iterator.set(new ExprNodeConstantDesc(child.getTypeInfo(), dummy));
        } else {
          String newColumn;
          if (child instanceof ExprNodeColumnDesc) {
            newColumn = ((ExprNodeColumnDesc) child).getColumn();
          } else {
            newColumn = collectColumnAndReplaceDummyValues(child, column);
          }

          if (column != null && newColumn != null && !newColumn.equals(column)) {
            throw new UnsupportedOperationException("Partition pruning does not support filtering for more columns");
          }

          if (column == null) {
            column = newColumn;
          }
        }
      }
    }

    return column;
  }

  /**
   * If any of the following checks is true we fall back to non vectorized mode:
   * <ul>
   *   <li>fileformat is set to avro</li>
   *   <li>querying metadata tables</li>
   *   <li>fileformat is set to ORC, and table schema has time type column</li>
   *   <li>fileformat is set to PARQUET, and table schema has a list type column, that has a complex type element</li>
   * </ul>
   * @param tableProps table properties, must be not null
   */
  private void fallbackToNonVectorizedModeBasedOnProperties(Properties tableProps) {
    Schema tableSchema = SchemaParser.fromJson(tableProps.getProperty(InputFormatConfig.TABLE_SCHEMA));

    if (FileFormat.AVRO.name().equalsIgnoreCase(tableProps.getProperty(TableProperties.DEFAULT_FILE_FORMAT)) ||
        isValidMetadataTable(tableProps.getProperty(IcebergAcidUtil.META_TABLE_PROPERTY)) ||
        hasOrcTimeInSchema(tableProps, tableSchema) ||
        !hasParquetNestedTypeWithinListOrMap(tableProps, tableSchema)) {
      // disable vectorization
      SessionStateUtil.getQueryState(conf).ifPresent(queryState ->
          queryState.getConf().setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, false));
    }
  }

  /**
   * Iceberg Time type columns are written as longs into ORC files. There is no Time type in Hive, so it is represented
   * as String instead. For ORC there's no automatic conversion from long to string during vectorized reading such as
   * for example in Parquet (in Parquet files Time type is an int64 with 'time' logical annotation).
   * @param tableProps iceberg table properties
   * @param tableSchema iceberg table schema
   * @return true if having time type column
   */
  private static boolean hasOrcTimeInSchema(Properties tableProps, Schema tableSchema) {
    if (!FileFormat.ORC.name().equalsIgnoreCase(tableProps.getProperty(TableProperties.DEFAULT_FILE_FORMAT))) {
      return false;
    }
    return tableSchema.columns().stream().anyMatch(f -> Types.TimeType.get().typeId() == f.type().typeId());
  }

  /**
   * Vectorized reads of parquet files from columns with list or map type is only supported if the nested types are of
   * primitive type category
   * check {@link VectorizedParquetRecordReader#checkListColumnSupport} for details on nested types under lists
   * @param tableProps iceberg table properties
   * @param tableSchema iceberg table schema
   * @return true if having nested types
   */
  private static boolean hasParquetNestedTypeWithinListOrMap(Properties tableProps, Schema tableSchema) {
    if (!FileFormat.PARQUET.name().equalsIgnoreCase(tableProps.getProperty(TableProperties.DEFAULT_FILE_FORMAT))) {
      return true;
    }

    for (Types.NestedField field : tableSchema.columns()) {
      if (field.type().isListType() || field.type().isMapType()) {
        for (Types.NestedField nestedField : field.type().asNestedType().fields()) {
          if (!nestedField.type().isPrimitiveType()) {
            return false;
          }
        }
      }
    }

    return true;
  }

  private String getOperationType() {
    return SessionStateUtil.getProperty(conf, Operation.class.getSimpleName())
        .orElse(Operation.OTHER.name());
  }

  private static class NonSerializingConfig implements Serializable {

    private final transient Configuration conf;

    NonSerializingConfig(Configuration conf) {
      this.conf = conf;
    }

    public Configuration get() {
      if (conf == null) {
        throw new IllegalStateException("Configuration was not serialized on purpose but was not set manually either");
      }

      return conf;
    }
  }

  @Override
  public boolean areSnapshotsSupported() {
    return true;
  }

  @Override
  public SnapshotContext getCurrentSnapshotContext(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table table = IcebergTableUtil.getTable(conf, tableDesc.getProperties());
    Snapshot current = table.currentSnapshot();
    if (current == null) {
      return null;
    }
    return toSnapshotContext(current);
  }

  private SnapshotContext toSnapshotContext(Snapshot snapshot) {
    Map<String, String> summaryMap = snapshot.summary();
    long addedRecords = getLongSummary(summaryMap, ADDED_RECORDS_PROP);
    long deletedRecords = getLongSummary(summaryMap, DELETED_RECORDS_PROP);
    return new SnapshotContext(
        snapshot.snapshotId(), toWriteOperationType(snapshot.operation()), addedRecords, deletedRecords);
  }

  private SnapshotContext.WriteOperationType toWriteOperationType(String operation) {
    try {
      return SnapshotContext.WriteOperationType.valueOf(operation.toUpperCase());
    } catch (NullPointerException | IllegalArgumentException ex) {
      return SnapshotContext.WriteOperationType.UNKNOWN;
    }
  }

  private long getLongSummary(Map<String, String> summaryMap, String key) {
    String textValue = summaryMap.get(key);
    if (StringUtils.isBlank(textValue)) {
      return 0;
    }
    return Long.parseLong(textValue);
  }

  @Override
  public Iterable<SnapshotContext> getSnapshotContexts(
      org.apache.hadoop.hive.ql.metadata.Table hmsTable, SnapshotContext since) {

    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table table = IcebergTableUtil.getTable(conf, tableDesc.getProperties());
    return getSnapshots(table.snapshots(), since);
  }

  @VisibleForTesting
  Iterable<SnapshotContext> getSnapshots(Iterable<Snapshot> snapshots, SnapshotContext since) {
    List<SnapshotContext> result = Lists.newArrayList();

    boolean foundSince = Objects.isNull(since);
    for (Snapshot snapshot : snapshots) {
      if (!foundSince) {
        if (snapshot.snapshotId() == since.getSnapshotId()) {
          foundSince = true;
        }
      } else {
        result.add(toSnapshotContext(snapshot));
      }
    }

    return foundSince ? result : Collections.emptyList();
  }

  @Override
  public void prepareAlterTableEnvironmentContext(AbstractAlterTableDesc alterTableDesc,
      EnvironmentContext environmentContext) {
    if (alterTableDesc instanceof AlterTableSetPropertiesDesc &&
        alterTableDesc.getProps().containsKey(BaseMetastoreTableOperations.METADATA_LOCATION_PROP)) {
      // signal manual iceberg metadata location updated by user
      environmentContext.putToProperties(HiveIcebergMetaHook.MANUAL_ICEBERG_METADATA_LOCATION_CHANGE, "true");
    }
  }

  @Override
  public void setTableParametersForCTLT(org.apache.hadoop.hive.ql.metadata.Table tbl, CreateTableLikeDesc desc,
      Map<String, String> origParams) {
    // Preserve the format-version of the iceberg table and filter out rest.
    if (IcebergTableUtil.isV2TableOrAbove(origParams)) {
      tbl.getParameters().put(TableProperties.FORMAT_VERSION, IcebergTableUtil.formatVersion(origParams).toString());
      tbl.getParameters().put(TableProperties.DELETE_MODE, MERGE_ON_READ);
      tbl.getParameters().put(TableProperties.UPDATE_MODE, MERGE_ON_READ);
      tbl.getParameters().put(TableProperties.MERGE_MODE, MERGE_ON_READ);
    }

    // check if the table is being created as managed table, in that case we translate it to external
    if (!desc.isExternal()) {
      tbl.getParameters().put(HiveMetaHook.TRANSLATED_TO_EXTERNAL, "TRUE");
      desc.setIsExternal(true);
    }

    // If source is Iceberg table set the schema and the partition spec
    if (MetaStoreUtils.isIcebergTable(origParams)) {
      tbl.getParameters()
          .put(InputFormatConfig.TABLE_SCHEMA, origParams.get(InputFormatConfig.TABLE_SCHEMA));
      tbl.getParameters()
          .put(InputFormatConfig.PARTITION_SPEC, origParams.get(InputFormatConfig.PARTITION_SPEC));
    } else {
      // if the source is partitioned non-iceberg table set the partition transform spec and set the table as
      // unpartitioned
      List<TransformSpec> spec = PartitionTransform.getPartitionTransformSpec(tbl.getPartitionKeys());
      SessionStateUtil.addResourceOrThrow(conf, hive_metastoreConstants.PARTITION_TRANSFORM_SPEC, spec);
      tbl.getSd().getCols().addAll(tbl.getPartitionKeys());
      tbl.getTTable().setPartitionKeysIsSet(false);
    }
  }

  @Override
  public void setTableLocationForCTAS(CreateTableDesc desc, String location) {
    desc.setLocation(location);
  }

  @Override
  public Map<String, String> getNativeProperties(org.apache.hadoop.hive.ql.metadata.Table table) {
    Table origTable = IcebergTableUtil.getTable(conf, table.getTTable());
    Map<String, String> props = Maps.newHashMap();
    props.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(origTable.schema()));
    props.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(origTable.spec()));
    return props;
  }

  @Override
  public boolean shouldOverwrite(org.apache.hadoop.hive.ql.metadata.Table mTable, Operation operation) {
    return IcebergTableUtil.isCopyOnWriteMode(operation, mTable.getParameters()::getOrDefault);
  }

  @Override
  public void addResourcesForCreateTable(Map<String, String> tblProps, HiveConf hiveConf) {
    String metadataLocation = tblProps.get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    if (StringUtils.isNotEmpty(metadataLocation)) {
      SessionStateUtil.addResourceOrThrow(hiveConf, BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
          metadataLocation);
    }
  }

  /**
   * Check the operation type of all snapshots which are newer than the specified. The specified snapshot is excluded.
   * @param hmsTable table metadata stored in Hive Metastore
   * @param since the snapshot preceding the oldest snapshot which should be checked.
   *              The value null means all should be checked.
   * @return null if table is empty, true if all snapshots are {@link SnapshotContext.WriteOperationType#APPEND}s,
   * false otherwise.
   *
   * @deprecated
   * <br>Use {@link HiveStorageHandler#getSnapshotContexts(
   * org.apache.hadoop.hive.ql.metadata.Table hmsTable, SnapshotContext since)}
   * and check {@link SnapshotContext.WriteOperationType#APPEND}.equals({@link SnapshotContext#getOperation()}).
   */
  @Deprecated
  @Override
  public Boolean hasAppendsOnly(org.apache.hadoop.hive.ql.metadata.Table hmsTable, SnapshotContext since) {
    TableDesc tableDesc = Utilities.getTableDesc(hmsTable);
    Table table = IcebergTableUtil.getTable(conf, tableDesc.getProperties());
    return hasAppendsOnly(table.snapshots(), since);
  }

  @VisibleForTesting
  Boolean hasAppendsOnly(Iterable<Snapshot> snapshots, SnapshotContext since) {
    boolean foundSince = since == null;
    for (Snapshot snapshot : snapshots) {
      if (!foundSince) {
        if (snapshot.snapshotId() == since.getSnapshotId()) {
          foundSince = true;
        }
      } else {
        if (!DataOperations.APPEND.equals(snapshot.operation())) {
          return false;
        }
      }
    }

    if (foundSince) {
      return true;
    }

    return null;
  }

  @Override
  public void validatePartSpec(org.apache.hadoop.hive.ql.metadata.Table hmsTable, Map<String, String> partitionSpec,
      RewritePolicy policy) throws SemanticException {
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    List<PartitionField> partitionFields = IcebergTableUtil.getPartitionFields(table,
        policy != RewritePolicy.PARTITION);
    validatePartSpecImpl(hmsTable, partitionSpec, partitionFields);
  }

  private void validatePartSpecImpl(org.apache.hadoop.hive.ql.metadata.Table hmsTable,
      Map<String, String> partitionSpec, List<PartitionField> partitionFields) throws SemanticException {
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    if (hmsTable.getSnapshotRef() != null && IcebergTableUtil.hasUndergonePartitionEvolution(table)) {
      // for this case we rewrite the query as delete query, so validations would be done as part of delete.
      return;
    }

    if (table.spec().isUnpartitioned() && MapUtils.isNotEmpty(partitionSpec)) {
      throw new SemanticException("Writing data into a partition fails when the Iceberg table is unpartitioned.");
    }

    Map<String, Types.NestedField> mapOfPartColNamesWithTypes = Maps.newHashMap();
    for (PartitionField partField : partitionFields) {
      Types.NestedField field = table.schema().findField(partField.sourceId());
      mapOfPartColNamesWithTypes.put(field.name(), field);
    }

    for (Map.Entry<String, String> spec : partitionSpec.entrySet()) {
      Types.NestedField field = mapOfPartColNamesWithTypes.get(spec.getKey());
      Objects.requireNonNull(field, String.format("%s is not a partition column", spec.getKey()));
      // If the partition spec value is null, it's a dynamic partition column.
      if (spec.getValue() != null) {
        Object partKeyVal = Conversions.fromPartitionString(field.type(), spec.getValue());
        Objects.requireNonNull(partKeyVal,
            String.format("Partition spec value for column : %s is invalid", field.name()));
      }
    }
  }

  /**
   * A function to decide whether a given truncate query can perform a metadata delete or not.
   * If its not possible to perform metadata delete then try to perform a delete based on the mode.
   * The steps to decide whether truncate is possible is as follows - <br>
   * a. Create an expression based on the partition spec columns and partition spec values. <br>
   * b. Find files which match the expression using Apache Iceberg's FindFiles API. <br>
   * c. Do evaluation on whether the expression can match the partition value in the file. <br>
   * If for all files, the strict evaluation returns true, it means that we safely delete all files
   * by performing a metadata delete operation. If not, we must convert the truncate to delete query
   * which eventually performs a delete based on the mode.
   * @param hmsTable A Hive table instance.
   * @param partitionSpec Map containing partition specification given by user.
   * @return true if we can perform metadata delete, otherwise false.
   * @throws SemanticException Exception raised when a partition transform is being used
   * or when partition column is not present in the table.
   */
  @Override
  public boolean canUseTruncate(org.apache.hadoop.hive.ql.metadata.Table hmsTable, Map<String, String> partitionSpec)
      throws SemanticException {
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    if (MapUtils.isEmpty(partitionSpec) || !IcebergTableUtil.hasUndergonePartitionEvolution(table)) {
      return true;
    } else if (hmsTable.getSnapshotRef() != null) {
      return false;
    }

    Expression finalExp = IcebergTableUtil.generateExpressionFromPartitionSpec(table, partitionSpec, true);
    FindFiles.Builder builder = new FindFiles.Builder(table).withRecordsMatching(finalExp);
    Set<DataFile> dataFiles = Sets.newHashSet(builder.collect());
    boolean result = true;
    for (DataFile dataFile : dataFiles) {
      PartitionData partitionData = (PartitionData) dataFile.partition();
      Expression residual = ResidualEvaluator.of(table.spec(), finalExp, false)
          .residualFor(partitionData);
      if (!residual.isEquivalentTo(Expressions.alwaysTrue())) {
        result = false;
        break;
      }
    }
    return result;
  }

  @Override
  public List<Partition> getPartitions(org.apache.hadoop.hive.ql.metadata.Table hmsTable,
      Map<String, String> partitionSpec, boolean latestSpecOnly) throws SemanticException {
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    List<String> partNames = IcebergTableUtil.getPartitionNames(table, partitionSpec, latestSpecOnly);
    return IcebergTableUtil.convertNameToMetastorePartition(hmsTable, partNames);
  }

  public boolean isPartitioned(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    if (!hmsTable.getTTable().isSetId()) {
      return false;
    }
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    Snapshot snapshot = IcebergTableUtil.getTableSnapshot(table, hmsTable);

    boolean readsNonCurrentSnapshot = snapshot != null && !snapshot.equals(table.currentSnapshot());
    if (readsNonCurrentSnapshot && IcebergTableUtil.hasUndergonePartitionEvolution(table)) {
      return false;
    }
    return table.spec().isPartitioned();
  }

  @Override
  public Partition getPartition(org.apache.hadoop.hive.ql.metadata.Table table,
      Map<String, String> partitionSpec, RewritePolicy policy) throws SemanticException {
    validatePartSpec(table, partitionSpec, policy);
    return getPartitionImpl(table, partitionSpec);
  }

  private Partition getPartitionImpl(org.apache.hadoop.hive.ql.metadata.Table table,
      Map<String, String> partitionSpec) throws SemanticException {
    try {
      String partName = Warehouse.makePartName(partitionSpec, false);
      return new DummyPartition(table, partName, partitionSpec);
    } catch (MetaException e) {
      throw new SemanticException("Unable to construct name for dummy partition due to: ", e);
    }
  }

  /**
   * Returns a list of partitions which are corresponding to the table based on the partition spec provided.
   * @param hmsTable A Hive table instance.
   * @param partitionSpec Map containing partition specification.
   * @return A list of partition values which satisfies the partition spec provided corresponding to the table.
   * @throws SemanticException Exception raised when there is an issue performing a scan on the partitions table.
   */
  public List<String> getPartitionNames(org.apache.hadoop.hive.ql.metadata.Table hmsTable,
      Map<String, String> partitionSpec) throws SemanticException {
    Table icebergTable = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    return IcebergTableUtil.getPartitionNames(icebergTable, partitionSpec, false);
  }

  /**
   * A function to fetch the column information of the underlying column defined by the table format.
   * @param hmsTable A Hive table instance
   * @param colName Column name
   * @return An instance of ColumnInfo.
   * @throws SemanticException An exception is thrown when the column is not present, or if we unable to fetch
   * column type due to SerDeException, or if the associated field object inspector is not present.
   */
  @Override
  public ColumnInfo getColumnInfo(org.apache.hadoop.hive.ql.metadata.Table hmsTable, String colName)
      throws SemanticException {
    Table icebergTbl = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    Deserializer deserializer = hmsTable.getDeserializer();
    Types.NestedField field = icebergTbl.schema().findField(colName);
    if (field != null) {
      try {
        ObjectInspector fieldObjInspector = null;
        StructObjectInspector structObjectInspector = (StructObjectInspector) deserializer.getObjectInspector();
        for (StructField structField : structObjectInspector.getAllStructFieldRefs()) {
          if (field.name().equalsIgnoreCase(structField.getFieldName())) {
            fieldObjInspector = structField.getFieldObjectInspector();
            break;
          }
        }
        if (fieldObjInspector != null) {
          return new ColumnInfo(field.name(), fieldObjInspector, hmsTable.getTableName(), false);
        } else {
          throw new SemanticException(String.format("Unable to fetch column type of column %s " +
              "since we are not able to infer its object inspector.", colName));
        }
      } catch (SerDeException e) {
        throw new SemanticException(String.format("Unable to fetch column type of column %s due to: %s", colName, e));
      }
    } else {
      throw new SemanticException(String.format("Unable to find a column with the name: %s", colName));
    }
  }

  @Override
  public boolean canPerformMetadataDelete(org.apache.hadoop.hive.ql.metadata.Table hmsTable,
      String branchName, SearchArgument sarg) {
    Expression exp;
    try {
      exp = HiveIcebergFilterFactory.generateFilterExpression(sarg);
    } catch (UnsupportedOperationException e) {
      LOG.warn("Unable to create Iceberg filter, skipping metadata delete: ", e);
      return false;
    }
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());

    // The following code is inspired & copied from Iceberg's SparkTable.java#canDeleteUsingMetadata
    if (ExpressionUtil.selectsPartitions(exp, table, false)) {
      return true;
    }

    TableScan scan = table.newScan().filter(exp)
        .caseSensitive(false).includeColumnStats().ignoreResiduals();
    if (branchName != null) {
      scan.useRef(HiveUtils.getTableSnapshotRef(branchName));
    }

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Map<Integer, Evaluator> evaluators = Maps.newHashMap();
      StrictMetricsEvaluator metricsEvaluator =
          new StrictMetricsEvaluator(SnapshotUtil.schemaFor(table, branchName), exp);

      return Iterables.all(tasks, task -> {
        DataFile file = task.file();
        PartitionSpec spec = task.spec();
        Evaluator evaluator = evaluators.computeIfAbsent(spec.specId(), specId ->
            new Evaluator(spec.partitionType(), Projections.strict(spec).project(exp)));
        return evaluator.eval(file.partition()) || metricsEvaluator.eval(file);
      });
    } catch (IOException ioe) {
      LOG.warn("Failed to close task iterable", ioe);
      return false;
    }
  }

  @Override
  public List<FieldSchema> getPartitionKeys(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    if (!hmsTable.getTTable().isSetId()) {
      return Collections.emptyList();
    }
    Table icebergTable = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    return IcebergTableUtil.getPartitionKeys(icebergTable, icebergTable.spec().specId());
  }

  @Override
  public List<Partition> getPartitionsByExpr(org.apache.hadoop.hive.ql.metadata.Table hmsTable, ExprNodeDesc filter,
      Boolean latestSpecOnly) throws SemanticException {
    Expression exp = HiveIcebergInputFormat.getFilterExpr(conf, (ExprNodeGenericFuncDesc) filter);
    if (exp == null) {
      return ImmutableList.of();
    }
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    int tableSpecId = table.spec().specId();
    Set<Partition> partitions = Sets.newHashSet();

    TableScan scan = table.newScan().filter(exp)
        .caseSensitive(false).includeColumnStats().ignoreResiduals();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      FluentIterable.from(tasks).filter(task -> task.spec().isPartitioned()).forEach(task -> {
        DataFile file = task.file();
        PartitionSpec spec = task.spec();

        if (latestSpecOnly == null || latestSpecOnly && file.specId() == tableSpecId ||
              !latestSpecOnly && file.specId() != tableSpecId) {

          PartitionData partitionData = IcebergTableUtil.toPartitionData(task.partition(), spec.partitionType());
          String partName = spec.partitionToPath(partitionData);

          Map<String, String> partSpecMap = Maps.newLinkedHashMap();
          Warehouse.makeSpecFromName(partSpecMap, new Path(partName), null);

          DummyPartition partition = new DummyPartition(hmsTable, partName, partSpecMap);
          partitions.add(partition);
        }
      });
    } catch (IOException e) {
      throw new SemanticException(String.format("Error while fetching the partitions due to: %s", e));
    }
    return ImmutableList.copyOf(partitions);
  }

  @Override
  public boolean hasDataMatchingFilterExpr(org.apache.hadoop.hive.ql.metadata.Table hmsTable, ExprNodeDesc filter) {
    SearchArgument sarg = ConvertAstToSearchArg.create(conf, (ExprNodeGenericFuncDesc) filter);
    Expression exp = HiveIcebergFilterFactory.generateFilterExpression(sarg);
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    TableScan scan = table.newScan().filter(exp)
        .caseSensitive(false).includeColumnStats().ignoreResiduals();
    boolean result = false;

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      result = tasks.iterator().hasNext();
    } catch (IOException ioe) {
      LOG.warn("Failed to close task iterable", ioe);
    }
    return result;
  }

  @Override
  public boolean supportsMergeFiles() {
    return true;
  }

  @Override
  public List<FileStatus> getMergeTaskInputFiles(Properties properties) throws IOException {
    List<JobContext> jobContextList = IcebergMergeTaskProperties.getJobContexts(properties);
    if (jobContextList.isEmpty()) {
      return Collections.emptyList();
    }
    return new HiveIcebergOutputCommitter().getOutputFiles(jobContextList);
  }

  @Override
  public MergeTaskProperties getMergeTaskProperties(Properties properties) {
    return new IcebergMergeTaskProperties(properties);
  }

  @Override
  public void setMergeTaskDeleteProperties(TableDesc tableDesc) {
    tableDesc.setProperty(InputFormatConfig.OPERATION_TYPE_PREFIX + tableDesc.getTableName(),
            Operation.DELETE.name());
    tableDesc.setProperty(HiveCustomStorageHandlerUtils.WRITE_OPERATION_CONFIG_PREFIX +
            tableDesc.getTableName(), Operation.DELETE.name());
  }

  @Override
  public boolean hasUndergonePartitionEvolution(org.apache.hadoop.hive.ql.metadata.Table hmsTable) {
    Table table = IcebergTableUtil.getTable(conf, hmsTable.getTTable());
    return IcebergTableUtil.hasUndergonePartitionEvolution(table);
  }

  private static List<FieldSchema> schema(List<VirtualColumn> exprs) {
    return exprs.stream().map(v ->
        new FieldSchema(v.getName(), v.getTypeInfo().getTypeName(), ""))
      .collect(Collectors.toList());
  }

  private static List<FieldSchema> orderBy(VirtualColumn... exprs) {
    return schema(Arrays.asList(exprs));
  }
}
