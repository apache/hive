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

package org.apache.hadoop.hive.ql.parse;

import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESTATSDBCLASS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.StatsSetupConst.StatDB;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConf.ResultFileFormat;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.cache.results.QueryResultsCache;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.preinsert.PreInsertTableDesc;
import org.apache.hadoop.hive.ql.ddl.table.misc.properties.AlterTableUnsetPropertiesDesc;
import org.apache.hadoop.hive.ql.ddl.view.create.CreateMaterializedViewDesc;
import org.apache.hadoop.hive.ql.ddl.view.materialized.update.MaterializedViewUpdateDesc;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.ReduceField;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.io.arrow.ArrowColumnarBatchSerDe;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration.AnalyzeState;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LoadFileDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCardinalityViolation;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFSurrogateKey;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.NoOpFetchFormatter;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.thrift.ThriftJDBCBinarySerDe;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GenFileSinkPlan {
  protected static final Logger LOG = LoggerFactory.getLogger(GenFileSinkPlan.class.getName());

  private final Operator output;
  private final Map<Operator<? extends OperatorDesc>, OpParseContext> newOperatorMap = new HashMap<>();
  private final Map<String, String> idToTableNameMap = new HashMap<>();
  private final List<LoadTableDesc> loadTableWork = new ArrayList<>();
  private final List<LoadFileDesc> loadFileWork = new ArrayList<>();
  private final Set<FileSinkDesc> acidFileSinks = new HashSet<>();
  private final List<PotentialWriteEntity> writeEntityOutputs = new ArrayList<PotentialWriteEntity>();
  private final List<Task<?>> fileSinkTasks = new ArrayList<>();
  private final FileSinkDesc fileSinkDesc;
  private final List<ReduceSinkOperator> reduceSinkOperatorsAddedByEnforceBucketingSorting;
  private List<ColumnStatsAutoGatherContext> columnStatsAutoGatherContexts = ImmutableList.of();
  private boolean isMmCreate = false;
  private boolean isDirectInsert = false; // should we add files directly to the final path
  private MaterializedViewUpdateDesc materializedViewUpdateDesc = null;
  private int nextDestTableId;
  private AcidUtils.Operation acidOperation = null;
  private Table destinationTable;
  private LoadTableDesc ltd;
  private Path destinationPath = null; // the final destination directory
  private boolean needSetFsResultCache = false;
  private boolean setNoOpFetchFormatter = false;
  private boolean setAcidConfigParams = false;

  public GenFileSinkPlan(String dest, QB qb, Operator input,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap,
      ReadOnlySemanticAnalyzer sa
      ) throws SemanticException {
    newOperatorMap.putAll(operatorMap);
    Context ctx = sa.getContext();
    Hive db = sa.getDb();
    HiveConf conf = sa.getConf();
    HiveTxnManager txnMgr = sa.getTxnMgr();
    nextDestTableId = sa.getDestTableId();
    ImmutableList.Builder<ReduceSinkOperator> reduceSinkOpBuilder = ImmutableList.builder();

    RowResolver inputRR = newOperatorMap.get(input).getRowResolver();
    QBMetaData qbm = qb.getMetaData();
    Integer destType = qbm.getDestTypeForAlias(dest);

    boolean destTableIsTransactional;     // true for full ACID table and MM table
    boolean destTableIsFullAcid; // should the destination table be written to using ACID
    boolean destTableIsTemporary = false;
    boolean destTableIsMaterialization = false;
    Partition destinationPartition = null;// destination partition if any
    Path queryTmpdir = null; // the intermediate destination directory
    String moveTaskId = null;
    TableDesc tableDescriptor = null;
    StructObjectInspector specificRowObjectInspector = null;
    int currentTableId = 0;
    boolean isLocal = false;
    SortBucketRSCtx rsCtx = new SortBucketRSCtx();
    DynamicPartitionCtx dpCtx = null;
    ListBucketingCtx lbCtx = null;
    Map<String, String> partSpec = null;
    boolean isMmTable = false, isNonNativeTable = false;
    Long writeId = null;

    switch (destType.intValue()) {
    case QBMetaData.DEST_TABLE: {

      destinationTable = qbm.getDestTableForAlias(dest);
      destTableIsTransactional = AcidUtils.isTransactionalTable(destinationTable);
      destTableIsFullAcid = AcidUtils.isFullAcidTable(destinationTable);
      destTableIsTemporary = destinationTable.isTemporary();

      // Is the user trying to insert into a external tables
      checkExternalTable(conf, destinationTable);

      partSpec = qbm.getPartSpecForAlias(dest);
      destinationPath = destinationTable.getPath();

      checkImmutableTable(qb, destinationTable, destinationPath, false, conf);

      // Check for dynamic partitions.
      dpCtx = checkDynPart(qb, qbm, destinationTable, partSpec, dest, conf);

      isNonNativeTable = destinationTable.isNonNative();
      isMmTable = AcidUtils.isInsertOnlyTable(destinationTable.getParameters());
      AcidUtils.Operation acidOp = AcidUtils.Operation.NOT_ACID;
      // this table_desc does not contain the partitioning columns
      tableDescriptor = Utilities.getTableDesc(destinationTable);

      if (!isNonNativeTable) {
        if (destTableIsTransactional) {
          acidOp = SemanticAnalyzer.getAcidType(tableDescriptor.getOutputFileFormatClass(), dest,
              isMmTable, txnMgr);
        }
      }
      isDirectInsert = isDirectInsert(destTableIsFullAcid, acidOp, ctx, conf);
      acidOperation = acidOp;
      queryTmpdir = getTmpDir(isNonNativeTable, isMmTable, isDirectInsert, destinationPath, dpCtx, ctx, conf);
      moveTaskId = ctx.getMoveTaskId();
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("create filesink w/DEST_TABLE specifying " + queryTmpdir
            + " from " + destinationPath);
      }
      if (dpCtx != null) {
        // set the root of the temporary path where dynamic partition columns will populate
        dpCtx.setRootPath(queryTmpdir);
      }

      // Add NOT NULL constraint check
      GenConstraintsPlan genConstraintsPlan = new GenConstraintsPlan(dest, qb, input, newOperatorMap, sa);
      if (genConstraintsPlan.createdOperator()) {
        input = genConstraintsPlan.getOperator();
        newOperatorMap.put(input,
           new OpParseContext(genConstraintsPlan.getRowResolver()));
      }

      if (!qb.getIsQuery()) {
        GenConversionSelectOperator gcso = new GenConversionSelectOperator(dest, qb, input,
            destinationTable.getDeserializer(), dpCtx, destinationTable.getPartitionKeys(),
            destinationTable, ImmutableMap.copyOf(newOperatorMap), conf);
        if (gcso.createdOperator()) {
          input = gcso.getOperator();
          newOperatorMap.put(input,
             new OpParseContext(gcso.getRowResolver()));
        }
      }

      if (destinationTable.isMaterializedView() &&
          sa.getMVRebuildMode() == SemanticAnalyzer.MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD) {
        // Data organization (DISTRIBUTED, SORTED, CLUSTERED) for materialized view
        // TODO: We only do this for a full rebuild
        String sortColsStr = destinationTable.getProperty(Constants.MATERIALIZED_VIEW_SORT_COLUMNS);
        String distributeColsStr = destinationTable.getProperty(Constants.MATERIALIZED_VIEW_DISTRIBUTE_COLUMNS);
        if (sortColsStr != null || distributeColsStr != null) {
          GenMaterializedViewOrgPlan gmvop =
              new GenMaterializedViewOrgPlan(destinationTable, sortColsStr, distributeColsStr, inputRR, input);
          newOperatorMap.putAll(gmvop.getOperatorMap());
          input = gmvop.getOperator();
        }
      } else {
        // Add sorting/bucketing if needed
        GenBucketingSortingDest gbsd = new GenBucketingSortingDest(dest, input, qb, tableDescriptor,
            destinationTable, rsCtx, ImmutableMap.copyOf(newOperatorMap), sa);
        input = gbsd.getOperator();
        newOperatorMap.putAll(gbsd.getOperatorMap());
        reduceSinkOpBuilder.addAll(
            gbsd.getReduceSinkOperatorsAddedByEnforceBucketingSorting());
      }

      currentTableId = nextDestTableId;
      idToTableNameMap.put(String.valueOf(nextDestTableId++), destinationTable.getTableName());

      // Create the work for moving the table
      // NOTE: specify Dynamic partitions in dest_tab for WriteEntity
      if (!isNonNativeTable || destinationTable.getStorageHandler().commitInMoveTask()) {
        if (destTableIsTransactional) {
          acidOp = SemanticAnalyzer.getAcidType(tableDescriptor.getOutputFileFormatClass(), dest,
              isMmTable, txnMgr);
          this.setAcidConfigParams = true;
        } else {
          lbCtx = BaseSemanticAnalyzer.constructListBucketingCtx(destinationTable.getSkewedColNames(),
              destinationTable.getSkewedColValues(), destinationTable.getSkewedColValueLocationMaps(),
              destinationTable.isStoredAsSubDirectories());
        }
        try {
          if (ctx.getExplainConfig() != null) {
            writeId = null; // For explain plan, txn won't be opened and doesn't make sense to allocate write id
          } else {
            if (isMmTable) {
              writeId = txnMgr.getTableWriteId(destinationTable.getDbName(), destinationTable.getTableName());
            } else {
              writeId = acidOp == Operation.NOT_ACID ? null :
                      txnMgr.getTableWriteId(destinationTable.getDbName(), destinationTable.getTableName());
            }
          }
        } catch (LockException ex) {
          throw new SemanticException("Failed to allocate write Id", ex);
        }
        boolean isReplace = !qb.getParseInfo().isInsertIntoTable(
            destinationTable.getDbName(), destinationTable.getTableName(), destinationTable.getSnapshotRef());
        ltd = new LoadTableDesc(queryTmpdir, tableDescriptor, dpCtx, acidOp, isReplace, writeId);
        if (writeId != null) {
          ltd.setStmtId(txnMgr.getCurrentStmtId());
        }
        ltd.setMoveTaskId(moveTaskId);
        // For Acid table, Insert Overwrite shouldn't replace the table content. We keep the old
        // deltas and base and leave them up to the cleaner to clean up
        boolean isInsertInto = qb.getParseInfo().isInsertIntoTable(
            destinationTable.getDbName(), destinationTable.getTableName(), destinationTable.getSnapshotRef());
        LoadFileType loadType;
        if (isDirectInsert) {
          loadType = LoadFileType.IGNORE;
        } else if (!isInsertInto && !destTableIsTransactional) {
          loadType = LoadFileType.REPLACE_ALL;
        } else {
          loadType = LoadFileType.KEEP_EXISTING;
        }
        ltd.setLoadFileType(loadType);
        ltd.setInsertOverwrite(!isInsertInto);
        ltd.setIsDirectInsert(isDirectInsert);
        ltd.setLbCtx(lbCtx);
        loadTableWork.add(ltd);
      } else {
        // This is a non-native table.
        // We need to set stats as inaccurate.
        fileSinkTasks.add(setStatsForNonNativeTable(destinationTable.getDbName(), destinationTable.getTableName(),
            sa.getInputs(), sa.getOutputs()));
        // true if it is insert overwrite.
        boolean overwrite = !qb.getParseInfo().isInsertIntoTable(destinationTable.getDbName(), destinationTable.getTableName(),
            destinationTable.getSnapshotRef());
        fileSinkTasks.add(createPreInsertDesc(destinationTable, overwrite, sa.getInputs(), sa.getOutputs()));

        ltd = new LoadTableDesc(queryTmpdir, tableDescriptor, partSpec == null ? ImmutableMap.of() : partSpec);
        ltd.setInsertOverwrite(overwrite);
        ltd.setLoadFileType(overwrite ? LoadFileType.REPLACE_ALL : LoadFileType.KEEP_EXISTING);
      }

      if (destinationTable.isMaterializedView()) {
        materializedViewUpdateDesc = new MaterializedViewUpdateDesc(
            destinationTable.getFullyQualifiedName(), false, false, true);
      }

      WriteEntity output = generateTableWriteEntity(
          dest, destinationTable, partSpec, ltd, dpCtx, sa.allowOutputMultipleTimes());
      ctx.getLoadTableOutputMap().put(ltd, output);
      break;
    }
    case QBMetaData.DEST_PARTITION: {

      destinationPartition = qbm.getDestPartitionForAlias(dest);
      destinationTable = destinationPartition.getTable();
      destTableIsTransactional = AcidUtils.isTransactionalTable(destinationTable);
      destTableIsFullAcid = AcidUtils.isFullAcidTable(destinationTable);

      checkExternalTable(conf, destinationTable);

      Path partPath = destinationPartition.getDataLocation();

      checkImmutableTable(qb, destinationTable, partPath, true, conf);

      // Previous behavior (HIVE-1707) used to replace the partition's dfs with the table's dfs.
      // The changes in HIVE-19891 appears to no longer support that behavior.
      destinationPath = partPath;

      if (MetaStoreUtils.isArchived(destinationPartition.getTPartition())) {
        try {
          String conflictingArchive = ArchiveUtils.conflictingArchiveNameOrNull(
                  db, destinationTable, destinationPartition.getSpec());
          String message = String.format("Insert conflict with existing archive: %s",
                  conflictingArchive);
          throw new SemanticException(message);
        } catch (SemanticException err) {
          throw err;
        } catch (HiveException err) {
          throw new SemanticException(err);
        }
      }

      isNonNativeTable = destinationTable.isNonNative();
      isMmTable = AcidUtils.isInsertOnlyTable(destinationTable.getParameters());
      AcidUtils.Operation acidOp = AcidUtils.Operation.NOT_ACID;
      // this table_desc does not contain the partitioning columns
      tableDescriptor = Utilities.getTableDesc(destinationTable);

      if (!isNonNativeTable) {
        if (destTableIsTransactional) {
          acidOp = SemanticAnalyzer.getAcidType(tableDescriptor.getOutputFileFormatClass(), dest,
              isMmTable, txnMgr);
        }
      }
      isDirectInsert = isDirectInsert(destTableIsFullAcid, acidOp, ctx, conf);
      acidOperation = acidOp;
      queryTmpdir = getTmpDir(isNonNativeTable, isMmTable, isDirectInsert, destinationPath, null, ctx, conf);
      moveTaskId = ctx.getMoveTaskId();
      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("create filesink w/DEST_PARTITION specifying "
            + queryTmpdir + " from " + destinationPath);
      }

      // Add NOT NULL constraint check
      GenConstraintsPlan genConstraintsPlan = new GenConstraintsPlan(dest, qb, input, newOperatorMap, sa);
      if (genConstraintsPlan.createdOperator()) {
        newOperatorMap.put(genConstraintsPlan.getOperator(),
           new OpParseContext(genConstraintsPlan.getRowResolver()));
      }
      input = genConstraintsPlan.getOperator();

      if (!qb.getIsQuery()) {
        GenConversionSelectOperator gcso = new GenConversionSelectOperator(dest, qb, input,
            destinationTable.getDeserializer(), dpCtx, null, destinationTable,
            ImmutableMap.copyOf(newOperatorMap), conf);
        if (gcso.createdOperator()) {
          input = gcso.getOperator();
          newOperatorMap.put(input,
             new OpParseContext(gcso.getRowResolver()));
        }
      }

      if (destinationTable.isMaterializedView() &&
          sa.getMVRebuildMode() == SemanticAnalyzer.MaterializationRebuildMode.INSERT_OVERWRITE_REBUILD) {
        // Data organization (DISTRIBUTED, SORTED, CLUSTERED) for materialized view
        // TODO: We only do this for a full rebuild
        String sortColsStr = destinationTable.getProperty(Constants.MATERIALIZED_VIEW_SORT_COLUMNS);
        String distributeColsStr = destinationTable.getProperty(Constants.MATERIALIZED_VIEW_DISTRIBUTE_COLUMNS);
        if (sortColsStr != null || distributeColsStr != null) {
          GenMaterializedViewOrgPlan gmvop =
              new GenMaterializedViewOrgPlan(destinationTable, sortColsStr, distributeColsStr, inputRR, input);
          newOperatorMap.putAll(gmvop.getOperatorMap());
          input = gmvop.getOperator();
        }
      } else {
        // Add sorting/bucketing if needed
        GenBucketingSortingDest gbsd = new GenBucketingSortingDest(dest, input, qb, tableDescriptor,
            destinationTable, rsCtx, ImmutableMap.copyOf(newOperatorMap), sa);
        input = gbsd.getOperator();
        newOperatorMap.putAll(gbsd.getOperatorMap());
        reduceSinkOpBuilder.addAll(
            gbsd.getReduceSinkOperatorsAddedByEnforceBucketingSorting());
      }

      currentTableId = nextDestTableId;
      idToTableNameMap.put(String.valueOf(nextDestTableId++), destinationTable.getTableName());

      if (destTableIsTransactional) {
        acidOp = SemanticAnalyzer.getAcidType(tableDescriptor.getOutputFileFormatClass(), dest,
            isMmTable, txnMgr);
        this.setAcidConfigParams = true;
      } else {
        // Transactional tables can't be list bucketed or have skewed cols
        lbCtx = BaseSemanticAnalyzer.constructListBucketingCtx(destinationPartition.getSkewedColNames(),
            destinationPartition.getSkewedColValues(), destinationPartition.getSkewedColValueLocationMaps(),
            destinationPartition.isStoredAsSubDirectories());
      }
      try {
        if (ctx.getExplainConfig() != null) {
          writeId = null; // For explain plan, txn won't be opened and doesn't make sense to allocate write id
        } else {
          if (isMmTable) {
            writeId = txnMgr.getTableWriteId(destinationTable.getDbName(), destinationTable.getTableName());
          } else {
            writeId = (acidOp == Operation.NOT_ACID) ? null :
                    txnMgr.getTableWriteId(destinationTable.getDbName(), destinationTable.getTableName());
          }
        }
      } catch (LockException ex) {
        throw new SemanticException("Failed to allocate write Id", ex);
      }
      ltd = new LoadTableDesc(queryTmpdir, tableDescriptor, destinationPartition.getSpec(), acidOp, writeId);
      if (writeId != null) {
        ltd.setStmtId(txnMgr.getCurrentStmtId());
      }
      // For the current context for generating File Sink Operator, it is either INSERT INTO or INSERT OVERWRITE.
      // So the next line works.
      boolean isInsertInto = !qb.getParseInfo().isDestToOpTypeInsertOverwrite(dest);
      // For Acid table, Insert Overwrite shouldn't replace the table content. We keep the old
      // deltas and base and leave them up to the cleaner to clean up
      LoadFileType loadType;
      if (isDirectInsert) {
        loadType = LoadFileType.IGNORE;
      } else if (!isInsertInto && !destTableIsTransactional) {
        loadType = LoadFileType.REPLACE_ALL;
      } else {
        loadType = LoadFileType.KEEP_EXISTING;
      }
      ltd.setLoadFileType(loadType);
      ltd.setInsertOverwrite(!isInsertInto);
      ltd.setIsDirectInsert(isDirectInsert);
      ltd.setLbCtx(lbCtx);
      ltd.setMoveTaskId(moveTaskId);

      loadTableWork.add(ltd);

      writeEntityOutputs.add(new PotentialWriteEntity(
          new WriteEntity(destinationPartition,determineWriteType(ltd, dest)),
          false,
          ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES.getMsg(destinationTable.getTableName() +
              "@" + destinationPartition.getName())));
     break;
    }
    case QBMetaData.DEST_LOCAL_FILE:
      isLocal = true;
      // fall through
    case QBMetaData.DEST_DFS_FILE: {
      destinationPath = getDestinationFilePath(sa, qbm.getDestFileForAlias(dest), isMmTable);
      // CTAS case: the file output format and serde are defined by the create
      // table command rather than taking the default value
      List<FieldSchema> fieldSchemas = null;
      List<FieldSchema> partitionColumns = null;
      List<String> partitionColumnNames = null;
      List<FieldSchema> sortColumns = null;
      List<String> sortColumnNames = null;
      List<FieldSchema> distributeColumns = null;
      List<String> distributeColumnNames = null;
      List<ColumnInfo> fileSinkColInfos = null;
      List<ColumnInfo> sortColInfos = null;
      List<ColumnInfo> distributeColInfos = null;
      TableName tableName = null;
      Map<String, String> tblProps = null;
      CreateTableDesc tblDesc = qb.getTableDesc();
      CreateMaterializedViewDesc viewDesc = qb.getViewDesc();
      boolean createTableUseSuffix = false;
      if (tblDesc != null) {
        fieldSchemas = new ArrayList<>();
        partitionColumns = new ArrayList<>();
        partitionColumnNames = tblDesc.getPartColNames();
        fileSinkColInfos = new ArrayList<>();
        destTableIsTemporary = tblDesc.isTemporary();
        destTableIsMaterialization = tblDesc.isMaterialization();
        tableName = TableName.fromString(tblDesc.getDbTableName(), null, tblDesc.getDatabaseName());
        tblProps = tblDesc.getTblProps();
        // Add suffix only when required confs are present
        // and user has not specified a location to the table.
        createTableUseSuffix = (HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX)
                || HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED))
                && tblDesc.getLocation() == null;
      } else if (viewDesc != null) {
        fieldSchemas = new ArrayList<>();
        partitionColumns = new ArrayList<>();
        partitionColumnNames = viewDesc.getPartColNames();
        sortColumns = new ArrayList<>();
        sortColumnNames = viewDesc.getSortColNames();
        distributeColumns = new ArrayList<>();
        distributeColumnNames = viewDesc.getDistributeColNames();
        fileSinkColInfos = new ArrayList<>();
        sortColInfos = new ArrayList<>();
        distributeColInfos = new ArrayList<>();
        destTableIsTemporary = false;
        destTableIsMaterialization = false;
        tableName = HiveTableName.ofNullableWithNoDefault(viewDesc.getViewName());
        tblProps = viewDesc.getTblProps();
        // Add suffix only when required confs are present
        // and user has not specified a location to the table.
        createTableUseSuffix = (HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_CREATE_TABLE_USE_SUFFIX)
                || HiveConf.getBoolVar(conf, ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED))
                && viewDesc.getLocation() == null;
      }

      destTableIsTransactional = tblProps != null && AcidUtils.isTablePropertyTransactional(tblProps);
      if (destTableIsTransactional) {
        isNonNativeTable = MetaStoreUtils.isNonNativeTable(tblProps);
        boolean isCtas = tblDesc != null && tblDesc.isCTAS();
        boolean isCMV = viewDesc != null && qb.isMaterializedView();
        isMmTable = isMmCreate = AcidUtils.isInsertOnlyTable(tblProps);
        if (!isNonNativeTable && !destTableIsTemporary && (isCtas || isCMV)) {
          destTableIsFullAcid = AcidUtils.isFullAcidTable(tblProps);
          acidOperation = SemanticAnalyzer.getAcidType(dest);
          isDirectInsert = isDirectInsert(destTableIsFullAcid, acidOperation, ctx, conf);
          if (isDirectInsert || isMmTable) {
            destinationPath = getCtasOrCMVLocation(tblDesc, viewDesc, createTableUseSuffix, ctx, db, conf);
            if (createTableUseSuffix) {
              if (tblDesc != null) {
                tblDesc.getTblProps().put(SOFT_DELETE_TABLE, Boolean.TRUE.toString());
              } else {
                viewDesc.getTblProps().put(SOFT_DELETE_TABLE, Boolean.TRUE.toString());
              }
            }
            // Set the location in context for possible rollback.
            ctx.setLocation(destinationPath);
            // Setting the location so that metadata transformers
            // does not change the location later while creating the table.
            if (tblDesc != null) {
              tblDesc.setLocation(destinationPath.toString());
            } else {
              viewDesc.setLocation(destinationPath.toString());
            }
          } else {
            // Set the location in context for possible rollback.
            ctx.setLocation(getCtasOrCMVLocation(tblDesc, viewDesc, createTableUseSuffix, ctx, db, conf));
          }
        }
        try {
          if (ctx.getExplainConfig() != null) {
            writeId = 0L; // For explain plan, txn won't be opened and doesn't make sense to allocate write id
          } else {
            writeId = txnMgr.getTableWriteId(tableName.getDb(), tableName.getTable());
          }
        } catch (LockException ex) {
          throw new SemanticException("Failed to allocate write Id", ex);
        }
        if (isMmTable || isDirectInsert) {
          if (tblDesc != null) {
            tblDesc.setInitialWriteId(writeId);
          } else {
            viewDesc.setInitialWriteId(writeId);
          }
        }
      }

      // Check for dynamic partitions.
      final String cols, colTypes;
      final boolean isPartitioned;
      if (dpCtx != null) {
        throw new SemanticException("Dynamic partition context has already been created, this should not happen");
      }
      if (!CollectionUtils.isEmpty(partitionColumnNames)) {
        ColsAndTypes ct = deriveFileSinkColTypes(
            inputRR, partitionColumnNames, sortColumnNames, distributeColumnNames, fieldSchemas, partitionColumns,
            sortColumns, distributeColumns, fileSinkColInfos, sortColInfos, distributeColInfos, sa.isCBOExecuted());
        cols = ct.cols;
        colTypes = ct.colTypes;
        dpCtx = new DynamicPartitionCtx(partitionColumnNames,
            conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME),
            conf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTSPERNODE));
        qbm.setDPCtx(dest, dpCtx);
        isPartitioned = true;
      } else {
        ColsAndTypes ct = deriveFileSinkColTypes(
            inputRR, sortColumnNames, distributeColumnNames, fieldSchemas, sortColumns, distributeColumns,
            sortColInfos, distributeColInfos, sa.isCBOExecuted());
        cols = ct.cols;
        colTypes = ct.colTypes;
        isPartitioned = false;
      }

      if (isLocal) {
        assert !isMmTable;
        // for local directory - we always write to map-red intermediate
        // store and then copy to local fs
        queryTmpdir = ctx.getMRTmpPath();
        if (dpCtx != null && dpCtx.getSPPath() != null) {
          queryTmpdir = new Path(queryTmpdir, dpCtx.getSPPath());
        }
      } else {
        // otherwise write to the file system implied by the directory
        // no copy is required. we may want to revisit this policy in future
        try {
          Path qPath = FileUtils.makeQualified(destinationPath, conf);
          queryTmpdir = getTmpDir(false, isMmTable, isDirectInsert, qPath, dpCtx, ctx, conf);
        } catch (Exception e) {
          throw new SemanticException("Error creating "
              + destinationPath, e);
        }
      }
      // set the root of the temporary path where dynamic partition columns will populate
      if (dpCtx != null) {
        dpCtx.setRootPath(queryTmpdir);
      }

      if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
        Utilities.FILE_OP_LOGGER.trace("Setting query directory " + queryTmpdir
            + " from " + destinationPath + " (" + isMmTable + ")");
      }
      // update the create table descriptor with the resulting schema.
      if (tblDesc != null) {
        tblDesc.setCols(new ArrayList<>(fieldSchemas));
        tblDesc.setPartCols(new ArrayList<>(partitionColumns));
      } else if (viewDesc != null) {
        viewDesc.setSchema(new ArrayList<>(fieldSchemas));
        viewDesc.setPartCols(new ArrayList<>(partitionColumns));
        if (viewDesc.isOrganized()) {
          viewDesc.setSortCols(new ArrayList<>(sortColumns));
          viewDesc.setDistributeCols(new ArrayList<>(distributeColumns));
        }
      }

      boolean isDestTempFile = true;
      if (ctx.isMRTmpFileURI(destinationPath.toUri().toString()) == false
          && ctx.isResultCacheDir(destinationPath) == false) {
        // not a temp dir and not a result cache dir
        idToTableNameMap.put(String.valueOf(nextDestTableId), destinationPath.toUri().toString());
        currentTableId = nextDestTableId;
        nextDestTableId++;
        isDestTempFile = false;
      }

      try {
        if (tblDesc == null) {
          if (viewDesc != null) {
            if (viewDesc.getStorageHandler() != null) {
              viewDesc.setLocation(getCtasOrCMVLocation(tblDesc, viewDesc, createTableUseSuffix, ctx, db, conf).toString());
            }
            tableDescriptor = PlanUtils.getTableDesc(viewDesc, cols, colTypes);
          } else if (qb.getIsQuery()) {
            Class<? extends Deserializer> serdeClass = LazySimpleSerDe.class;
            String fileFormat = conf.getResultFileFormat().toString();
            if (SessionState.get().getIsUsingThriftJDBCBinarySerDe()) {
              serdeClass = ThriftJDBCBinarySerDe.class;
              fileFormat = ResultFileFormat.SEQUENCEFILE.toString();
              // Set the fetch formatter to be a no-op for the ListSinkOperator, since we'll
              // write out formatted thrift objects to SequenceFile
              this.setNoOpFetchFormatter = true;
              conf.set(SerDeUtils.LIST_SINK_OUTPUT_FORMATTER, NoOpFetchFormatter.class.getName());
            } else if (fileFormat.equals(PlanUtils.LLAP_OUTPUT_FORMAT_KEY)) {
              // If this output format is Llap, check to see if Arrow is requested
              boolean useArrow = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_OUTPUT_FORMAT_ARROW);
              serdeClass = useArrow ? ArrowColumnarBatchSerDe.class : LazyBinarySerDe2.class;
            }
            tableDescriptor = PlanUtils.getDefaultQueryOutputTableDesc(cols, colTypes, fileFormat,
                serdeClass);
          } else {
            tableDescriptor = PlanUtils.getDefaultTableDesc(qb.getDirectoryDesc(), cols, colTypes);
          }
        } else {
          if (tblDesc.isCTAS() && tblDesc.getStorageHandler() != null) {
            tblDesc.setLocation(getCtasOrCMVLocation(tblDesc, viewDesc, createTableUseSuffix, ctx, db, conf).toString());
          }
          tableDescriptor = PlanUtils.getTableDesc(tblDesc, cols, colTypes);
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }

      // We need a specific rowObjectInspector in this case
      try {
        specificRowObjectInspector =
            (StructObjectInspector) tableDescriptor.getDeserializer(conf).getObjectInspector();
      } catch (Exception e) {
        throw new SemanticException(e.getMessage(), e);
      }

      boolean isDfsDir = (destType == QBMetaData.DEST_DFS_FILE);

      try {
        if (tblDesc != null) {
          Table t = tblDesc.toTable(conf);
          destinationTable = tblDesc.isMaterialization() ? t : db.getTranslateTableDryrun(t.getTTable());
        } else {
          destinationTable = viewDesc != null ? viewDesc.toTable(conf) : null;
        }
      } catch (HiveException e) {
        throw new SemanticException(e);
      }

      destTableIsFullAcid = AcidUtils.isFullAcidTable(destinationTable);

      // Data organization (DISTRIBUTED, SORTED, CLUSTERED) for materialized view
      if (viewDesc != null && viewDesc.isOrganized()) {
        GenMaterializedViewOrgPlan gmvop =
            new GenMaterializedViewOrgPlan(sortColInfos, distributeColInfos, inputRR, input);
        newOperatorMap.putAll(gmvop.getOperatorMap());
        input = gmvop.getOperator();
      }

      moveTaskId = ctx.getMoveTaskId();

      if (isPartitioned) {
        // Create a SELECT that may reorder the columns if needed
        RowResolver rowResolver = new RowResolver();
        List<ExprNodeDesc> columnExprs = new ArrayList<>();
        List<String> colNames = new ArrayList<>();
        Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
        for (int i = 0; i < fileSinkColInfos.size(); i++) {
          ColumnInfo ci = fileSinkColInfos.get(i);
          ExprNodeDesc columnExpr = new ExprNodeColumnDesc(ci);
          String name = HiveConf.getColumnInternalName(i);
          rowResolver.put("", name, new ColumnInfo(name, columnExpr.getTypeInfo(), "", false));
          columnExprs.add(columnExpr);
          colNames.add(name);
          colExprMap.put(name, columnExpr);
        }

        input = createOperator(new SelectDesc(columnExprs, colNames),
            new RowSchema(rowResolver.getColumnInfos()), input);
        newOperatorMap.put(input, new OpParseContext(rowResolver));

        input.setColumnExprMap(colExprMap);

        // If this is a partitioned CTAS or MV statement, we are going to create a LoadTableDesc
        // object. Although the table does not exist in metastore, we will swap the CreateTableTask
        // and MoveTask resulting from this LoadTable so in this specific case, first we create
        // the metastore table, then we move and commit the partitions. At least for the time being,
        // this order needs to be enforced because metastore expects a table to exist before we can
        // add any partitions to it.
        isNonNativeTable = tableDescriptor.isNonNative();
        if (!isNonNativeTable || destinationTable.getStorageHandler().commitInMoveTask()) {
          AcidUtils.Operation acidOp = AcidUtils.Operation.NOT_ACID;
          if (destTableIsTransactional) {
            acidOp = SemanticAnalyzer.getAcidType(tableDescriptor.getOutputFileFormatClass(), dest,
                isMmTable, txnMgr);
            this.setAcidConfigParams = true;
          }
          // isReplace = false in case concurrent operation is executed
          ltd = new LoadTableDesc(queryTmpdir, tableDescriptor, dpCtx, acidOp, false, writeId);
          if (writeId != null) {
            ltd.setStmtId(txnMgr.getCurrentStmtId());
          }
          ltd.setLoadFileType(LoadFileType.KEEP_EXISTING);
          ltd.setInsertOverwrite(false);
          ltd.setIsDirectInsert(isDirectInsert);
          loadTableWork.add(ltd);
        } else {
          // This is a non-native table.
          // We need to set stats as inaccurate.
          fileSinkTasks.add(setStatsForNonNativeTable(tableDescriptor.getDbName(), tableDescriptor.getTableName(),
              sa.getInputs(), sa.getOutputs()));
          ltd = new LoadTableDesc(queryTmpdir, tableDescriptor, dpCtx.getPartSpec());
          ltd.setInsertOverwrite(false);
          ltd.setLoadFileType(LoadFileType.KEEP_EXISTING);
        }
        ltd.setMoveTaskId(moveTaskId);
        ltd.setMdTable(destinationTable);
        WriteEntity output = generateTableWriteEntity(
            dest, destinationTable, dpCtx.getPartSpec(), ltd, dpCtx, sa.allowOutputMultipleTimes());
        ctx.getLoadTableOutputMap().put(ltd, output);
      } else {
        // Create LFD even for MM CTAS - it's a no-op move, but it still seems to be used for stats.
        LoadFileDesc loadFileDesc = new LoadFileDesc(tblDesc, viewDesc, queryTmpdir, destinationPath, isDfsDir, cols,
            colTypes,
            destTableIsFullAcid ?//there is a change here - prev version had 'transactional', one before 'acid'
                Operation.INSERT : Operation.NOT_ACID,
            isMmCreate);
        loadFileDesc.setMoveTaskId(moveTaskId);
        loadFileWork.add(loadFileDesc);
        try {
          Path qualifiedPath = conf.getBoolVar(ConfVars.HIVE_RANGER_USE_FULLY_QUALIFIED_URL) ?
                  destinationPath.getFileSystem(conf).makeQualified(destinationPath) : destinationPath;
          WriteEntity we = new WriteEntity(qualifiedPath, !isDfsDir, isDestTempFile);
          writeEntityOutputs.add(new PotentialWriteEntity(we, false,
              ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES.getMsg(destinationPath.toUri().toString())));
        } catch (IOException ex) {
          throw new SemanticException("Error while getting the full qualified path for the given directory: " + ex.getMessage());
        }
      }
      break;
    }
    default:
      throw new SemanticException("Unknown destination type: " + destType);
    }

    if (!(destType == QBMetaData.DEST_DFS_FILE && qb.getIsQuery())
        && destinationTable != null && destinationTable.getStorageHandler() != null) {
      try {
        GenConversionSelectOperator gcso = new GenConversionSelectOperator(dest, qb, input,
            destinationTable.getDeserializer(), dpCtx, null, destinationTable,
            ImmutableMap.copyOf(newOperatorMap), conf);
        if (gcso.createdOperator()) {
          input = gcso.getOperator();
          newOperatorMap.put(input,
             new OpParseContext(gcso.getRowResolver()));
        }
      } catch (Exception e) {
        throw new SemanticException(e);
      }
    }

    inputRR = newOperatorMap.get(input).getRowResolver();

    List<ColumnInfo> vecCol = new ArrayList<ColumnInfo>();

    if (SemanticAnalyzer.updating(dest) || SemanticAnalyzer.deleting(dest)) {
      if (AcidUtils.isNonNativeAcidTable(destinationTable, true)) {
        destinationTable.getStorageHandler().acidVirtualColumns().stream()
            .map(col -> new ColumnInfo(col.getName(), col.getTypeInfo(), "", true))
            .forEach(vecCol::add);
      } else {
        vecCol.add(new ColumnInfo(VirtualColumn.ROWID.getName(), VirtualColumn.ROWID.getTypeInfo(),
            "", true));
      }
    } else {
      try {
        // If we already have a specific inspector (view or directory as a target) use that
        // Otherwise use the table deserializer to get the inspector
        StructObjectInspector rowObjectInspector = specificRowObjectInspector != null ? specificRowObjectInspector :
            (StructObjectInspector) destinationTable.getDeserializer().getObjectInspector();
        List<? extends StructField> fields = rowObjectInspector
            .getAllStructFieldRefs();
        for (StructField field : fields) {
          vecCol.add(new ColumnInfo(field.getFieldName(), TypeInfoUtils
              .getTypeInfoFromObjectInspector(field
                  .getFieldObjectInspector()), "", false));
        }
      } catch (Exception e) {
        throw new SemanticException(e.getMessage(), e);
      }
    }

    RowSchema fsRS = new RowSchema(vecCol);

    // The output files of a FileSink can be merged if they are either not being written to a table
    // or are being written to a table which is not bucketed
    // and table the table is not sorted
    boolean canBeMerged = (destinationTable == null || !((destinationTable.getNumBuckets() > 0) ||
        (destinationTable.getSortCols() != null && destinationTable.getSortCols().size() > 0)));

    // If this table is working with ACID semantics, turn off merging
    canBeMerged &= !destTableIsFullAcid;

    // Generate the partition columns from the parent input
    if (destType == QBMetaData.DEST_TABLE || destType == QBMetaData.DEST_PARTITION) {
      genPartnCols(dest, input, qb, tableDescriptor, destinationTable, rsCtx, newOperatorMap, conf);
    }

    fileSinkDesc = createFileSinkDesc(dest, tableDescriptor, destinationPartition,
        destinationPath, currentTableId, destTableIsFullAcid, destTableIsTemporary,//this was 1/4 acid
        destTableIsMaterialization, queryTmpdir, rsCtx, dpCtx, lbCtx, fsRS,
        canBeMerged, destinationTable, writeId, isMmCreate, destType, qb, isDirectInsert, acidOperation, moveTaskId,
        ctx, conf);

    if (fileSinkDesc.getInsertOverwrite()) {
      if (ltd != null) {
        ltd.setInsertOverwrite(true);
      }
    }
    if (null != tableDescriptor && useBatchingSerializer(tableDescriptor.getSerdeClassName(), conf)) {
      fileSinkDesc.setIsUsingBatchingSerDe(true);
    } else {
      fileSinkDesc.setIsUsingBatchingSerDe(false);
    }

    output = createOperator(fileSinkDesc, fsRS, input);
    newOperatorMap.put(output, new OpParseContext(inputRR));

    setWriteIdForSurrogateKeys(ltd, input);

    LOG.debug("Created FileSink Plan for clause: {}dest_path: {} row schema: {}", dest, destinationPath, inputRR);

    FileSinkOperator fso = (FileSinkOperator) output;
    fso.getConf().setTable(destinationTable);
    // the following code is used to collect column stats when
    // hive.stats.autogather=true
    // and it is an insert overwrite or insert into table
    if (conf.getBoolVar(ConfVars.HIVESTATSAUTOGATHER)
        && conf.getBoolVar(ConfVars.HIVESTATSCOLAUTOGATHER)
        && sa.enableColumnStatsCollecting()
        && destinationTable != null
        && (!destinationTable.isNonNative() || destinationTable.getStorageHandler().commitInMoveTask())
        && !destTableIsTemporary && !destTableIsMaterialization
        && ColumnStatsAutoGatherContext.canRunAutogatherStats(fso)) {
      if (destType == QBMetaData.DEST_TABLE) {
        columnStatsAutoGatherContexts = ImmutableList.of(
            genAutoColumnStatsGatheringPipeline(destinationTable, partSpec, input,
                qb.getParseInfo().isInsertIntoTable(destinationTable.getDbName(), destinationTable.getTableName(),
                destinationTable.getSnapshotRef()), false, ctx, conf, newOperatorMap));
      } else if (destType == QBMetaData.DEST_PARTITION) {
        columnStatsAutoGatherContexts = ImmutableList.of(
            genAutoColumnStatsGatheringPipeline(destinationTable, destinationPartition.getSpec(), input,
                qb.getParseInfo().isInsertIntoTable(destinationTable.getDbName(), destinationTable.getTableName(),
                destinationTable.getSnapshotRef()), false, ctx, conf, newOperatorMap));
      } else if (destType == QBMetaData.DEST_LOCAL_FILE || destType == QBMetaData.DEST_DFS_FILE) {
        // CTAS or CMV statement
        columnStatsAutoGatherContexts = ImmutableList.of(genAutoColumnStatsGatheringPipeline(destinationTable,
            null, input, false, true, ctx, conf, newOperatorMap));
      }
    }
    reduceSinkOperatorsAddedByEnforceBucketingSorting = reduceSinkOpBuilder.build();
  }

  private static void checkExternalTable(HiveConf conf, Table dest_tab) throws SemanticException {
    if ((!conf.getBoolVar(HiveConf.ConfVars.HIVE_INSERT_INTO_EXTERNAL_TABLES)) &&
        (dest_tab.getTableType().equals(TableType.EXTERNAL_TABLE))) {
      throw new SemanticException(
          ErrorMsg.INSERT_EXTERNAL_TABLE.getMsg(dest_tab.getTableName()));
    }
  }

  private static void checkImmutableTable(QB qb, Table dest_tab, Path dest_path, boolean isPart,
      HiveConf conf)
      throws SemanticException {
    // If the query here is an INSERT_INTO and the target is an immutable table,
    // verify that our destination is empty before proceeding
    if (!dest_tab.isImmutable() || !qb.getParseInfo().isInsertIntoTable(
        dest_tab.getDbName(), dest_tab.getTableName(), dest_tab.getSnapshotRef())) {
      return;
    }
    try {
      FileSystem fs = dest_path.getFileSystem(conf);
      if (! org.apache.hadoop.hive.metastore.utils.FileUtils.isDirEmpty(fs,dest_path)){
        LOG.warn("Attempted write into an immutable table : "
            + dest_tab.getTableName() + " : " + dest_path);
        throw new SemanticException(
            ErrorMsg.INSERT_INTO_IMMUTABLE_TABLE.getMsg(dest_tab.getTableName()));
      }
    } catch (IOException ioe) {
      LOG.warn("Error while trying to determine if immutable table "
          + (isPart ? "partition " : "") + "has any data : "  + dest_tab.getTableName()
          + " : " + dest_path);
      throw new SemanticException(ErrorMsg.INSERT_INTO_IMMUTABLE_TABLE.getMsg(ioe.getMessage()));
    }
  }

  private static DynamicPartitionCtx checkDynPart(QB qb, QBMetaData qbm, Table dest_tab,
                                           Map<String, String> partSpec, String dest,
                                           HiveConf conf) throws SemanticException {
    List<FieldSchema> parts = dest_tab.getPartitionKeys();
    if (parts == null || parts.isEmpty()) {
      return null; // table is not partitioned
    }
    if (partSpec == null || partSpec.size() == 0) { // user did NOT specify partition
      throw new SemanticException(SemanticAnalyzer.generateErrorMessage(qb.getParseInfo().getDestForClause(dest),
          ErrorMsg.NEED_PARTITION_ERROR.getMsg()));
    }
    DynamicPartitionCtx dpCtx = qbm.getDPCtx(dest);
    if (dpCtx == null) {
      dest_tab.validatePartColumnNames(partSpec, false);
      dpCtx = new DynamicPartitionCtx(partSpec,
          conf.getVar(HiveConf.ConfVars.DEFAULTPARTITIONNAME),
          conf.getIntVar(HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTSPERNODE));
      qbm.setDPCtx(dest, dpCtx);
    }

    verifyDynamicPartitionEnabled(conf, qb, dest);

    if ((dest_tab.getNumBuckets() > 0)) {
      dpCtx.setNumBuckets(dest_tab.getNumBuckets());
    }
    return dpCtx;
  }

  private static void verifyDynamicPartitionEnabled(HiveConf conf, QB qb, String dest) throws SemanticException {
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.DYNAMICPARTITIONING)) { // allow DP
      throw new SemanticException(SemanticAnalyzer.generateErrorMessage(qb.getParseInfo().getDestForClause(dest),
          ErrorMsg.DYNAMIC_PARTITION_DISABLED.getMsg()));
    }
  }

  private static boolean isDirectInsert(boolean destTableIsFullAcid, AcidUtils.Operation acidOp,
      Context ctx, HiveConf conf) {
    // In case of an EXPLAIN ANALYZE query, the direct insert has to be turned off. HIVE-24336
    if (ctx.getExplainAnalyze() == AnalyzeState.RUNNING) {
      return false;
    }
    boolean directInsertEnabled = conf.getBoolVar(HiveConf.ConfVars.HIVE_ACID_DIRECT_INSERT_ENABLED);
    boolean directInsert = directInsertEnabled && destTableIsFullAcid && acidOp != AcidUtils.Operation.NOT_ACID;
    if (LOG.isDebugEnabled() && directInsert) {
      LOG.debug("Direct insert for ACID tables is enabled.");
    }
    return directInsert;
  }

  public static Path getTmpDir(boolean isNonNativeTable, boolean isMmTable, boolean isDirectInsert,
      Path destinationPath, DynamicPartitionCtx dpCtx, Context ctx, HiveConf conf) {
    /**
     * We will directly insert to the final destination in the following cases:
     * 1. Non native table
     * 2. Micro-managed (insert only table)
     * 3. Full ACID table and operation type is INSERT
     */
    Path destPath = null;
    if (isNonNativeTable || isMmTable || isDirectInsert) {
      destPath = destinationPath;
    } else if (HiveConf.getBoolVar(conf, ConfVars.HIVE_USE_SCRATCHDIR_FOR_STAGING)) {
      destPath = ctx.getTempDirForInterimJobPath(destinationPath);
    } else {
      destPath = ctx.getTempDirForFinalJobPath(destinationPath);
    }
    if (dpCtx != null && dpCtx.getSPPath() != null) {
      return new Path(destPath, dpCtx.getSPPath());
    }
    return destPath;
  }

  public static void genPartnCols(String dest, Operator input, QB qb,
      TableDesc table_desc, Table dest_tab, SortBucketRSCtx ctx,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap,
      HiveConf conf
      ) throws SemanticException {
    boolean enforceBucketing = false;
    List<ExprNodeDesc> partnColsNoConvert = new ArrayList<ExprNodeDesc>();

    if ((dest_tab.getNumBuckets() > 0)) {
      enforceBucketing = true;
      if (SemanticAnalyzer.updating(dest) || SemanticAnalyzer.deleting(dest)) {
        partnColsNoConvert = GenBucketingSortingDest.getPartitionColsFromBucketColsForUpdateDelete(
            input, false, operatorMap);
      } else {
        partnColsNoConvert = GenBucketingSortingDest.getPartitionColsFromBucketCols(
            dest, qb, dest_tab, table_desc, input, false, operatorMap, conf);
      }
    }

    if ((dest_tab.getSortCols() != null) &&
        (dest_tab.getSortCols().size() > 0)) {
      if (!enforceBucketing) {
        throw new SemanticException(ErrorMsg.TBL_SORTED_NOT_BUCKETED.getErrorCodedMsg(dest_tab.getCompleteName()));
      }
      enforceBucketing = true;
    }

    if (enforceBucketing) {
      ctx.setPartnCols(partnColsNoConvert);
    }
  }

  public static Task<?> setStatsForNonNativeTable(String dbName, String tableName,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs) throws SemanticException {
    TableName qTableName = HiveTableName.ofNullable(tableName, dbName);
    Map<String, String> mapProp = new HashMap<>();
    mapProp.put(StatsSetupConst.COLUMN_STATS_ACCURATE, null);
    AlterTableUnsetPropertiesDesc alterTblDesc = new AlterTableUnsetPropertiesDesc(qTableName, null, null, false,
        mapProp, false, null);
    // TODO: Dangerous code!  We are passing in inputs and outputs into DDLWork, but these inputs and outputs are not
    // in their final form.  This should be refactored.
    return TaskFactory.get(new DDLWork(inputs, outputs, alterTblDesc));
  }

  public static Task<?> createPreInsertDesc(Table table, boolean overwrite,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs) {
    PreInsertTableDesc preInsertTableDesc = new PreInsertTableDesc(table, overwrite);
    // Dangerous code!  We are passing in inputs and outputs into DDLWork, but these inputs and outputs are not
    // in their final form.  This should be refactored.
    return TaskFactory.get(new DDLWork(inputs, outputs, preInsertTableDesc));

  }

  public WriteEntity generateTableWriteEntity(String dest, Table dest_tab,
                                               Map<String, String> partSpec, LoadTableDesc ltd,
                                               DynamicPartitionCtx dpCtx,
                                               boolean allowOutputMultipleTimes)
      throws SemanticException {
    WriteEntity output = null;

    // Here only register the whole table for post-exec hook if no DP present
    // in the case of DP, we will register WriteEntity in MoveTask when the
    // list of dynamically created partitions are known.
    if ((dpCtx == null || dpCtx.getNumDPCols() == 0)) {
      output = new WriteEntity(dest_tab, determineWriteType(ltd, dest));
      writeEntityOutputs.add(new PotentialWriteEntity(output, allowOutputMultipleTimes,
          ErrorMsg.OUTPUT_SPECIFIED_MULTIPLE_TIMES.getMsg(dest_tab.getTableName())));
    }

    if ((dpCtx != null) && (dpCtx.getNumDPCols() >= 0)) {
      // No static partition specified
      if (dpCtx.getNumSPCols() == 0) {
        output = new WriteEntity(dest_tab, determineWriteType(ltd, dest), true);
        writeEntityOutputs.add(new PotentialWriteEntity(output, true, ""));
        output.setDynamicPartitionWrite(true);
      }
      // part of the partition specified
      // Create a DummyPartition in this case. Since, the metastore does not store partial
      // partitions currently, we need to store dummy partitions
      else {
        try {
          String ppath = dpCtx.getSPPath();
          ppath = ppath.substring(0, ppath.length() - 1);
          DummyPartition p =
              new DummyPartition(dest_tab, dest_tab.getDbName()
                  + "@" + dest_tab.getTableName() + "@" + ppath,
                  partSpec);
          output = new WriteEntity(p, getWriteType(dest), false);
          output.setDynamicPartitionWrite(true);
          writeEntityOutputs.add(new PotentialWriteEntity(output, true, ""));
        } catch (HiveException e) {
          throw new SemanticException(e.getMessage(), e);
        }
      }
    }
    return output;
  }

  public static WriteEntity.WriteType determineWriteType(LoadTableDesc ltd, String dest) {

    if (ltd == null) {
      return WriteEntity.WriteType.INSERT_OVERWRITE;
    }
    return ((ltd.getLoadFileType() == LoadFileType.REPLACE_ALL || ltd
        .isInsertOverwrite()) ? WriteEntity.WriteType.INSERT_OVERWRITE : getWriteType(dest));

  }

  private static WriteEntity.WriteType getWriteType(String dest) {
    return SemanticAnalyzer.updating(dest) ? WriteEntity.WriteType.UPDATE :
        (SemanticAnalyzer.deleting(dest) ? WriteEntity.WriteType.DELETE : WriteEntity.WriteType.INSERT);
  }

  public static Path getCtasOrCMVLocation(CreateTableDesc tblDesc, CreateMaterializedViewDesc viewDesc,
                                    boolean createTableWithSuffix,
                                    Context ctx, Hive db,
                                    HiveConf conf) throws SemanticException {
    Path location;
    String[] names;
    String protoName = null;
    Table tbl;
    try {
      if (tblDesc != null) {
        protoName = tblDesc.getDbTableName();

        // Handle table translation initially and if not present
        // use default table path.
        // Property modifications of the table is handled later.
        // We are interested in the location if it has changed
        // due to table translation.
        tbl = tblDesc.toTable(conf);
        tbl = db.getTranslateTableDryrun(tbl.getTTable());
      } else {
        protoName = viewDesc.getViewName();
        tbl = viewDesc.toTable(conf);
      }
      names = Utilities.getDbTableName(protoName);

      Warehouse wh = new Warehouse(conf);
      if (tbl.getSd() == null
          || tbl.getSd().getLocation() == null) {
        location = wh.getDefaultTablePath(db.getDatabase(names[0]), names[1], false);
      } else {
        location = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
      }

      if (createTableWithSuffix) {
        location = new Path(location.toString() +
                Utilities.getTableOrMVSuffix(ctx, createTableWithSuffix));
      }

      return location;
    } catch (HiveException | MetaException e) {
      throw new SemanticException(e);
    }
  }

  public static ColsAndTypes deriveFileSinkColTypes(RowResolver inputRR, List<String> sortColumnNames, List<String> distributeColumnNames,
      List<FieldSchema> fieldSchemas, List<FieldSchema> sortColumns, List<FieldSchema> distributeColumns,
      List<ColumnInfo> sortColInfos, List<ColumnInfo> distributeColInfos,
      boolean isCBOExecuted) throws SemanticException {
    return deriveFileSinkColTypes(inputRR, new ArrayList<>(), sortColumnNames, distributeColumnNames,
        fieldSchemas, new ArrayList<>(), sortColumns, distributeColumns, new ArrayList<>(),
        sortColInfos, distributeColInfos, isCBOExecuted);
  }

  public static ColsAndTypes deriveFileSinkColTypes(
      RowResolver inputRR, List<String> partitionColumnNames, List<String> sortColumnNames, List<String> distributeColumnNames,
      List<FieldSchema> columns, List<FieldSchema> partitionColumns, List<FieldSchema> sortColumns, List<FieldSchema> distributeColumns,
      List<ColumnInfo> fileSinkColInfos, List<ColumnInfo> sortColInfos, List<ColumnInfo> distributeColInfos,
      boolean isCBOExecuted) throws SemanticException {
    ColsAndTypes result = new ColsAndTypes("", "");
    List<String> allColumns = new ArrayList<>();
    List<ColumnInfo> colInfos = inputRR.getColumnInfos();
    List<ColumnInfo> nonPartColInfos = new ArrayList<>();
    SortedMap<Integer, Pair<FieldSchema, ColumnInfo>> partColInfos = new TreeMap<>();
    SortedMap<Integer, Pair<FieldSchema, ColumnInfo>> sColInfos = new TreeMap<>();
    SortedMap<Integer, Pair<FieldSchema, ColumnInfo>> dColInfos = new TreeMap<>();
    boolean first = true;
    int numNonPartitionedCols = colInfos.size() - partitionColumnNames.size();
    if (numNonPartitionedCols <= 0) {
      throw new SemanticException("Too many partition columns declared");
    }
    for (ColumnInfo colInfo : colInfos) {
      String[] nm = inputRR.reverseLookup(colInfo.getInternalName());

      if (nm[1] != null) { // non-null column alias
        colInfo.setAlias(nm[1]);
      }

      boolean isPartitionCol = false;
      String colName = colInfo.getInternalName();  //default column name
      if (columns != null) {
        FieldSchema col = new FieldSchema();
        if (!("".equals(nm[0])) && nm[1] != null) {
          colName = BaseSemanticAnalyzer.unescapeIdentifier(colInfo.getAlias()).toLowerCase(); // remove ``
        }
        if (isCBOExecuted) {
          colName = fixCtasColumnName(colName);
        }
        col.setName(colName);
        allColumns.add(colName);
        String typeName = colInfo.getType().getTypeName();
        // CTAS should NOT create a VOID type
        if (typeName.equals(serdeConstants.VOID_TYPE_NAME)) {
          throw new SemanticException(ErrorMsg.CTAS_CREATES_VOID_TYPE.getMsg(colName));
        }
        col.setType(typeName);
        int idx = partitionColumnNames.indexOf(colName);
        if (idx >= 0) {
          partColInfos.put(idx, Pair.of(col, colInfo));
          isPartitionCol = true;
        } else {
          if (sortColumnNames != null) {
            idx = sortColumnNames.indexOf(colName);
            if (idx >= 0) {
              sColInfos.put(idx, Pair.of(col, colInfo));
            }
          }
          if (distributeColumnNames != null) {
            idx = distributeColumnNames.indexOf(colName);
            if (idx >= 0) {
              dColInfos.put(idx, Pair.of(col, colInfo));
            }
          }
          columns.add(col);
          nonPartColInfos.add(colInfo);
        }
      }

      if (!isPartitionCol) {
        if (!first) {
          result.cols = result.cols.concat(",");
          result.colTypes = result.colTypes.concat(":");
        }

        first = false;
        result.cols = result.cols.concat(colName);

        // Replace VOID type with string when the output is a temp table or
        // local files.
        // A VOID type can be generated under the query:
        //
        // select NULL from tt;
        // or
        // insert overwrite local directory "abc" select NULL from tt;
        //
        // where there is no column type to which the NULL value should be
        // converted.
        //
        String tName = colInfo.getType().getTypeName();
        if (tName.equals(serdeConstants.VOID_TYPE_NAME)) {
          result.colTypes = result.colTypes.concat(serdeConstants.STRING_TYPE_NAME);
        } else {
          result.colTypes = result.colTypes.concat(tName);
        }
      }

    }

    if (partColInfos.size() != partitionColumnNames.size()) {
      throw new SemanticException("Table declaration contains partition columns that are not present " +
        "in query result schema. " +
        "Query columns: " + allColumns + ". " +
        "Partition columns: " + partitionColumnNames);
    }

    if (sortColumnNames != null && sColInfos.size() != sortColumnNames.size()) {
      throw new SemanticException("Table declaration contains cluster/sort columns that are not present " +
          "in query result schema. " +
          "Query columns: " + allColumns + ". " +
          "Organization columns: " + sortColumnNames);
    }

    if (distributeColumnNames != null && dColInfos.size() != distributeColumnNames.size()) {
      throw new SemanticException("Table declaration contains cluster/distribute columns that are not present " +
          "in query result schema. " +
          "Query columns: " + allColumns + ". " +
          "Organization columns: " + distributeColumnNames);
    }

    // FileSinkColInfos comprise nonPartCols followed by partCols
    fileSinkColInfos.addAll(nonPartColInfos);
    partitionColumns.addAll(partColInfos.values().stream().map(Pair::getLeft).collect(Collectors.toList()));
    fileSinkColInfos.addAll(partColInfos.values().stream().map(Pair::getRight).collect(Collectors.toList()));
    // data org columns
    if (sortColumnNames != null) {
      sortColumns.addAll(sColInfos.values().stream().map(Pair::getLeft).collect(Collectors.toList()));
      sortColInfos.addAll(sColInfos.values().stream().map(Pair::getRight).collect(Collectors.toList()));
    }
    if (distributeColumnNames != null) {
      distributeColumns.addAll(dColInfos.values().stream().map(Pair::getLeft).collect(Collectors.toList()));
      distributeColInfos.addAll(dColInfos.values().stream().map(Pair::getRight).collect(Collectors.toList()));
    }

    return result;
  }

  public FileSinkDesc createFileSinkDesc(String dest, TableDesc table_desc,
                                          Partition dest_part, Path dest_path, int currentTableId,
                                          boolean destTableIsAcid, boolean destTableIsTemporary,
                                          boolean destTableIsMaterialization, Path queryTmpdir,
                                          SortBucketRSCtx rsCtx, DynamicPartitionCtx dpCtx, ListBucketingCtx lbCtx,
                                          RowSchema fsRS, boolean canBeMerged, Table dest_tab, Long mmWriteId, boolean isMmCtas,
                                          Integer dest_type, QB qb, boolean isDirectInsert, AcidUtils.Operation acidOperation,
                                          String moveTaskId, Context ctx,
                                          HiveConf conf) throws SemanticException {
    boolean isInsertOverwrite = false;
    Context.Operation writeOperation = getWriteOperation(dest);
    switch (dest_type) {
    case QBMetaData.DEST_PARTITION:
      //fall through
    case QBMetaData.DEST_TABLE:
      //INSERT [OVERWRITE] path
      String destTableFullName = dest_tab.getCompleteName().replace('@', '.');
      Map<String, ASTNode> iowMap = qb.getParseInfo().getInsertOverwriteTables();
      if (iowMap.containsKey(destTableFullName) &&
          qb.getParseInfo().isDestToOpTypeInsertOverwrite(dest)) {
        isInsertOverwrite = true;
      }

      // Some non-native tables might be partitioned without partition spec information being present in the Table object
      HiveStorageHandler storageHandler = dest_tab.getStorageHandler();
      if (storageHandler != null && storageHandler.alwaysUnpartitioned()) {
        DynamicPartitionCtx nonNativeDpCtx = storageHandler.createDPContext(conf, dest_tab, writeOperation);
        if (dpCtx == null && nonNativeDpCtx != null) {
          dpCtx = nonNativeDpCtx;
        }
      }

      break;
    case QBMetaData.DEST_LOCAL_FILE:
    case QBMetaData.DEST_DFS_FILE:
      //CTAS path or insert into file/directory
      break;
    default:
      throw new IllegalStateException("Unexpected dest_type=" + dest_tab);
    }
    FileSinkDesc fileSinkDesc = new FileSinkDesc(queryTmpdir, table_desc,
        conf.getBoolVar(HiveConf.ConfVars.COMPRESSRESULT), currentTableId, rsCtx.isMultiFileSpray(),
        canBeMerged, rsCtx.getNumFiles(), rsCtx.getTotalFiles(), rsCtx.getPartnCols(), dpCtx,
        dest_path, mmWriteId, isMmCtas, isInsertOverwrite, qb.getIsQuery(),
        qb.isCTAS() || qb.isMaterializedView(), isDirectInsert, acidOperation,
            ctx.isDeleteBranchOfUpdate(dest));

    fileSinkDesc.setMoveTaskId(moveTaskId);
    boolean isHiveServerQuery = SessionState.get().isHiveServerQuery();
    fileSinkDesc.setHiveServerQuery(isHiveServerQuery);
    // If this is an insert, update, or delete on an ACID table then mark that so the
    // FileSinkOperator knows how to properly write to it.
    boolean isDestInsertOnly = (dest_part != null && dest_part.getTable() != null &&
        AcidUtils.isInsertOnlyTable(dest_part.getTable().getParameters()))
        || (table_desc != null && AcidUtils.isInsertOnlyTable(table_desc.getProperties()));

    if (isDestInsertOnly) {
      fileSinkDesc.setWriteType(Operation.INSERT);
      //TODO: this should be refactored, should not set member variables outside of constructor
      this.acidFileSinks.add(fileSinkDesc);
    }

    if (destTableIsAcid) {
      AcidUtils.Operation wt = SemanticAnalyzer.updating(dest) ? AcidUtils.Operation.UPDATE :
          (SemanticAnalyzer.deleting(dest) ? AcidUtils.Operation.DELETE : AcidUtils.Operation.INSERT);
      fileSinkDesc.setWriteType(wt);
      //TODO: this should be refactored, should not set member variables outside of constructor
      this.acidFileSinks.add(fileSinkDesc);
    }

    fileSinkDesc.setWriteOperation(writeOperation);

    fileSinkDesc.setTemporary(destTableIsTemporary);
    fileSinkDesc.setMaterialization(destTableIsMaterialization);

    /* Set List Bucketing context. */
    if (lbCtx != null) {
      lbCtx.processRowSkewedIndex(fsRS);
      lbCtx.calculateSkewedValueSubDirList();
    }
    fileSinkDesc.setLbCtx(lbCtx);

    // set the stats publishing/aggregating key prefix
    // the same as directory name. The directory name
    // can be changed in the optimizer but the key should not be changed
    // it should be the same as the MoveWork's sourceDir.
    fileSinkDesc.setStatsAggPrefix(fileSinkDesc.getDirName().toString());
    if (!destTableIsMaterialization &&
        HiveConf.getVar(conf, HIVESTATSDBCLASS).equalsIgnoreCase(StatDB.fs.name())) {
      String statsTmpLoc = ctx.getTempDirForInterimJobPath(dest_path).toString();
      fileSinkDesc.setStatsTmpDir(statsTmpLoc);
      LOG.debug("Set stats collection dir : " + statsTmpLoc);
    }

    if (dest_part != null) {
      try {
        String staticSpec = Warehouse.makePartPath(dest_part.getSpec());
        fileSinkDesc.setStaticSpec(staticSpec);
      } catch (MetaException e) {
        throw new SemanticException(e);
      }
    } else if (dpCtx != null) {
      fileSinkDesc.setStaticSpec(dpCtx.getSPPath());
    }
    return fileSinkDesc;
  }

  private static Context.Operation getWriteOperation(String destination) {
    return SemanticAnalyzer.deleting(destination) ? Context.Operation.DELETE :
        (SemanticAnalyzer.updating(destination) ? Context.Operation.UPDATE :
            Context.Operation.OTHER);
  }

  public static boolean useBatchingSerializer(String serdeClassName, HiveConf conf) {
    return SessionState.get().isHiveServerQuery() &&
      hasSetBatchSerializer(serdeClassName, conf);
  }

  private static boolean hasSetBatchSerializer(String serdeClassName,
      HiveConf conf) {
    return (serdeClassName.equalsIgnoreCase(ThriftJDBCBinarySerDe.class.getName()) &&
      HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_THRIFT_RESULTSET_SERIALIZE_IN_TASKS)) ||
    serdeClassName.equalsIgnoreCase(ArrowColumnarBatchSerDe.class.getName());
  }

  public static void setWriteIdForSurrogateKeys(LoadTableDesc ltd, Operator input) {
    if (ltd == null) {
      return;
    }

    Map<String, ExprNodeDesc> columnExprMap = input.getConf().getColumnExprMap();
    if (columnExprMap != null) {
      for (ExprNodeDesc desc : columnExprMap.values()) {
        if (desc instanceof ExprNodeGenericFuncDesc) {
          GenericUDF genericUDF = ((ExprNodeGenericFuncDesc)desc).getGenericUDF();
          if (genericUDF instanceof GenericUDFSurrogateKey) {
            ((GenericUDFSurrogateKey)genericUDF).setWriteId(ltd.getWriteId());
          }
        }
      }
    }

    for (Operator<? extends OperatorDesc> parent : (List<Operator<? extends OperatorDesc>>)input.getParentOperators()) {
      setWriteIdForSurrogateKeys(ltd, parent);
    }
  }

  public ColumnStatsAutoGatherContext genAutoColumnStatsGatheringPipeline(
      Table table, Map<String, String> partSpec,
      Operator curr, boolean isInsertInto, boolean useTableValueConstructor,
      Context ctx, HiveConf conf,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap
      )
      throws SemanticException {
    LOG.info("Generate an operator pipeline to autogather column stats for table " + table.getTableName()
        + " in query " + ctx.getCmd());
    ColumnStatsAutoGatherContext columnStatsAutoGatherContext = null;
    columnStatsAutoGatherContext = new ColumnStatsAutoGatherContext(conf, curr, table, partSpec,
        isInsertInto, ctx, ImmutableMap.copyOf(operatorMap));
    if (useTableValueConstructor) {
      // Table does not exist, use table value constructor to simulate
      columnStatsAutoGatherContext.insertTableValuesAnalyzePipeline();
    } else {
      // Table already exists
      columnStatsAutoGatherContext.insertAnalyzePipeline();
    }
    return columnStatsAutoGatherContext;
  }

  private static String fixCtasColumnName(String colName) {
    int lastDot = colName.lastIndexOf('.');
    if (lastDot < 0)
     {
      return colName; // alias is not fully qualified
    }
    String nqColumnName = colName.substring(lastDot + 1);
    LOG.debug("Replacing " + colName + " (produced by CBO) by " + nqColumnName);
    return nqColumnName;
  }

  private Path getDestinationFilePath(ReadOnlySemanticAnalyzer sa,
      final String destinationFile, boolean isMmTable) {
    if (sa.isResultsCacheEnabled() && sa.queryTypeCanUseCache()) {
      assert (!isMmTable);
      QueryResultsCache instance = QueryResultsCache.getInstance();
      // QueryResultsCache should have been initialized by now
      if (instance != null) {
        //TODO: this should be refactored, should not set member variables outside of constructor
        this.needSetFsResultCache = true;
        Path resultCacheTopDir = instance.getCacheDirPath();
        String dirName = UUID.randomUUID().toString();
        Path resultDir = new Path(resultCacheTopDir, dirName);
        return resultDir;
      }
    }
    return new Path(destinationFile);
  }

  public Operator getOperator() {
    return output;
  }

  public Map<Operator<? extends OperatorDesc>, OpParseContext> getOperatorMap() {
    return newOperatorMap;
  }

  public Map<String, String> getIdToTableNameMap() {
    return idToTableNameMap;
  }

  public int getNextDestTableId() {
    return nextDestTableId;
  }

  public List<LoadTableDesc> getLoadTableWork() {
    return loadTableWork;
  }

  public List<LoadFileDesc> getLoadFileWork() {
    return loadFileWork;
  }

  public List<PotentialWriteEntity> getPotentialWriteEntities() {
    return writeEntityOutputs;
  }

  public List<Task<?>> getFileSinkTasks() {
    return fileSinkTasks;
  }

  public MaterializedViewUpdateDesc getMaterializedViewUpdateDesc() {
    return materializedViewUpdateDesc;
  }

  public Set<FileSinkDesc> getAcidFileSinks() {
    return acidFileSinks;
  }

  public FileSinkDesc getFileSinkDesc() {
    return fileSinkDesc;
  }

  public boolean isMmCreate() {
    return isMmCreate;
  }

  public boolean isDirectInsert() {
    return isDirectInsert;
  }

  public List<ReduceSinkOperator> getReduceSinkOperatorsAddedByEnforceBucketingSorting() {
    return reduceSinkOperatorsAddedByEnforceBucketingSorting;
  }
  public List<ColumnStatsAutoGatherContext> getColumnStatsAutoGatherContexts() {
    return columnStatsAutoGatherContexts;
  }

  public AcidUtils.Operation getAcidOperation() {
    return acidOperation;
  }

  public Table getDestinationTable() {
    return destinationTable;
  }

  public LoadTableDesc getLoadTableDesc() {
    return ltd;
  }

  public Path getDestinationPath() {
    return destinationPath;
  }

  public boolean needSetFsResultCache() {
    return needSetFsResultCache;
  }

  public boolean setNoOpFetchFormatter() {
    return setNoOpFetchFormatter;
  }

  public boolean setAcidConfigParams() {
    return setAcidConfigParams;
  }

  public static <T extends OperatorDesc> Operator<T> createOperator(
      T conf, RowSchema rwsch, Operator op) {
    Operator<T> retOp = OperatorFactory.getAndMakeChild(conf, rwsch, op);
    retOp.augmentPlan();
    return retOp;
  }

  public static final class ColsAndTypes {
    public ColsAndTypes(String cols, String colTypes) {
      this.cols = cols;
      this.colTypes = colTypes;
    }
    public String cols;
    public String colTypes;
  }

  public static class SortBucketRSCtx {
    List<ExprNodeDesc> partnCols;
    boolean multiFileSpray;
    int numFiles;
    int totalFiles;

    public SortBucketRSCtx() {
      partnCols = null;
      multiFileSpray = false;
      numFiles = 1;
      totalFiles = 1;
    }

    /**
     * @return the partnCols
     */
    public List<ExprNodeDesc> getPartnCols() {
      return partnCols;
    }

    /**
     * @param partnCols
     *          the partnCols to set
     */
    public void setPartnCols(List<ExprNodeDesc> partnCols) {
      this.partnCols = partnCols;
    }

    /**
     * @return the multiFileSpray
     */
    public boolean isMultiFileSpray() {
      return multiFileSpray;
    }

    /**
     * @param multiFileSpray
     *          the multiFileSpray to set
     */
    public void setMultiFileSpray(boolean multiFileSpray) {
      this.multiFileSpray = multiFileSpray;
    }

    /**
     * @return the numFiles
     */
    public int getNumFiles() {
      return numFiles;
    }

    /**
     * @param numFiles
     *          the numFiles to set
     */
    public void setNumFiles(int numFiles) {
      this.numFiles = numFiles;
    }

    /**
     * @return the totalFiles
     */
    public int getTotalFiles() {
      return totalFiles;
    }

    /**
     * @param totalFiles
     *          the totalFiles to set
     */
    public void setTotalFiles(int totalFiles) {
      this.totalFiles = totalFiles;
    }
  }

  /**
   * PotentialWriteEntity gathers all the write entities from GenFileSinkPlan and
   * allows the caller to add them to the list of all write entities.
   */
  public static class PotentialWriteEntity {
    // the WriteEntity
    WriteEntity output;
    // if allowMultipleOutputs is false, the caller should fail the query if the
    // WriteEntity has previously been tracked.
    boolean allowMultipleOutputs;
    // Contains the error message if there is a failure with multiple outputs
    String errorMessageIfNotAllowed;

    private PotentialWriteEntity(WriteEntity writeEntity, boolean allowMultipleOutputs,
        String errorMessageIfNotAllowed) {
      this.output = writeEntity;
      this.allowMultipleOutputs = allowMultipleOutputs;
      this.errorMessageIfNotAllowed = errorMessageIfNotAllowed;
    }
  }
}
