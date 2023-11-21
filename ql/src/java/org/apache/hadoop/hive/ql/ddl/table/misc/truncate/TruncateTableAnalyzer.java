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

package org.apache.hadoop.hive.ql.ddl.table.misc.truncate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractBaseAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableUtils;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveTableName;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.ParseUtils.ReparseResult;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputFormat;

/**
 * Analyzer for truncate table commands.
 */
@DDLType(types = HiveParser.TOK_TRUNCATETABLE)
public class TruncateTableAnalyzer extends AbstractBaseAlterTableAnalyzer {
  public TruncateTableAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    ASTNode tableNode = (ASTNode) root.getChild(0); // TOK_TABLE_PARTITION
    String tableNameString = getUnescapedName((ASTNode) tableNode.getChild(0));
    Table table = getTable(tableNameString, true);
    TableName tableName = HiveTableName.of(table);
    checkTruncateEligibility(root, tableNode, tableNameString, table);

    Map<String, String> partitionSpec = getPartSpec((ASTNode) tableNode.getChild(1));
    addTruncateTableOutputs(tableNode, table, partitionSpec);

    if (table.getStorageHandler() != null && !table.getStorageHandler().canUseTruncate(table, partitionSpec)) {
      // Transform the truncate query to delete query.
      StringBuilder rewrittenQuery = constructDeleteQuery(getFullTableNameForSQL((ASTNode) tableNode.getChild(0)),
          table, partitionSpec);
      ReparseResult rr = ParseUtils.parseRewrittenQuery(ctx, rewrittenQuery);
      Context rewrittenCtx = rr.rewrittenCtx;
      ASTNode rewrittenTree = rr.rewrittenTree;
      rewrittenCtx.setOperation(Context.Operation.DELETE);
      rewrittenCtx.addDestNamePrefix(1, Context.DestClausePrefix.DELETE);

      BaseSemanticAnalyzer deleteAnalyzer = SemanticAnalyzerFactory.get(queryState, rewrittenTree);
      // Note: this will overwrite this.ctx with rewrittenCtx
      rewrittenCtx.setEnableUnparse(false);
      deleteAnalyzer.analyze(rewrittenTree, rewrittenCtx);

      rootTasks = deleteAnalyzer.getRootTasks();
      return;
    }

    Task<?> truncateTask;
    ASTNode colNamesNode = (ASTNode) root.getFirstChildWithType(HiveParser.TOK_TABCOLNAME);
    if (colNamesNode == null) {
      truncateTask = getTruncateTaskWithoutColumnNames(tableName, partitionSpec, table);
    } else {
      truncateTask = getTruncateTaskWithColumnNames(tableNode, tableName, table, partitionSpec, colNamesNode);
    }

    rootTasks.add(truncateTask);
  }

  private void checkTruncateEligibility(ASTNode ast, ASTNode root, String tableName, Table table)
      throws SemanticException {

    if (table.isNonNative()) {
      if (table.getStorageHandler() == null || !table.getStorageHandler().supportsTruncateOnNonNativeTables()) {
        throw new SemanticException(ErrorMsg.TRUNCATE_FOR_NON_NATIVE_TABLE.format(tableName)); //TODO
      } else {
        // If the storage handler supports truncate, then we do not need to check anything else
        return;
      }
    }

    boolean isForce = ast.getFirstChildWithType(HiveParser.TOK_FORCE) != null;
    if (!isForce &&
        table.getTableType() != TableType.MANAGED_TABLE &&
        table.getParameters().getOrDefault(MetaStoreUtils.EXTERNAL_TABLE_PURGE, "FALSE").equalsIgnoreCase("FALSE")) {
      throw new SemanticException(ErrorMsg.TRUNCATE_FOR_NON_MANAGED_TABLE.format(tableName));
    }

    if (!table.isPartitioned() && root.getChildCount() > 1) {
      throw new SemanticException(ErrorMsg.PARTSPEC_FOR_NON_PARTITIONED_TABLE.format(tableName));
    }
  }

  private void addTruncateTableOutputs(ASTNode root, Table table, Map<String, String> partitionSpec)
      throws SemanticException {
    boolean truncateUseBase = (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_TRUNCATE_USE_BASE)
        || HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ACID_LOCKLESS_READS_ENABLED))
      && AcidUtils.isTransactionalTable(table);
    
    WriteEntity.WriteType writeType =
        truncateUseBase ? WriteEntity.WriteType.DDL_EXCL_WRITE : WriteEntity.WriteType.DDL_EXCLUSIVE;
    
    if (partitionSpec == null) {
      if (!table.isPartitioned()) {
        outputs.add(new WriteEntity(table, writeType));
      } else {
        for (Partition partition : PartitionUtils.getPartitions(db, table, null, false)) {
          outputs.add(new WriteEntity(partition, writeType));
        }
      }
    } else {
      if (AlterTableUtils.isFullPartitionSpec(table, partitionSpec)) {
        if (table.getStorageHandler() != null && table.getStorageHandler().alwaysUnpartitioned()) {
          table.getStorageHandler().validatePartSpec(table, partitionSpec);
          try {
            String partName = Warehouse.makePartName(partitionSpec, false);
            Partition partition = new DummyPartition(table, partName, partitionSpec);
            outputs.add(new WriteEntity(partition, writeType));
          } catch (MetaException e) {
            throw new SemanticException("Unable to construct name for dummy partition due to: ", e);
          }
        } else {
          Partition partition = PartitionUtils.getPartition(db, table, partitionSpec, true);
          outputs.add(new WriteEntity(partition, writeType));
        }
      } else {
        validatePartSpec(table, partitionSpec, (ASTNode) root.getChild(1), conf, false);
        for (Partition partition : PartitionUtils.getPartitions(db, table, partitionSpec, false)) {
          outputs.add(new WriteEntity(partition, writeType));
        }
      }
    }
  }

  private Task<?> getTruncateTaskWithoutColumnNames(TableName tableName, Map<String, String> partitionSpec,
      Table table) {
    TruncateTableDesc desc = new TruncateTableDesc(tableName, partitionSpec, null, table);
    if (desc.mayNeedWriteId()) {
      setAcidDdlDesc(desc);
    }

    DDLWork ddlWork = new DDLWork(getInputs(), getOutputs(), desc);
    return TaskFactory.get(ddlWork);
  }

  private Task<?> getTruncateTaskWithColumnNames(ASTNode root, TableName tableName, Table table,
      Map<String, String> partitionSpec, ASTNode columnNamesNode) throws SemanticException {
    try {
      // It would be possible to support this, but this is such a pointless command.
      if (AcidUtils.isInsertOnlyTable(table.getParameters())) {
        throw new SemanticException("Truncating MM table columns not presently supported");
      }

      List<String> columnNames = getColumnNames(columnNamesNode);

      if (table.isPartitioned()) {
        return truncatePartitionedTableWithColumnNames(root, tableName, table, partitionSpec, columnNames);
      } else {
        return truncateUnpartitionedTableWithColumnNames(root, tableName, table, partitionSpec, columnNames);
      }
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
  }

  @SuppressWarnings("rawtypes")
  private Task<?> truncatePartitionedTableWithColumnNames(ASTNode root, TableName tableName, Table table,
      Map<String, String> partitionSpec, List<String> columnNames) throws HiveException {
    Partition partition = db.getPartition(table, partitionSpec, false);

    Path tablePath = table.getPath();
    Path partitionPath = partition.getDataLocation();

    // if the table is in a different dfs than the partition, replace the partition's dfs with the table's dfs.
    Path oldPartitionLocation = partitionPath;
    Path newPartitionLocation = new Path(tablePath.toUri().getScheme(), tablePath.toUri().getAuthority(),
        partitionPath.toUri().getPath());

    List<FieldSchema> columns = partition.getCols();
    List<String> bucketColumns = partition.getBucketCols();
    List<String> listBucketColumns = partition.getSkewedColNames();

    Class<? extends InputFormat> inputFormatClass = partition.getInputFormatClass();
    boolean isArchived = ArchiveUtils.isArchived(partition);
    ListBucketingCtx lbCtx = constructListBucketingCtx(partition.getSkewedColNames(), partition.getSkewedColValues(),
        partition.getSkewedColValueLocationMaps(), partition.isStoredAsSubDirectories());
    boolean isListBucketed = partition.isStoredAsSubDirectories();

    return createTasks(root, tableName, table, partitionSpec, columnNames, bucketColumns, inputFormatClass,
        isArchived, newPartitionLocation, oldPartitionLocation, columns, lbCtx, isListBucketed, listBucketColumns);
  }

  @SuppressWarnings("rawtypes")
  private Task<?> truncateUnpartitionedTableWithColumnNames(ASTNode root, TableName tableName, Table table,
      Map<String, String> partitionSpec, List<String> columnNames) throws SemanticException {
    // input and output are the same
    Path oldPartitionLocation = table.getPath();
    Path newPartitionLocation = table.getPath();

    List<FieldSchema> columns  = table.getCols();
    List<String> bucketColumns = table.getBucketCols();
    List<String> listBucketColumns = table.getSkewedColNames();

    Class<? extends InputFormat> inputFormatClass = table.getInputFormatClass();
    ListBucketingCtx lbCtx = constructListBucketingCtx(table.getSkewedColNames(), table.getSkewedColValues(),
        table.getSkewedColValueLocationMaps(), table.isStoredAsSubDirectories());
    boolean isListBucketed = table.isStoredAsSubDirectories();

    return createTasks(root, tableName, table, partitionSpec, columnNames, bucketColumns, inputFormatClass,
        false, newPartitionLocation, oldPartitionLocation, columns, lbCtx, isListBucketed, listBucketColumns);
  }

  @SuppressWarnings("rawtypes")
  private Task<?> createTasks(ASTNode root, TableName tableName, Table table, Map<String, String> partitionSpec,
      List<String> columnNames, List<String> bucketColumns, Class<? extends InputFormat> inputFormatClass,
      boolean isArchived, Path newPartitionLocation, Path oldPartitionLocation, List<FieldSchema> columns,
      ListBucketingCtx lbCtx, boolean isListBucketed, List<String> listBucketColumns) throws SemanticException {
    if (!inputFormatClass.equals(RCFileInputFormat.class)) {
      throw new SemanticException(ErrorMsg.TRUNCATE_COLUMN_NOT_RC.getMsg());
    }

    if (isArchived) {
      throw new SemanticException(ErrorMsg.TRUNCATE_COLUMN_ARCHIVED.getMsg());
    }

    Set<Integer> columnIndexes =
        getColumnIndexes(columnNames, bucketColumns, columns, isListBucketed, listBucketColumns);

    addInputsOutputsAlterTable(tableName, partitionSpec, null, AlterTableType.TRUNCATE, false);

    TableDesc tableDesc = Utilities.getTableDesc(table);
    Path queryTmpdir = ctx.getExternalTmpPath(newPartitionLocation);

    Task<?> truncateTask =
        createTruncateTask(tableName, table, partitionSpec, oldPartitionLocation, lbCtx, columnIndexes, queryTmpdir);

    addMoveTask(root, table, partitionSpec, oldPartitionLocation, newPartitionLocation, lbCtx, queryTmpdir,
        truncateTask, tableDesc);

    return truncateTask;
  }

  private Set<Integer> getColumnIndexes(List<String> columnNames, List<String> bucketColumns, List<FieldSchema> columns,
      boolean isListBucketed, List<String> listBucketColumns) throws SemanticException {
    Set<Integer> columnIndexes = new HashSet<Integer>();
    for (String columnName : columnNames) {
      boolean found = false;
      for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (columnName.equalsIgnoreCase(columns.get(columnIndex).getName())) {
          columnIndexes.add(columnIndex);
          found = true;
          break;
        }
      }

      if (!found) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN.getMsg(columnName));
      }

      for (String bucketColumn : bucketColumns) {
        if (bucketColumn.equalsIgnoreCase(columnName)) {
          throw new SemanticException(ErrorMsg.TRUNCATE_BUCKETED_COLUMN.getMsg(columnName));
        }
      }

      if (isListBucketed) {
        for (String listBucketColumn : listBucketColumns) {
          if (listBucketColumn.equalsIgnoreCase(columnName)) {
            throw new SemanticException(ErrorMsg.TRUNCATE_LIST_BUCKETED_COLUMN.getMsg(columnName));
          }
        }
      }
    }
    return columnIndexes;
  }

  private Task<?> createTruncateTask(TableName tableName, Table table, Map<String, String> partitionSpec,
      Path oldPartitionLocation, ListBucketingCtx lbCtx, Set<Integer> columnIndexes, Path queryTmpdir) {
    TruncateTableDesc desc = new TruncateTableDesc(tableName, partitionSpec, null, table,
        new ArrayList<Integer>(columnIndexes), oldPartitionLocation, queryTmpdir, lbCtx);
    if (desc.mayNeedWriteId()) {
      setAcidDdlDesc(desc);
    }

    DDLWork ddlWork = new DDLWork(getInputs(), getOutputs(), desc);
    ddlWork.setNeedLock(true);
    return TaskFactory.get(ddlWork);
  }

  private void addMoveTask(ASTNode root, Table table, Map<String, String> partitionSpec, Path oldPartitionLocation,
      Path newPartitionLocation, ListBucketingCtx lbCtx, Path queryTmpdir, Task<?> truncateTask, TableDesc tableDesc)
      throws SemanticException {
    // Write the output to temporary directory and move it to the final location at the end
    // so the operation is atomic.
    LoadTableDesc loadTableDesc =
        new LoadTableDesc(queryTmpdir, tableDesc, partitionSpec == null ? new HashMap<>() : partitionSpec);
    loadTableDesc.setLbCtx(lbCtx);
    Task<MoveWork> moveTask = TaskFactory.get(new MoveWork(null, null, loadTableDesc, null, false));
    truncateTask.addDependentTask(moveTask);

    addStatTask(root, table, oldPartitionLocation, newPartitionLocation, loadTableDesc, moveTask);
  }

  private void addStatTask(ASTNode root, Table table, Path oldPartitionLocation, Path newPartitionLocation,
      LoadTableDesc loadTableDesc, Task<MoveWork> moveTask) throws SemanticException {
    // Recalculate the HDFS stats if auto gather stats is set
    if (conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
      BasicStatsWork basicStatsWork;
      if (oldPartitionLocation.equals(newPartitionLocation)) {
        // If we're merging to the same location, we can avoid some metastore calls
        TableSpec partitionSpec = new TableSpec(db, conf, root);
        basicStatsWork = new BasicStatsWork(partitionSpec);
      } else {
        basicStatsWork = new BasicStatsWork(loadTableDesc);
      }
      basicStatsWork.setNoStatsAggregator(true);
      basicStatsWork.setClearAggregatorStats(true);
      StatsWork columnStatsWork = new StatsWork(table, basicStatsWork, conf);

      Task<?> statTask = TaskFactory.get(columnStatsWork);
      moveTask.addDependentTask(statTask);
    }
  }

  public StringBuilder constructDeleteQuery(String qualifiedTableName, Table table, Map<String, String> partitionSpec)
      throws SemanticException {
    StringBuilder sb = new StringBuilder().append("delete from ").append(qualifiedTableName)
            .append(" where ");
    boolean first = true;
    for (String key : partitionSpec.keySet()) {
      ColumnInfo columnInfo = table.getStorageHandler().getColumnInfo(table, key);
      if (columnInfo.getObjectInspector() instanceof PrimitiveObjectInspector) {
        PrimitiveObjectInspector primitiveObjInspector = (PrimitiveObjectInspector) columnInfo.getObjectInspector();
        String value = partitionSpec.get(key);
        if (value != null) {
          sb.append(first ? "" : " and ").append(HiveUtils.unparseIdentifier(key, conf)).append(" = ")
            .append(TypeInfoUtils.convertStringToLiteralForSQL(value, primitiveObjInspector.getPrimitiveCategory()));
        }
      } else {
        throw new SemanticException(String.format(
            "Only primitive partition column type is supported via TRUNCATE operation " +
            " but column %s is not a primitive category.", key));
      }
      first = false;
    }
    return sb;
  }
}
