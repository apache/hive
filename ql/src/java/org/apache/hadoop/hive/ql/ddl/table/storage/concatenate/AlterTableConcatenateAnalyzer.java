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

package org.apache.hadoop.hive.ql.ddl.table.storage.concatenate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AlterTableType;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.ddl.table.storage.compact.AlterTableCompactDesc;
import org.apache.hadoop.hive.ql.exec.ArchiveUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.InputFormat;

/**
 * Analyzer for compact commands.
 */
@DDLType(types = {HiveParser.TOK_ALTERTABLE_MERGEFILES, HiveParser.TOK_ALTERPARTITION_MERGEFILES})
public class AlterTableConcatenateAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableConcatenateAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    Table table = getTable(tableName);

    if (AcidUtils.isTransactionalTable(table)) {
      String poolName = table.getProperty(Constants.HIVE_COMPACTOR_WORKER_POOL);
      compactAcidTable(tableName, partitionSpec, poolName);
    } else {
      // non-native and non-managed tables are not supported as MoveTask requires filenames to be in specific format,
      // violating which can cause data loss
      if (table.isNonNative()) {
        throw new SemanticException(ErrorMsg.CONCATENATE_UNSUPPORTED_TABLE_NON_NATIVE.getMsg());
      }

      if (table.getTableType() != TableType.MANAGED_TABLE) {
        // Enable concatenate for external tables if config is set.
        if (!conf.getBoolVar(ConfVars.CONCATENATE_EXTERNAL_TABLE)
            || table.getTableType() != TableType.EXTERNAL_TABLE) {
          throw new SemanticException(ErrorMsg.CONCATENATE_UNSUPPORTED_TABLE_NOT_MANAGED.getMsg());
        }
      }

      if (table.isPartitioned()) {
        concatenatePartitionedTable(tableName, table, partitionSpec);
      } else {
        concatenateUnpartitionedTable(tableName, table, partitionSpec);
      }
    }
  }

  private void compactAcidTable(TableName tableName, Map<String, String> partitionSpec, String poolName) throws SemanticException {
    boolean isBlocking = !HiveConf.getBoolVar(conf, ConfVars.TRANSACTIONAL_CONCATENATE_NOBLOCK, false);

    AlterTableCompactDesc desc = new AlterTableCompactDesc(tableName, partitionSpec, CompactionType.MAJOR.name(), isBlocking,
        poolName, 0, null, null);
    addInputsOutputsAlterTable(tableName, partitionSpec, desc, desc.getType(), false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
    setAcidDdlDesc(getTable(tableName), desc);
  }

  @SuppressWarnings("rawtypes")
  private void concatenatePartitionedTable(TableName tableName, Table table, Map<String, String> partitionSpec)
      throws SemanticException {
    if (partitionSpec == null) {
      throw new SemanticException("source table " + tableName + " is partitioned but no partition desc found.");
    }

    Partition part = PartitionUtils.getPartition(db, table, partitionSpec, false);
    if (part == null) {
      throw new SemanticException("source table " + tableName + " is partitioned but partition not found.");
    }
    if (ArchiveUtils.isArchived(part)) {
      throw new SemanticException(ErrorMsg.CONCATENATE_UNSUPPORTED_PARTITION_ARCHIVED.getMsg());
    }

    Path tablePath = table.getPath();
    Path partitionPath = part.getDataLocation();

    Path oldLocation = partitionPath;
    // if the table is in a different dfs than the partition, replace the partition's dfs with the table's dfs.
    Path newLocation =
        new Path(tablePath.toUri().getScheme(), tablePath.toUri().getAuthority(), partitionPath.toUri().getPath());

    ListBucketingCtx lbCtx = constructListBucketingCtx(part.getSkewedColNames(), part.getSkewedColValues(),
        part.getSkewedColValueLocationMaps(), part.isStoredAsSubDirectories());
    List<String> bucketCols = part.getBucketCols();

    Class<? extends InputFormat> inputFormatClass = null;
    try {
      inputFormatClass = part.getInputFormatClass();
    } catch (HiveException e) {
      throw new SemanticException(e);
    }

    createConcatenateTasks(tableName, table, partitionSpec, oldLocation, newLocation, lbCtx, bucketCols,
        inputFormatClass);
  }

  @SuppressWarnings("rawtypes")
  private void concatenateUnpartitionedTable(TableName tableName, Table table, Map<String, String> partitionSpec)
      throws SemanticException {
    Path oldLocation = table.getPath();
    Path newLocation = table.getPath();

    ListBucketingCtx lbCtx = constructListBucketingCtx(table.getSkewedColNames(), table.getSkewedColValues(),
        table.getSkewedColValueLocationMaps(), table.isStoredAsSubDirectories());

    List<String> bucketCols = table.getBucketCols();
    Class<? extends InputFormat> inputFormatClass = table.getInputFormatClass();

    createConcatenateTasks(tableName, table, partitionSpec, oldLocation, newLocation, lbCtx, bucketCols,
        inputFormatClass);
  }

  @SuppressWarnings("rawtypes")
  private void createConcatenateTasks(TableName tableName, Table table, Map<String, String> partitionSpec,
      Path oldLocation, Path newLocation, ListBucketingCtx lbCtx, List<String> bucketCols,
      Class<? extends InputFormat> inputFormatClass) throws SemanticException {
    if (!(inputFormatClass.equals(RCFileInputFormat.class) || inputFormatClass.equals(OrcInputFormat.class))) {
      throw new SemanticException(ErrorMsg.CONCATENATE_UNSUPPORTED_FILE_FORMAT.getMsg());
    }
    if (bucketCols != null && bucketCols.size() > 0) {
      throw new SemanticException(ErrorMsg.CONCATENATE_UNSUPPORTED_TABLE_BUCKETED.getMsg());
    }

    addInputsOutputsAlterTable(tableName, partitionSpec, null, AlterTableType.MERGEFILES, false);

    TableDesc tableDesc = Utilities.getTableDesc(table);
    Path queryTmpDir = ctx.getExternalTmpPath(newLocation);

    Task<?> mergeTask =
        createMergeTask(tableName, table, partitionSpec, oldLocation, lbCtx, inputFormatClass, queryTmpDir);

    addMoveTask(tableName, table, partitionSpec, oldLocation, newLocation, lbCtx, tableDesc, queryTmpDir, mergeTask);

    rootTasks.add(mergeTask);
  }

  @SuppressWarnings("rawtypes")
  private Task<?> createMergeTask(TableName tableName, Table table, Map<String, String> partitionSpec, Path oldLocation,
      ListBucketingCtx lbCtx, Class<? extends InputFormat> inputFormatClass, Path queryTmpDir) throws SemanticException {
    AlterTableConcatenateDesc desc = new AlterTableConcatenateDesc(tableName, partitionSpec, lbCtx, oldLocation,
        queryTmpDir, inputFormatClass, Utilities.getTableDesc(table));
    DDLWork ddlWork = new DDLWork(getInputs(), getOutputs(), desc);
    ddlWork.setNeedLock(true);
    return TaskFactory.get(ddlWork);
  }

  private void addMoveTask(TableName tableName, Table table, Map<String, String> partitionSpec, Path oldLocation,
      Path newLocation, ListBucketingCtx lbCtx, TableDesc tableDesc, Path queryTmpDir, Task<?> mergeTask)
      throws SemanticException {
    // No need to handle MM tables - unsupported path.
    LoadTableDesc loadTableDesc = new LoadTableDesc(queryTmpDir, tableDesc,
        partitionSpec == null ? new HashMap<>() : partitionSpec);
    loadTableDesc.setLbCtx(lbCtx);
    loadTableDesc.setInheritTableSpecs(true);
    Task<MoveWork> moveTask = TaskFactory.get(new MoveWork(null, null, loadTableDesc, null, false));
    mergeTask.addDependentTask(moveTask);

    addStatTask(tableName, table, partitionSpec, oldLocation, newLocation, loadTableDesc, moveTask);
  }

  private void addStatTask(TableName tableName, Table table, Map<String, String> partitionSpec, Path oldLocation,
      Path newLocation, LoadTableDesc loadTableDesc, Task<MoveWork> moveTask) throws SemanticException {
    if (conf.getBoolVar(HiveConf.ConfVars.HIVESTATSAUTOGATHER)) {
      BasicStatsWork basicStatsWork;
      if (oldLocation.equals(newLocation)) {
        // If we're merging to the same location, we can avoid some metastore calls
        try {
          TableSpec tableSpec = new TableSpec(db, tableName, partitionSpec);
          basicStatsWork = new BasicStatsWork(tableSpec);
        } catch (HiveException e){
          throw new SemanticException(e);
        }
      } else {
        basicStatsWork = new BasicStatsWork(loadTableDesc);
      }
      basicStatsWork.setNoStatsAggregator(true);
      basicStatsWork.setClearAggregatorStats(true);
      StatsWork statsWork = new StatsWork(table, basicStatsWork, conf);

      Task<?> statTask = TaskFactory.get(statsWork);
      moveTask.addDependentTask(statTask);
    }
  }
}
