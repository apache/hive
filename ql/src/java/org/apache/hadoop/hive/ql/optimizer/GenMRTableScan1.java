/**
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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.rcfile.stats.PartialScanWork;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.StatsNoJobWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.mapred.InputFormat;

/**
 * Processor for the rule - table scan.
 */
public class GenMRTableScan1 implements NodeProcessor {
  public GenMRTableScan1() {
  }

  /**
   * Table Sink encountered.
   * @param nd
   *          the table sink operator encountered
   * @param opProcCtx
   *          context
   */
  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx opProcCtx,
      Object... nodeOutputs) throws SemanticException {
    TableScanOperator op = (TableScanOperator) nd;
    GenMRProcContext ctx = (GenMRProcContext) opProcCtx;
    ParseContext parseCtx = ctx.getParseCtx();
    Class<? extends InputFormat> inputFormat = op.getConf().getTableMetadata()
        .getInputFormatClass();
    Map<Operator<? extends OperatorDesc>, GenMapRedCtx> mapCurrCtx = ctx.getMapCurrCtx();

    // create a dummy MapReduce task
    MapredWork currWork = GenMapRedUtils.getMapRedWork(parseCtx);
    MapRedTask currTask = (MapRedTask) TaskFactory.get(currWork, parseCtx.getConf());
    ctx.setCurrTask(currTask);
    ctx.setCurrTopOp(op);

    for (String alias : parseCtx.getTopOps().keySet()) {
      Operator<? extends OperatorDesc> currOp = parseCtx.getTopOps().get(alias);
      if (currOp == op) {
        String currAliasId = alias;
        ctx.setCurrAliasId(currAliasId);
        mapCurrCtx.put(op, new GenMapRedCtx(currTask, currAliasId));

        if (parseCtx.getQueryProperties().isAnalyzeCommand()) {
          boolean partialScan = parseCtx.getQueryProperties().isPartialScanAnalyzeCommand();
          boolean noScan = parseCtx.getQueryProperties().isNoScanAnalyzeCommand();
          if (OrcInputFormat.class.isAssignableFrom(inputFormat) ||
                  MapredParquetInputFormat.class.isAssignableFrom(inputFormat)) {
            // For ORC and Parquet, all the following statements are the same
            // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS
            // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS partialscan;
            // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS noscan;

            // There will not be any MR or Tez job above this task
            StatsNoJobWork snjWork = new StatsNoJobWork(op.getConf().getTableMetadata().getTableSpec());
            snjWork.setStatsReliable(parseCtx.getConf().getBoolVar(
                HiveConf.ConfVars.HIVE_STATS_RELIABLE));
            // If partition is specified, get pruned partition list
            Set<Partition> confirmedParts = GenMapRedUtils.getConfirmedPartitionsForScan(op);
            if (confirmedParts.size() > 0) {
              Table source = op.getConf().getTableMetadata();
              List<String> partCols = GenMapRedUtils.getPartitionColumns(op);
              PrunedPartitionList partList = new PrunedPartitionList(source, confirmedParts,
                  partCols, false);
              snjWork.setPrunedPartitionList(partList);
            }
            Task<StatsNoJobWork> snjTask = TaskFactory.get(snjWork, parseCtx.getConf());
            ctx.setCurrTask(snjTask);
            ctx.setCurrTopOp(null);
            ctx.getRootTasks().clear();
            ctx.getRootTasks().add(snjTask);
          } else {
            // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS;
            // The plan consists of a simple MapRedTask followed by a StatsTask.
            // The MR task is just a simple TableScanOperator

            StatsWork statsWork = new StatsWork(op.getConf().getTableMetadata().getTableSpec());
            statsWork.setAggKey(op.getConf().getStatsAggPrefix());
            statsWork.setStatsTmpDir(op.getConf().getTmpStatsDir());
            statsWork.setSourceTask(currTask);
            statsWork.setStatsReliable(parseCtx.getConf().getBoolVar(
                HiveConf.ConfVars.HIVE_STATS_RELIABLE));
            Task<StatsWork> statsTask = TaskFactory.get(statsWork, parseCtx.getConf());
            currTask.addDependentTask(statsTask);
            if (!ctx.getRootTasks().contains(currTask)) {
              ctx.getRootTasks().add(currTask);
            }

            // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS noscan;
            // The plan consists of a StatsTask only.
            if (noScan) {
              statsTask.setParentTasks(null);
              statsWork.setNoScanAnalyzeCommand(true);
              ctx.getRootTasks().remove(currTask);
              ctx.getRootTasks().add(statsTask);
            }

            // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS partialscan;
            if (partialScan) {
              handlePartialScanCommand(op, ctx, parseCtx, currTask, statsWork, statsTask);
            }

            currWork.getMapWork().setGatheringStats(true);
            if (currWork.getReduceWork() != null) {
              currWork.getReduceWork().setGatheringStats(true);
            }

            // NOTE: here we should use the new partition predicate pushdown API to get a list of
            // pruned list,
            // and pass it to setTaskPlan as the last parameter
            Set<Partition> confirmedPartns = GenMapRedUtils
                .getConfirmedPartitionsForScan(op);
            if (confirmedPartns.size() > 0) {
              Table source = op.getConf().getTableMetadata();
              List<String> partCols = GenMapRedUtils.getPartitionColumns(op);
              PrunedPartitionList partList = new PrunedPartitionList(source, confirmedPartns, partCols, false);
              GenMapRedUtils.setTaskPlan(currAliasId, op, currTask, false, ctx, partList);
            } else { // non-partitioned table
              GenMapRedUtils.setTaskPlan(currAliasId, op, currTask, false, ctx);
            }
          }
        }

        return true;
      }
    }
    assert false;
    return null;
  }

  /**
   * handle partial scan command. It is composed of PartialScanTask followed by StatsTask .
   * @param op
   * @param ctx
   * @param parseCtx
   * @param currTask
   * @param parseInfo
   * @param statsWork
   * @param statsTask
   * @throws SemanticException
   */
  private void handlePartialScanCommand(TableScanOperator op, GenMRProcContext ctx,
      ParseContext parseCtx, Task<? extends Serializable> currTask,
      StatsWork statsWork, Task<StatsWork> statsTask) throws SemanticException {
    String aggregationKey = op.getConf().getStatsAggPrefix();
    StringBuilder aggregationKeyBuffer = new StringBuilder(aggregationKey);
    List<Path> inputPaths = GenMapRedUtils.getInputPathsForPartialScan(op, aggregationKeyBuffer);
    aggregationKey = aggregationKeyBuffer.toString();

    // scan work
    PartialScanWork scanWork = new PartialScanWork(inputPaths);
    scanWork.setMapperCannotSpanPartns(true);
    scanWork.setAggKey(aggregationKey);
    scanWork.setStatsTmpDir(op.getConf().getTmpStatsDir(), parseCtx.getConf());

    // stats work
    statsWork.setPartialScanAnalyzeCommand(true);

    // partial scan task
    DriverContext driverCxt = new DriverContext();
    Task<PartialScanWork> psTask = TaskFactory.get(scanWork, parseCtx.getConf());
    psTask.initialize(parseCtx.getQueryState(), null, driverCxt, op.getCompilationOpContext());
    psTask.setWork(scanWork);

    // task dependency
    ctx.getRootTasks().remove(currTask);
    ctx.getRootTasks().add(psTask);
    psTask.addDependentTask(statsTask);
    List<Task<? extends Serializable>> parentTasks = new ArrayList<Task<? extends Serializable>>();
    parentTasks.add(psTask);
    statsTask.setParentTasks(parentTasks);
  }

}
