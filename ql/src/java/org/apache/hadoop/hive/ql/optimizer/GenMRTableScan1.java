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
import org.apache.hadoop.hive.ql.io.rcfile.stats.PartialScanWork;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
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
    Operator<? extends OperatorDesc> currTopOp = op;
    ctx.setCurrTask(currTask);
    ctx.setCurrTopOp(currTopOp);

    for (String alias : parseCtx.getTopOps().keySet()) {
      Operator<? extends OperatorDesc> currOp = parseCtx.getTopOps().get(alias);
      if (currOp == op) {
        String currAliasId = alias;
        ctx.setCurrAliasId(currAliasId);
        mapCurrCtx.put(op, new GenMapRedCtx(currTask, currAliasId));

        QBParseInfo parseInfo = parseCtx.getQB().getParseInfo();
        if (parseInfo.isAnalyzeCommand()) {
          boolean partialScan = parseInfo.isPartialScanAnalyzeCommand();
          boolean noScan = parseInfo.isNoScanAnalyzeCommand();
          if (inputFormat.equals(OrcInputFormat.class) && (noScan || partialScan)) {

            // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS partialscan;
            // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS noscan;
            // There will not be any MR or Tez job above this task
            StatsNoJobWork snjWork = new StatsNoJobWork(parseCtx.getQB().getParseInfo().getTableSpec());
            snjWork.setStatsReliable(parseCtx.getConf().getBoolVar(
                HiveConf.ConfVars.HIVE_STATS_RELIABLE));
            Task<StatsNoJobWork> snjTask = TaskFactory.get(snjWork, parseCtx.getConf());
            ctx.setCurrTask(snjTask);
            ctx.setCurrTopOp(null);
            ctx.getRootTasks().clear();
            ctx.getRootTasks().add(snjTask);
          } else {
            // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS;
            // The plan consists of a simple MapRedTask followed by a StatsTask.
            // The MR task is just a simple TableScanOperator

            StatsWork statsWork = new StatsWork(parseCtx.getQB().getParseInfo().getTableSpec());
            statsWork.setAggKey(op.getConf().getStatsAggPrefix());
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
            if (parseInfo.isNoScanAnalyzeCommand()) {
              statsTask.setParentTasks(null);
              statsWork.setNoScanAnalyzeCommand(true);
              ctx.getRootTasks().remove(currTask);
              ctx.getRootTasks().add(statsTask);
            }

            // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS partialscan;
            if (parseInfo.isPartialScanAnalyzeCommand()) {
              handlePartialScanCommand(op, ctx, parseCtx, currTask, parseInfo, statsWork, statsTask);
            }

            currWork.getMapWork().setGatheringStats(true);
            if (currWork.getReduceWork() != null) {
              currWork.getReduceWork().setGatheringStats(true);
            }

            // NOTE: here we should use the new partition predicate pushdown API to get a list of
            // pruned list,
            // and pass it to setTaskPlan as the last parameter
            Set<Partition> confirmedPartns = GenMapRedUtils
                .getConfirmedPartitionsForScan(parseInfo);
            if (confirmedPartns.size() > 0) {
              Table source = parseCtx.getQB().getMetaData().getTableForAlias(alias);
              List<String> partCols = GenMapRedUtils.getPartitionColumns(parseInfo);
              PrunedPartitionList partList = new PrunedPartitionList(source, confirmedPartns, partCols, false);
              GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, currTask, false, ctx, partList);
            } else { // non-partitioned table
              GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, currTask, false, ctx);
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
      ParseContext parseCtx, Task<? extends Serializable> currTask, QBParseInfo parseInfo,
      StatsWork statsWork, Task<StatsWork> statsTask) throws SemanticException {
    String aggregationKey = op.getConf().getStatsAggPrefix();
    StringBuffer aggregationKeyBuffer = new StringBuffer(aggregationKey);
    List<Path> inputPaths = GenMapRedUtils.getInputPathsForPartialScan(parseInfo,
        aggregationKeyBuffer);
    aggregationKey = aggregationKeyBuffer.toString();

    // scan work
    PartialScanWork scanWork = new PartialScanWork(inputPaths);
    scanWork.setMapperCannotSpanPartns(true);
    scanWork.setAggKey(aggregationKey);

    // stats work
    statsWork.setPartialScanAnalyzeCommand(true);

    // partial scan task
    DriverContext driverCxt = new DriverContext();
    Task<PartialScanWork> psTask = TaskFactory.get(scanWork, parseCtx.getConf());
    psTask.initialize(parseCtx.getConf(), null, driverCxt);
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
