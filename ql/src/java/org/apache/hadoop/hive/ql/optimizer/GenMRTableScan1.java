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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.StatsWork;
/**
 * Processor for the rule - table scan.
 */
public class GenMRTableScan1 implements NodeProcessor {
  public GenMRTableScan1() {
  }

  /**
   * Table Sink encountered.
   *
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
    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx.getMapCurrCtx();

    // create a dummy MapReduce task
    MapredWork currWork = GenMapRedUtils.getMapRedWork(parseCtx);
    Task<? extends Serializable> currTask = TaskFactory.get(currWork, parseCtx.getConf());
    Operator<? extends Serializable> currTopOp = op;
    ctx.setCurrTask(currTask);
    ctx.setCurrTopOp(currTopOp);

    for (String alias : parseCtx.getTopOps().keySet()) {
      Operator<? extends Serializable> currOp = parseCtx.getTopOps().get(alias);
      if (currOp == op) {
        String currAliasId = alias;
        ctx.setCurrAliasId(currAliasId);
        mapCurrCtx.put(op, new GenMapRedCtx(currTask, currTopOp, currAliasId));

        QBParseInfo parseInfo = parseCtx.getQB().getParseInfo();
        if (parseInfo.isAnalyzeCommand()) {

          //   ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS;
          // The plan consists of a simple MapRedTask followed by a StatsTask.
          // The MR task is just a simple TableScanOperator

          StatsWork statsWork = new StatsWork(parseCtx.getQB().getParseInfo().getTableSpec());
          statsWork.setAggKey(op.getConf().getStatsAggPrefix());
          statsWork.setStatsReliable(
            parseCtx.getConf().getBoolVar(HiveConf.ConfVars.HIVE_STATS_RELIABLE));
          Task<StatsWork> statsTask = TaskFactory.get(statsWork, parseCtx.getConf());
          currTask.addDependentTask(statsTask);
          if (!ctx.getRootTasks().contains(currTask)) {
            ctx.getRootTasks().add(currTask);
          }
          currWork.setGatheringStats(true);
          // NOTE: here we should use the new partition predicate pushdown API to get a list of pruned list,
          // and pass it to setTaskPlan as the last parameter
          Set<Partition> confirmedPartns = new HashSet<Partition>();
          tableSpec tblSpec = parseInfo.getTableSpec();
          if (tblSpec.specType == tableSpec.SpecType.STATIC_PARTITION) {
            // static partition
            confirmedPartns.add(tblSpec.partHandle);
          } else if (tblSpec.specType == tableSpec.SpecType.DYNAMIC_PARTITION) {
            // dynamic partition
            confirmedPartns.addAll(tblSpec.partitions);
          }
          if (confirmedPartns.size() > 0) {
            Table source = parseCtx.getQB().getMetaData().getTableForAlias(alias);
            PrunedPartitionList partList = new PrunedPartitionList(source, confirmedPartns,
                new HashSet<Partition>(), null);
            GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, currWork, false, ctx, partList);
          } else { // non-partitioned table
            GenMapRedUtils.setTaskPlan(currAliasId, currTopOp, currWork, false, ctx);
          }
        }
        return null;
      }
    }
    assert false;
    return null;
  }

}
