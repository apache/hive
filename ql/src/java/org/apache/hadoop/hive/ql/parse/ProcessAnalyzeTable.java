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

import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.plan.StatsWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.BasicStatsWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.stats.BasicStatsNoJobTask;
import org.apache.hadoop.mapred.InputFormat;

/**
 * ProcessAnalyzeTable sets up work for the several variants of analyze table
 * (normal, no scan.) The plan at this point will be a single
 * table scan operator.
 */
public class ProcessAnalyzeTable implements SemanticNodeProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessAnalyzeTable.class.getName());

  // shared plan utils for tez
  private GenTezUtils utils = null;

  /**
   * Injecting the utils in the constructor facilitates testing
   */
  public ProcessAnalyzeTable(GenTezUtils utils) {
    this.utils = utils;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procContext,
      Object... nodeOutputs) throws SemanticException {

    GenTezProcContext context = (GenTezProcContext) procContext;

    TableScanOperator tableScan = (TableScanOperator) nd;

    ParseContext parseContext = context.parseContext;
    Table table = tableScan.getConf().getTableMetadata();
    Class<? extends InputFormat> inputFormat = table.getInputFormatClass();

    if (parseContext.getQueryProperties().isAnalyzeCommand()) {

      assert tableScan.getChildOperators() == null || tableScan.getChildOperators().size() == 0;

      String alias = null;
      for (String a : parseContext.getTopOps().keySet()) {
        if (tableScan == parseContext.getTopOps().get(a)) {
          alias = a;
        }
      }

      assert alias != null;
      TezWork tezWork = context.currentTask.getWork();
      if (BasicStatsNoJobTask.canUseBasicStats(table, inputFormat)) {
        // For ORC, Parquet and Iceberg tables, all the following statements are the same
        // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS
        // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS noscan;

        // There will not be any Tez job above this task
        StatsWork statWork = new StatsWork(table, parseContext.getConf());
        statWork.setFooterScan();

        // If partition is specified, get pruned partition list
        Set<Partition> confirmedParts = GenMapRedUtils.getConfirmedPartitionsForScan(tableScan);
        if (confirmedParts.size() > 0) {
          List<String> partCols = GenMapRedUtils.getPartitionColumns(tableScan);
          PrunedPartitionList partList = new PrunedPartitionList(table, confirmedParts, partCols, false);
          statWork.addInputPartitions(partList.getPartitions());
        }
        Task<StatsWork> snjTask = TaskFactory.get(statWork);
        snjTask.setParentTasks(null);
        context.rootTasks.remove(context.currentTask);
        context.rootTasks.add(snjTask);
        return true;
      } else {

        // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS;
        // The plan consists of a simple TezTask followed by a StatsTask.
        // The Tez task is just a simple TableScanOperator

        BasicStatsWork basicStatsWork = new BasicStatsWork(table.getTableSpec());
        basicStatsWork.setIsExplicitAnalyze(true);
        basicStatsWork.setNoScanAnalyzeCommand(parseContext.getQueryProperties().isNoScanAnalyzeCommand());
        StatsWork columnStatsWork = new StatsWork(table, basicStatsWork, parseContext.getConf());
        columnStatsWork.collectStatsFromAggregator(tableScan.getConf());

        columnStatsWork.setSourceTask(context.currentTask);
        Task<StatsWork> statsTask = TaskFactory.get(columnStatsWork);
        context.currentTask.addDependentTask(statsTask);

        // ANALYZE TABLE T [PARTITION (...)] COMPUTE STATISTICS noscan;
        // The plan consists of a StatsTask only.
        if (parseContext.getQueryProperties().isNoScanAnalyzeCommand()) {
          statsTask.setParentTasks(null);
          context.rootTasks.remove(context.currentTask);
          context.rootTasks.add(statsTask);
        }

        // NOTE: here we should use the new partition predicate pushdown API to
        // get a list of pruned list,
        // and pass it to setTaskPlan as the last parameter
        Set<Partition> confirmedPartns = GenMapRedUtils.getConfirmedPartitionsForScan(tableScan);
        PrunedPartitionList partitions = null;
        if (confirmedPartns.size() > 0) {
          List<String> partCols = GenMapRedUtils.getPartitionColumns(tableScan);
          partitions = new PrunedPartitionList(table, confirmedPartns, partCols, false);
        }

        MapWork w = utils.createMapWork(context, tableScan, tezWork, partitions);
        w.setGatheringStats(true);

        return true;
      }
    }

    return null;
  }
}
