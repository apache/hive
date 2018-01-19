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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.OptimizeSparkProcContext;
import org.apache.hadoop.hive.ql.parse.spark.SparkPartitionPruningSinkOperator;

/**
 * Check if dynamic partition pruning should be disabled.  Currently the following 2 cases
 * checked.
 * 1.  The expected number of keys for dynamic pruning is too large
 * 2.  If DPP enabled only for mapjoin and join is not a map join.
 *
 * Cloned from RemoveDynamicPruningBySize
 */
public class SparkRemoveDynamicPruning implements NodeProcessor {

  static final private Logger LOG =
      LoggerFactory.getLogger(SparkRemoveDynamicPruning.class.getName());

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procContext,
      Object... nodeOutputs)
      throws SemanticException {

    OptimizeSparkProcContext context = (OptimizeSparkProcContext) procContext;
    boolean remove = false;

    SparkPartitionPruningSinkOperator op = (SparkPartitionPruningSinkOperator) nd;
    SparkPartitionPruningSinkDesc desc = op.getConf();

    if (context.getConf().isSparkDPPOnlyMapjoin() &&
        !op.isWithMapjoin()) {
      LOG.info("Disabling dynamic partition pruning based on: " + desc.getTableScan().getName()
          + ". This is not part of a map join.");
      remove = true;
    }
    else if (desc.getStatistics().getDataSize() > context.getConf()
        .getLongVar(ConfVars.SPARK_DYNAMIC_PARTITION_PRUNING_MAX_DATA_SIZE)) {
      LOG.info("Disabling dynamic partition pruning based on: "
          + desc.getTableScan().getName()
          + ". Expected data size is too big: " + desc.getStatistics().getDataSize());
      remove = true;
    }

    if (remove) {
      // at this point we've found the fork in the op pipeline that has the pruning as a child plan.
      OperatorUtils.removeBranch(op);
    }

    return false;
  }
}
