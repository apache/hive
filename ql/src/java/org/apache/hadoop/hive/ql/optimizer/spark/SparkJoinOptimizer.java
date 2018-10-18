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

package org.apache.hadoop.hive.ql.optimizer.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.OptimizeSparkProcContext;

import java.util.Stack;

/**
 * Converts a join to a more optimized join for the Spark path.
 * Delegates to a more specialized join processor.
 */
public class SparkJoinOptimizer implements NodeProcessor {

  private SparkSortMergeJoinOptimizer smbJoinOptimizer;
  private SparkMapJoinOptimizer mapJoinOptimizer;

  public SparkJoinOptimizer(ParseContext procCtx) {
    smbJoinOptimizer = new SparkSortMergeJoinOptimizer(procCtx);
    mapJoinOptimizer = new SparkMapJoinOptimizer();
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
    OptimizeSparkProcContext context = (OptimizeSparkProcContext) procCtx;
    HiveConf conf = context.getConf();

    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN_TOMAPJOIN)) {
      Object mapJoinOp = mapJoinOptimizer.process(nd, stack, procCtx, nodeOutputs);
      if (mapJoinOp == null) {
        smbJoinOptimizer.process(nd, stack, procCtx, nodeOutputs);
      }
    } else {
      Object sortMergeJoinOp = smbJoinOptimizer.process(nd, stack, procCtx, nodeOutputs);
      if (sortMergeJoinOp == null) {
        mapJoinOptimizer.process(nd, stack, procCtx, nodeOutputs);
      }
    }
    return null;
  }
}
