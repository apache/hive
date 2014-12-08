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

package org.apache.hadoop.hive.ql.optimizer.spark;

import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.physical.GenSparkSkewJoinProcessor;
import org.apache.hadoop.hive.ql.optimizer.physical.SkewJoinProcFactory;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.io.Serializable;
import java.util.Stack;

/**
 * Spark-version of SkewJoinProcFactory
 */
public class SparkSkewJoinProcFactory {
  private SparkSkewJoinProcFactory() {
    // prevent instantiation
  }

  public static NodeProcessor getDefaultProc() {
    return SkewJoinProcFactory.getDefaultProc();
  }

  public static NodeProcessor getJoinProc() {
    return new SparkSkewJoinJoinProcessor();
  }

  public static class SparkSkewJoinJoinProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      SparkSkewJoinResolver.SparkSkewJoinProcCtx context =
          (SparkSkewJoinResolver.SparkSkewJoinProcCtx) procCtx;
      JoinOperator op = (JoinOperator) nd;
      if (op.getConf().isFixedAsSorted()) {
        return null;
      }
      ParseContext parseContext = context.getParseCtx();
      Task<? extends Serializable> currentTsk = context.getCurrentTask();
      if (currentTsk instanceof SparkTask) {
        GenSparkSkewJoinProcessor.processSkewJoin(op, currentTsk,
            context.getReducerToReduceWork().get(op), parseContext);
      }
      return null;
    }
  }
}
