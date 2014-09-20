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

package org.apache.hadoop.hive.ql.parse.spark;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Map;
import java.util.Stack;

public class SparkMergeTaskProcessor implements NodeProcessor {

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
    GenSparkProcContext context = (GenSparkProcContext) procCtx;
    Operator<?> op = (Operator<?>) nd;
    Map<Operator<?>, SparkTask> opTable = context.opToTaskMap;
    SparkTask currentTask = opTable.get(context.currentRootOperator);
    if (!opTable.containsKey(op)) {
      opTable.put(op, currentTask);
    } else {
      // If this op has already been visited, since we visit temporary TS first,
      // also with the assumption that two paths from two different tembporary TS will NOT
      // meet, the current task must be the default task.
      // TODO: better we can prove that they'll never meet.
      SparkTask existingTask = opTable.get(op);
      if (currentTask == context.defaultTask && existingTask != context.defaultTask) {
        opTable.put(context.currentRootOperator, existingTask);
      }
    }

    return null;
  }
}
