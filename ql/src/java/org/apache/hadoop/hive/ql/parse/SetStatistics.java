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

package org.apache.hadoop.hive.ql.parse;

import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * SetStatistics just invokes getStatistics on FileSinkOperators
 * to populate the stats in the operator tree
 */
public class SetStatistics implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(SetStatistics.class.getName());

  @SuppressWarnings("unchecked")
  @Override
  public Object process(Node nd, Stack<Node> stack,
      NodeProcessorCtx procContext, Object... nodeOutputs)
      throws SemanticException {

    OptimizeTezProcContext context = (OptimizeTezProcContext)procContext;
    FileSinkOperator sink = (FileSinkOperator) nd;
    try {
      sink.getStatistics(context.conf);
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    return false;
  }
}
