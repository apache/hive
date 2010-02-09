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
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Processor for the rule - no specific rule fired.
 */
public class GenMROperator implements NodeProcessor {

  public GenMROperator() {
  }

  /**
   * Reduce Scan encountered.
   * 
   * @param nd
   *          the reduce sink operator encountered
   * @param procCtx
   *          context
   */
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {
    GenMRProcContext ctx = (GenMRProcContext) procCtx;

    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx
        .getMapCurrCtx();
    GenMapRedCtx mapredCtx = mapCurrCtx.get(stack.get(stack.size() - 2));
    mapCurrCtx.put((Operator<? extends Serializable>) nd, new GenMapRedCtx(
        mapredCtx.getCurrTask(), mapredCtx.getCurrTopOp(), mapredCtx
        .getCurrAliasId()));
    return null;
  }
}
