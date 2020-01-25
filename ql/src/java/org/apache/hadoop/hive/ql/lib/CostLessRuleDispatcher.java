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

package org.apache.hadoop.hive.ql.lib;

import java.util.Stack;

import com.google.common.collect.SetMultimap;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.ASTNode;

/**
 * Dispatches calls to relevant method in processor. The user registers various
 * rules with the dispatcher, and the processor corresponding to the type of node
 */
public class CostLessRuleDispatcher implements SemanticDispatcher {

  private final SetMultimap<Integer, SemanticNodeProcessor> procRules;
  private final NodeProcessorCtx procCtx;
  private final SemanticNodeProcessor defaultProc;

  /**
   * Constructor.
   *
   * @param defaultProc default processor to be fired if no rule matches
   * @param rules       Map mapping the node's type to processor
   * @param procCtx     operator processor context, which is opaque to the dispatcher
   */
  public CostLessRuleDispatcher(SemanticNodeProcessor defaultProc, SetMultimap<Integer, SemanticNodeProcessor> rules,
                                NodeProcessorCtx procCtx) {
    this.defaultProc = defaultProc;
    procRules = rules;
    this.procCtx = procCtx;
  }

  /**
   * Dispatcher function.
   *
   * @param nd      operator to process
   * @param ndStack the operators encountered so far
   * @throws SemanticException
   */
  @Override public Object dispatch(Node nd, Stack<Node> ndStack, Object... nodeOutputs)
      throws SemanticException {

    int nodeType = ((ASTNode) nd).getType();
    SemanticNodeProcessor processor = this.defaultProc;
    if (this.procRules.containsKey(nodeType)) {
      processor = this.procRules.get(((ASTNode) nd).getType()).iterator().next();
    }
    return processor.process(nd, ndStack, procCtx, nodeOutputs);
  }
}

