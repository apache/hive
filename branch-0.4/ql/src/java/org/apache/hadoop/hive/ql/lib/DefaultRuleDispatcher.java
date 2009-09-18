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

package org.apache.hadoop.hive.ql.lib;

import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Dispatches calls to relevant method in processor. The user registers various rules with the dispatcher, and
 * the processor corresponding to closest matching rule is fired.
 */
public class DefaultRuleDispatcher implements Dispatcher {
  
  private Map<Rule, NodeProcessor>  procRules;
  private NodeProcessorCtx          procCtx;
  private NodeProcessor             defaultProc;

  /**
   * constructor
   * @param defaultProc default processor to be fired if no rule matches
   * @param rules operator processor that handles actual processing of the node
   * @param procCtx operator processor context, which is opaque to the dispatcher
   */
  public DefaultRuleDispatcher(NodeProcessor defaultProc, 
                               Map<Rule, NodeProcessor> rules, NodeProcessorCtx procCtx) {
    this.defaultProc = defaultProc;
    this.procRules = rules;
    this.procCtx   = procCtx;
  }

  /**
   * dispatcher function
   * @param nd operator to process
   * @param ndStack the operators encountered so far
   * @throws SemanticException
   */
  public Object dispatch(Node nd, Stack<Node> ndStack, Object... nodeOutputs) 
    throws SemanticException {

    // find the firing rule
    // find the rule from the stack specified
    Rule rule = null;
    int minCost = Integer.MAX_VALUE;
    for (Rule r : procRules.keySet()) {
      int cost = r.cost(ndStack);
      if ((cost >= 0) && (cost <= minCost)) {
        minCost = cost;
        rule = r;
      }
    }

    NodeProcessor proc;

    if (rule == null)
      proc = defaultProc;
    else
      proc = procRules.get(rule);

    // Do nothing in case proc is null
    if (proc != null) {
      // Call the process function
      return proc.process(nd, ndStack, procCtx, nodeOutputs);
    }
    else
      return null;
  }
}
