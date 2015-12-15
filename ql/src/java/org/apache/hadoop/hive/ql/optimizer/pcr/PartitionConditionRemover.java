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

package org.apache.hadoop.hive.ql.optimizer.pcr;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * The transformation step that does partition condition remover.
 *
 */
public class PartitionConditionRemover extends Transform {

  // The log
  private static final Logger LOG = LoggerFactory
      .getLogger("hive.ql.optimizer.pcr.PartitionConditionRemover");

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.hadoop.hive.ql.optimizer.Transform#transform(org.apache.hadoop
   * .hive.ql.parse.ParseContext)
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    // create a the context for walking operators
    List<PcrOpWalkerCtx.OpToDeleteInfo> opToRemove =
        new ArrayList<PcrOpWalkerCtx.OpToDeleteInfo>();
    PcrOpWalkerCtx opWalkerCtx = new PcrOpWalkerCtx(pctx, opToRemove);

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1",
      "(" + TableScanOperator.getOperatorName() + "%"
      + FilterOperator.getOperatorName() + "%)|("
      + TableScanOperator.getOperatorName() + "%"
      + FilterOperator.getOperatorName() + "%"
      + FilterOperator.getOperatorName() + "%)"),
      PcrOpProcFactory.getFilterProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(PcrOpProcFactory.getDefaultProc(),
        opRules, opWalkerCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    for (PcrOpWalkerCtx.OpToDeleteInfo entry : opToRemove) {
      entry.getParent().removeChildAndAdoptItsChildren(entry.getOperator());
    }

    return pctx;
  }

}
