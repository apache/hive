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
package org.apache.hadoop.hive.ql.ppd;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePredicatePushDown extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(SimplePredicatePushDown.class);
  private ParseContext pGraphContext;

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    pGraphContext = pctx;

    // create a the context for walking operators
    OpWalkerInfo opWalkerInfo = new OpWalkerInfo(pGraphContext);

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1",
      FilterOperator.getOperatorName() + "%"),
      OpProcFactory.getFilterSyntheticJoinPredicateProc());
    opRules.put(new RuleRegExp("R2",
      PTFOperator.getOperatorName() + "%"),
      OpProcFactory.getPTFProc());
    opRules.put(new RuleRegExp("R3",
      CommonJoinOperator.getOperatorName() + "%"),
      OpProcFactory.getJoinProc());
    opRules.put(new RuleRegExp("R4",
      TableScanOperator.getOperatorName() + "%"),
      OpProcFactory.getTSProc());
    opRules.put(new RuleRegExp("R5",
      ScriptOperator.getOperatorName() + "%"),
      OpProcFactory.getSCRProc());
    opRules.put(new RuleRegExp("R6",
      LimitOperator.getOperatorName() + "%"),
      OpProcFactory.getLIMProc());
    opRules.put(new RuleRegExp("R7",
      UDTFOperator.getOperatorName() + "%"),
      OpProcFactory.getUDTFProc());
    opRules.put(new RuleRegExp("R8",
      LateralViewForwardOperator.getOperatorName() + "%"),
      OpProcFactory.getLVFProc());
    opRules.put(new RuleRegExp("R9",
      LateralViewJoinOperator.getOperatorName() + "%"),
      OpProcFactory.getLVJProc());
    opRules.put(new RuleRegExp("R10",
        ReduceSinkOperator.getOperatorName() + "%"),
        OpProcFactory.getRSProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(OpProcFactory.getDefaultProc(),
        opRules, opWalkerInfo);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);

    if (LOG.isDebugEnabled()) {
      LOG.debug("After PPD:\n" + Operator.toString(pctx.getTopOps().values()));
    }
    return pGraphContext;
  }

}
