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

package org.apache.hadoop.hive.ql.optimizer.lineage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * This class generates the lineage information for the columns
 * and tables from the plan before it goes through other
 * optimization phases.
 */
public class Generator implements Transform {

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.optimizer.Transform#transform(org.apache.hadoop.hive.ql.parse.ParseContext)
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    // Create the lineage context
    LineageCtx lCtx = new LineageCtx(pctx);

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%"),
      OpProcFactory.getTSProc());
    opRules.put(new RuleRegExp("R2", ScriptOperator.getOperatorName() + "%"),
      OpProcFactory.getTransformProc());
    opRules.put(new RuleRegExp("R3", UDTFOperator.getOperatorName() + "%"),
      OpProcFactory.getTransformProc());
    opRules.put(new RuleRegExp("R4", SelectOperator.getOperatorName() + "%"),
      OpProcFactory.getSelProc());
    opRules.put(new RuleRegExp("R5", GroupByOperator.getOperatorName() + "%"),
      OpProcFactory.getGroupByProc());
    opRules.put(new RuleRegExp("R6", UnionOperator.getOperatorName() + "%"),
      OpProcFactory.getUnionProc());
    opRules.put(new RuleRegExp("R7",
      CommonJoinOperator.getOperatorName() + "%|" + MapJoinOperator.getOperatorName() + "%"),
      OpProcFactory.getJoinProc());
    opRules.put(new RuleRegExp("R8", ReduceSinkOperator.getOperatorName() + "%"),
      OpProcFactory.getReduceSinkProc());
    opRules.put(new RuleRegExp("R9", LateralViewJoinOperator.getOperatorName() + "%"),
      OpProcFactory.getLateralViewJoinProc());
    opRules.put(new RuleRegExp("R10", PTFOperator.getOperatorName() + "%"),
        OpProcFactory.getTransformProc());

    // The dispatcher fires the processor corresponding to the closest matching rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(OpProcFactory.getDefaultProc(), opRules, lCtx);
    GraphWalker ogw = new PreOrderWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    // Transfer the index from the lineage context to the session state.
    if (SessionState.get() != null) {
      SessionState.get().getLineageState().setIndex(lCtx.getIndex());
    }

    return pctx;
  }

}
