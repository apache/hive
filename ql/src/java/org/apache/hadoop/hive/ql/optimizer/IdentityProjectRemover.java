/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/** This optimization tries to remove {@link SelectOperator} from tree which don't do any
 * processing except forwarding columns from its parent to its children.
 * e.g., select * from (select * from src where key = value) t1 join (select * from src where key = value) t2;
 * Query tree
 *
 *  Without this optimization:
 *
 *  TS -> FIL -> SEL -> RS ->
 *                             JOIN -> SEL -> FS
 *  TS -> FIL -> SEL -> RS ->
 *
 *  With this optimization
 *
 *  TS -> FIL -> RS ->
 *                      JOIN -> FS
 *  TS -> FIL -> RS ->
 *
 *  Note absence of select operator after filter and after join operator.
 *  Also, see : identity_proj_remove.q
 */
public class IdentityProjectRemover extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(IdentityProjectRemover.class);
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    // 0. We check the conditions to apply this transformation,
    //    if we do not meet them we bail out
    final boolean cboEnabled = HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVE_CBO_ENABLED);
    final boolean returnPathEnabled = HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP);
    final boolean cboSucceeded = pctx.getContext().isCboSucceeded();
    if(cboEnabled && returnPathEnabled && cboSucceeded) {
      return pctx;
    }

    // 1. We apply the transformation
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1",
      "(" + SelectOperator.getOperatorName() + "%)"), new ProjectRemover());
    GraphWalker ogw = new DefaultGraphWalker(new DefaultRuleDispatcher(null, opRules, null));
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private static class ProjectRemover implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
    	
      SelectOperator sel = (SelectOperator)nd;
      List<Operator<? extends OperatorDesc>> parents = sel.getParentOperators();
      if (parents.size() != 1 || parents.get(0) instanceof LateralViewForwardOperator) {
        // Multi parents, cant handle that.
        // Right now, we do not remove projection on top of
        // LateralViewForward operators.
        return null;
      }
      Operator<? extends OperatorDesc> parent = parents.get(0);
      if (parent instanceof ReduceSinkOperator && Iterators.any(sel.getChildOperators().iterator(),
          Predicates.instanceOf(ReduceSinkOperator.class))) {
        // For RS-SEL-RS case. reducer operator in reducer task cannot be null in task compiler
        return null;
      }
      List<Operator<? extends OperatorDesc>> ancestorList = new ArrayList<Operator<? extends OperatorDesc>>();
      ancestorList.addAll(sel.getParentOperators());
      while (!ancestorList.isEmpty()) {
        Operator<? extends OperatorDesc> curParent = ancestorList.remove(0);
            // PTF need a SelectOp.
        if ((curParent instanceof PTFOperator)) {
          return null;
        }
        if ((curParent instanceof FilterOperator) && curParent.getParentOperators() != null) {
          ancestorList.addAll(curParent.getParentOperators());
        }
      }

      if(sel.isIdentitySelect()) {
        parent.removeChildAndAdoptItsChildren(sel);
        LOG.debug("Identity project remover optimization removed : " + sel);
      }
      return null;
    }
  }
}
