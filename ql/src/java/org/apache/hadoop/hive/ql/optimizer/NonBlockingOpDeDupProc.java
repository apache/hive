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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

/**
 * merges SEL-SEL or FIL-FIL into single operator
 */
public class NonBlockingOpDeDupProc implements Transform {

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    String SEL = SelectOperator.getOperatorName();
    String FIL = FilterOperator.getOperatorName();
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", SEL + "%" + SEL + "%"), new SelectDedup());
    opRules.put(new RuleRegExp("R2", FIL + "%" + FIL + "%"), new FilterDedup());

    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  static class SelectDedup implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator cSEL = (SelectOperator) nd;
      SelectOperator pSEL = (SelectOperator) stack.get(stack.size() - 2);
      if (pSEL.getNumChild() > 1) {
        return null;  // possible if all children have same expressions, but not likely.
      }
      if (pSEL.getConf().isSelStarNoCompute()) {
        // SEL(no-compute)-SEL. never seen this condition
        // and also, removing parent is not safe in current graph walker
        return null;
      }

      // For SEL-SEL(compute) case, move column exprs/names of child to parent.
      if (!cSEL.getConf().isSelStarNoCompute()) {
        Set<String> funcOutputs = getFunctionOutputs(
            pSEL.getConf().getOutputColumnNames(), pSEL.getConf().getColList());

        List<ExprNodeDesc> sources = cSEL.getConf().getColList();
        if (!funcOutputs.isEmpty() && !checkReferences(sources, funcOutputs)) {
          return null;
        }
        pSEL.getConf().setColList(ExprNodeDescUtils.backtrack(sources, cSEL, pSEL));
        pSEL.getConf().setOutputColumnNames(cSEL.getConf().getOutputColumnNames());

        // updates schema only (this should be the last optimizer modifying operator tree)
        pSEL.setSchema(cSEL.getSchema());
      }

      pSEL.getConf().setSelectStar(cSEL.getConf().isSelectStar());

      pSEL.removeChildAndAdoptItsChildren(cSEL);
      cSEL.setParentOperators(null);
      cSEL.setChildOperators(null);
      cSEL = null;

      return null;
    }

    // collect name of output columns which is result of function
    private Set<String> getFunctionOutputs(List<String> colNames, List<ExprNodeDesc> targets) {
      Set<String> functionOutputs = new HashSet<String>();
      for (int i = 0; i < targets.size(); i++) {
        if (targets.get(i) instanceof ExprNodeGenericFuncDesc) {
          functionOutputs.add(colNames.get(i));
        }
      }
      return functionOutputs;
    }

    // if any expression of child is referencing parent column which is result of function
    // twice or more, skip dedup.
    private boolean checkReferences(List<ExprNodeDesc> sources, Set<String> funcOutputs) {
      Set<String> ref = new HashSet<String>();
      for (ExprNodeDesc source : sources) {
        if (!checkReferences(source, funcOutputs, ref)) {
          return false;
        }
      }
      return true;
    }

    private boolean checkReferences(ExprNodeDesc expr, Set<String> funcOutputs, Set<String> ref) {
      if (expr instanceof ExprNodeColumnDesc) {
        String col = ((ExprNodeColumnDesc) expr).getColumn();
        if (funcOutputs.contains(col) && !ref.add(col)) {
          return false;
        }
      }
      if (expr.getChildren() != null) {
        for (ExprNodeDesc child : expr.getChildren()) {
          if (!checkReferences(child, funcOutputs, ref)) {
            return false;
          }
        }
      }
      return true;
    }
  }

  static class FilterDedup implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator cFIL = (FilterOperator) nd;
      FilterOperator pFIL = (FilterOperator) stack.get(stack.size() - 2);

      // Sampling predicates can be merged with predicates from children because PPD/PPR is
      // already applied. But to clarify the intention of sampling, just skips merging.
      if (pFIL.getConf().getIsSamplingPred()) {
        return null;
      }

      List<ExprNodeDesc> splits = new ArrayList<ExprNodeDesc>();
      ExprNodeDescUtils.split(cFIL.getConf().getPredicate(), splits);
      ExprNodeDescUtils.split(pFIL.getConf().getPredicate(), splits);

      pFIL.getConf().setPredicate(ExprNodeDescUtils.mergePredicates(splits));

      // if any of filter is sorted filter, it's sorted filter
      boolean sortedFilter = pFIL.getConf().isSortedFilter() || cFIL.getConf().isSortedFilter();
      pFIL.getConf().setSortedFilter(sortedFilter);

      pFIL.removeChildAndAdoptItsChildren(cFIL);
      cFIL.setParentOperators(null);
      cFIL.setChildOperators(null);
      cFIL = null;

      return null;
    }
  }
}
