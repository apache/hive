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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.ForwardWalker;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class HiveOpConverterPostProc extends Transform {

  private ParseContext                                  pctx;
  private Map<String, Operator<? extends OperatorDesc>> aliasToOpInfo;

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    // 0. We check the conditions to apply this transformation,
    //    if we do not meet them we bail out
    final boolean cboEnabled = HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVE_CBO_ENABLED);
    final boolean returnPathEnabled = HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVE_CBO_RETPATH_HIVEOP);
    final boolean cboSucceeded = pctx.getContext().isCboSucceeded();
    if(!(cboEnabled && returnPathEnabled && cboSucceeded)) {
      return pctx;
    }

    // 1. Initialize aux data structures
    this.pctx = pctx;
    this.aliasToOpInfo = new HashMap<String, Operator<? extends OperatorDesc>>();

    // 2. Trigger transformation
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", JoinOperator.getOperatorName() + "%"), new JoinAnnotate());
    opRules.put(new RuleRegExp("R2", TableScanOperator.getOperatorName() + "%"), new TableScanAnnotate());

    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    GraphWalker ogw = new ForwardWalker(disp);

    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private class JoinAnnotate implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      JoinOperator joinOp = (JoinOperator) nd;

      // 1. Additional data structures needed for the join optimization
      //    through Hive
      String[] baseSrc = new String[joinOp.getParentOperators().size()];
      String[] rightAliases = new String[joinOp.getParentOperators().size()-1];
      for (int i = 0; i < joinOp.getParentOperators().size(); i++) {
        ReduceSinkOperator rsOp = (ReduceSinkOperator) joinOp.getParentOperators().get(i);
        Set<String> aliases = rsOp.getSchema().getTableNames();
        if (aliases == null || aliases.size() != 1) {
          throw new SemanticException(
              "In return path join annotate rule, we find " + aliases == null ? null : aliases
                  .size() + " aliases for " + rsOp.toString());
        }
        baseSrc[i] = aliases.iterator().next();
        if (i == 0) {
          joinOp.getConf().setLeftAlias(baseSrc[i]);
        } else {
          rightAliases[i-1] = baseSrc[i];
        }
      }
      joinOp.getConf().setBaseSrc(baseSrc);
      joinOp.getConf().setRightAliases(rightAliases);
      joinOp.getConf().setAliasToOpInfo(aliasToOpInfo);

      // 2. Use self alias
      Set<String> aliases = joinOp.getSchema().getTableNames();
      if (aliases == null || aliases.size() != 1) {
        throw new SemanticException(
            "In return path join annotate rule, we find " + aliases == null ? null : aliases
                .size() + " aliases for " + joinOp.toString());
      }
      final String joinOpAlias = aliases.iterator().next();
      aliasToOpInfo.put(joinOpAlias, joinOp);

      // 3. Populate other data structures
      pctx.getJoinOps().add(joinOp);

      return null;
    }
  }


  private class TableScanAnnotate implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator tableScanOp = (TableScanOperator) nd;

      // 1. Get alias from topOps
      String opAlias = null;
      for (Map.Entry<String, TableScanOperator> topOpEntry : pctx.getTopOps().entrySet()) {
        if (topOpEntry.getValue() == tableScanOp) {
          opAlias = topOpEntry.getKey();
        }
      }

      assert opAlias != null;

      // 2. Add alias to 1) aliasToOpInfo and 2) opToAlias
      aliasToOpInfo.put(opAlias, tableScanOp);

      return null;
    }
  }

}
