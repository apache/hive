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

package org.apache.hadoop.hive.ql.optimizer.stats.annotation;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.LevelOrderWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class AnnotateWithStatistics extends Transform {

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    AnnotateStatsProcCtx aspCtx = new AnnotateStatsProcCtx(pctx);

    // create a walker which walks the tree in a BFS manner while maintaining the
    // operator stack. The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("TS", TableScanOperator.getOperatorName() + "%"),
        StatsRulesProcFactory.getTableScanRule());
    opRules.put(new RuleRegExp("SEL", SelectOperator.getOperatorName() + "%"),
        StatsRulesProcFactory.getSelectRule());
    opRules.put(new RuleRegExp("FIL", FilterOperator.getOperatorName() + "%"),
        StatsRulesProcFactory.getFilterRule());
    opRules.put(new RuleRegExp("GBY", GroupByOperator.getOperatorName() + "%"),
        StatsRulesProcFactory.getGroupByRule());
    opRules.put(new RuleRegExp("JOIN", CommonJoinOperator.getOperatorName() + "%|"
        + MapJoinOperator.getOperatorName() + "%"), StatsRulesProcFactory.getJoinRule());
    opRules.put(new RuleRegExp("LIM", LimitOperator.getOperatorName() + "%"),
        StatsRulesProcFactory.getLimitRule());
    opRules.put(new RuleRegExp("RS", ReduceSinkOperator.getOperatorName() + "%"),
        StatsRulesProcFactory.getReduceSinkRule());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(StatsRulesProcFactory.getDefaultRule(), opRules,
        aspCtx);
    GraphWalker ogw = new LevelOrderWalker(disp, 0);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pctx;
  }
}
