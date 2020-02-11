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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Implements predicate pushdown. Predicate pushdown is a term borrowed from
 * relational databases even though for Hive it is predicate pushup. The basic
 * idea is to process expressions as early in the plan as possible. The default
 * plan generation adds filters where they are seen but in some instances some
 * of the filter expressions can be pushed nearer to the operator that sees this
 * particular data for the first time. e.g. select a.*, b.* from a join b on
 * (a.col1 = b.col1) where a.col1 &gt; 20 and b.col2 &gt; 40
 *
 * For the above query, the predicates (a.col1 &gt; 20) and (b.col2 &gt; 40), without
 * predicate pushdown, would be evaluated after the join processing has been
 * done. Suppose the two predicates filter out most of the rows from a and b,
 * the join is unnecessarily processing these rows. With predicate pushdown,
 * these two predicates will be processed before the join.
 *
 * Predicate pushdown is disabled by setting hive.optimize.ppd to false. It is
 * enabled by default.
 *
 * The high-level algorithm is describe here - An operator is processed after
 * all its children have been processed - An operator processes its own
 * predicates and then merges (conjunction) with the processed predicates of its
 * children. In case of multiple children, there are combined using disjunction
 * (OR). - A predicate expression is processed for an operator using the
 * following steps - If the expr is a constant then it is a candidate for
 * predicate pushdown - If the expr is a col reference then it is a candidate
 * and its alias is noted - If the expr is an index and both the array and index
 * expr are treated as children - If the all child expr are candidates for
 * pushdown and all of the expression reference only one alias from the
 * operator's RowResolver then the current expression is also a candidate One
 * key thing to note is that some operators (Select, ReduceSink, GroupBy, Join
 * etc) change the columns as data flows through them. In such cases the column
 * references are replaced by the corresponding expression in the input data.
 */
public class PredicatePushDown extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(PredicatePushDown.class);
  private ParseContext pGraphContext;

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    pGraphContext = pctx;

    // create a the context for walking operators
    OpWalkerInfo opWalkerInfo = new OpWalkerInfo(pGraphContext);

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(new RuleRegExp("R1",
      FilterOperator.getOperatorName() + "%"),
      OpProcFactory.getFilterProc());
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
    SemanticDispatcher disp = new DefaultRuleDispatcher(OpProcFactory.getDefaultProc(),
        opRules, opWalkerInfo);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

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
