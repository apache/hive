/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
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
import java.util.Map;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

//try to replace a bucket map join with a sorted merge map join
public class SortedMergeBucketMapJoinOptimizer extends Transform {

  private static final Logger LOG = LoggerFactory
      .getLogger(SortedMergeBucketMapJoinOptimizer.class.getName());

  public SortedMergeBucketMapJoinOptimizer() {
  }

  private void getListOfRejectedJoins(
    ParseContext pctx, SortBucketJoinProcCtx smbJoinContext)
    throws SemanticException {

    // Go through all joins - it should only contain selects and filters between
    // tablescan and join operators.
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(new RuleRegExp("R1", JoinOperator.getOperatorName() + "%"),
      getCheckCandidateJoin());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SemanticDispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, smbJoinContext);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    HiveConf conf = pctx.getConf();
    SortBucketJoinProcCtx smbJoinContext =
      new SortBucketJoinProcCtx(conf);

    // Get a list of joins which cannot be converted to a sort merge join
    // Only selects and filters operators are allowed between the table scan and
    // join currently. More operators can be added - the method supportAutomaticSortMergeJoin
    // dictates which operator is allowed
    getListOfRejectedJoins(pctx, smbJoinContext);

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    // go through all map joins and find out all which have enabled bucket map
    // join.
    opRules.put(new RuleRegExp("R1", MapJoinOperator.getOperatorName() + "%"),
        getSortedMergeBucketMapjoinProc(pctx));
    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along

    // There is no need for the user to specify mapjoin for it to be
    // converted to sort-merge join
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN)) {
      opRules.put(new RuleRegExp("R2", "JOIN%"),
        getSortedMergeJoinProc(pctx));
    }

    SemanticDispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, smbJoinContext);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pctx;
  }

  private SemanticNodeProcessor getSortedMergeBucketMapjoinProc(ParseContext pctx) {
    return new SortedMergeBucketMapjoinProc(pctx);
  }

  private SemanticNodeProcessor getSortedMergeJoinProc(ParseContext pctx) {
    return new SortedMergeJoinProc(pctx);
  }

  private SemanticNodeProcessor getDefaultProc() {
    return new SemanticNodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
                            NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {
        return null;
      }
    };
  }

  // check if the join operator encountered is a candidate for being converted
  // to a sort-merge join
  private SemanticNodeProcessor getCheckCandidateJoin() {
    return new SemanticNodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
        SortBucketJoinProcCtx smbJoinContext = (SortBucketJoinProcCtx)procCtx;
        JoinOperator joinOperator = (JoinOperator)nd;
        int size = stack.size();
        if (!(stack.get(size-1) instanceof JoinOperator) ||
            !(stack.get(size-2) instanceof ReduceSinkOperator)) {
          smbJoinContext.getRejectedJoinOps().add(joinOperator);
          return null;
        }

        // If any operator in the stack does not support a auto-conversion, this join should
        // not be converted.
        for (int pos = size -3; pos >= 0; pos--) {
          Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>)stack.get(pos);
          if (!op.supportAutomaticSortMergeJoin()) {
            smbJoinContext.getRejectedJoinOps().add(joinOperator);
            return null;
          }
        }

        return null;
      }
    };
  }
}
