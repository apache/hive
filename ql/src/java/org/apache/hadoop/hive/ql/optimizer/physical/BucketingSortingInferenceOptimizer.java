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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecDriver;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleExactMatch;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 *
 * BucketingSortingInferenceOptimizer.
 *
 * For each map reduce task, attmepts to infer bucketing and sorting metadata for the outputs.
 *
 * Currently only map reduce tasks which produce final output have there output metadata inferred,
 * but it can be extended to intermediate tasks as well.
 *
 * This should be run as the last physical optimizer, as other physical optimizers may invalidate
 * the inferences made.  If a physical optimizer depends on the results and is designed to
 * carefully maintain these inferences, it may follow this one.
 */
public class BucketingSortingInferenceOptimizer implements PhysicalPlanResolver {

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    inferBucketingSorting(Utilities.getMRTasks(pctx.rootTasks));
    return pctx;
  }

  /**
   * For each map reduce task, if it has a reducer, infer whether or not the final output of the
   * reducer is bucketed and/or sorted
   *
   * @param mapRedTasks
   * @throws SemanticException
   */
  private void inferBucketingSorting(List<ExecDriver> mapRedTasks) throws SemanticException {
    for (ExecDriver mapRedTask : mapRedTasks) {

      // For now this only is used to determine the bucketing/sorting of outputs, in the future
      // this can be removed to optimize the query plan based on the bucketing/sorting properties
      // of the outputs of intermediate map reduce jobs.
      if (!mapRedTask.getWork().isFinalMapRed()) {
        continue;
      }

      if (mapRedTask.getWork().getReduceWork() == null) {
        continue;
      }
      Operator<? extends OperatorDesc> reducer = mapRedTask.getWork().getReduceWork().getReducer();

      // uses sampling, which means it's not bucketed
      boolean disableBucketing = mapRedTask.getWork().getMapWork().getSamplingType() > 0;
      BucketingSortingCtx bCtx = new BucketingSortingCtx(disableBucketing);

      // RuleRegExp rules are used to match operators anywhere in the tree
      // RuleExactMatch rules are used to specify exactly what the tree should look like
      // In particular, this guarantees that the first operator is the reducer
      // (and its parent(s) are ReduceSinkOperators) since it begins walking the tree from
      // the reducer.
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      opRules.put(new RuleRegExp("R1", SelectOperator.getOperatorName() + "%"),
          BucketingSortingOpProcFactory.getSelProc());
      // Matches only GroupByOperators which are reducers, rather than map group by operators,
      // or multi group by optimization specific operators
      opRules.put(new RuleExactMatch("R2", new String[]{GroupByOperator.getOperatorName()}),
          BucketingSortingOpProcFactory.getGroupByProc());
      // Matches only JoinOperators which are reducers, rather than map joins, SMB map joins, etc.
      opRules.put(new RuleExactMatch("R3", new String[]{JoinOperator.getOperatorName()}),
          BucketingSortingOpProcFactory.getJoinProc());
      opRules.put(new RuleRegExp("R5", FileSinkOperator.getOperatorName() + "%"),
          BucketingSortingOpProcFactory.getFileSinkProc());
      opRules.put(new RuleRegExp("R7", FilterOperator.getOperatorName() + "%"),
          BucketingSortingOpProcFactory.getFilterProc());
      opRules.put(new RuleRegExp("R8", LimitOperator.getOperatorName() + "%"),
          BucketingSortingOpProcFactory.getLimitProc());
      opRules.put(new RuleRegExp("R9", LateralViewForwardOperator.getOperatorName() + "%"),
          BucketingSortingOpProcFactory.getLateralViewForwardProc());
      opRules.put(new RuleRegExp("R10", LateralViewJoinOperator.getOperatorName() + "%"),
          BucketingSortingOpProcFactory.getLateralViewJoinProc());
      // Matches only ForwardOperators which are preceded by some other operator in the tree,
      // in particular it can't be a reducer (and hence cannot be one of the ForwardOperators
      // added by the multi group by optimization)
      opRules.put(new RuleRegExp("R11", ".+" + ForwardOperator.getOperatorName() + "%"),
          BucketingSortingOpProcFactory.getForwardProc());
      // Matches only ForwardOperators which are reducers and are followed by GroupByOperators
      // (specific to the multi group by optimization)
      opRules.put(new RuleExactMatch("R12",new String[]{ ForwardOperator.getOperatorName(),
          GroupByOperator.getOperatorName()}),
          BucketingSortingOpProcFactory.getMultiGroupByProc());

      // The dispatcher fires the processor corresponding to the closest matching rule and passes
      // the context along
      Dispatcher disp = new DefaultRuleDispatcher(BucketingSortingOpProcFactory.getDefaultProc(),
          opRules, bCtx);
      GraphWalker ogw = new PreOrderWalker(disp);

      // Create a list of topop nodes
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.add(reducer);
      ogw.startWalking(topNodes, null);

      mapRedTask.getWork().getMapWork().getBucketedColsByDirectory().putAll(bCtx.getBucketedColsByDirectory());
      mapRedTask.getWork().getMapWork().getSortedColsByDirectory().putAll(bCtx.getSortedColsByDirectory());
    }
  }
}
