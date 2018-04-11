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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

/**
 *
 * MetadataOnlyOptimizer determines to which TableScanOperators "metadata only"
 * optimization can be applied. Such operator must use only partition columns
 * (it is easy to check, because we are after column pruning and all places
 * where the data from the operator is used must go through GroupByOperator
 * distinct or distinct-like aggregations. Aggregation is distinct-like if
 * adding distinct wouldn't change the result, for example min, max.
 *
 * We cannot apply the optimization without group by, because the results depend
 * on the numbers of rows in partitions, for example count(hr) will count all
 * rows in matching partitions.
 *
 */
public class MetadataOnlyOptimizer implements PhysicalPlanResolver {
  static final Logger LOG = LoggerFactory.getLogger(MetadataOnlyOptimizer.class.getName());

  static class WalkerCtx implements NodeProcessorCtx {
    /* operators for which there is chance the optimization can be applied */
    private final HashSet<TableScanOperator> possible = new HashSet<TableScanOperator>();
    /* operators for which the optimization will be successful */
    private final HashSet<TableScanOperator> success = new HashSet<TableScanOperator>();

    /**
     * Sets operator as one for which there is a chance to apply optimization
     *
     * @param op
     *          the operator
     */
    public void setMayBeMetadataOnly(TableScanOperator op) {
      possible.add(op);
    }

    /** Convert all possible operators to success */
    public void convertMetadataOnly() {
      success.addAll(possible);
      possible.clear();
    }

    /**
     * Convert all possible operators to banned
     */
    public void convertNotMetadataOnly() {
      possible.clear();
      success.clear();
    }

    /**
     * Returns HashSet of collected operators for which the optimization may be
     * applicable.
     */
    public HashSet<TableScanOperator> getMayBeMetadataOnlyTableScans() {
      return possible;
    }

    /**
     * Returns HashSet of collected operators for which the optimization is
     * applicable.
     */
    public HashSet<TableScanOperator> getMetadataOnlyTableScans() {
      return success;
    }

  }

  static private class TableScanProcessor implements NodeProcessor {
    public TableScanProcessor() {
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator tsOp = (TableScanOperator) nd;
      WalkerCtx walkerCtx = (WalkerCtx) procCtx;
      List<Integer> colIDs = tsOp.getNeededColumnIDs();
      TableScanDesc desc = tsOp.getConf();
      boolean noColNeeded = (colIDs == null) || (colIDs.isEmpty());
      boolean noVCneeded = (desc == null) || (desc.getVirtualCols() == null)
                             || (desc.getVirtualCols().isEmpty());
      boolean isSkipHF = desc.isNeedSkipHeaderFooters();
      if (noColNeeded && noVCneeded && !isSkipHF) {
        walkerCtx.setMayBeMetadataOnly(tsOp);
      }
      return nd;
    }
  }

  static private class FileSinkProcessor implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      WalkerCtx walkerCtx = (WalkerCtx) procCtx;
      // There can be atmost one element eligible to be converted to
      // metadata only
      if (walkerCtx.getMayBeMetadataOnlyTableScans().isEmpty()) {
        return nd;
      }

      for (Node op : stack) {
        if (op instanceof GroupByOperator) {
          GroupByOperator gby = (GroupByOperator) op;
          if (!gby.getConf().isDistinctLike()) {
            // GroupBy not distinct like, disabling
            walkerCtx.convertNotMetadataOnly();
            return nd;
          }
        }
      }

      walkerCtx.convertMetadataOnly();
      return nd;
    }
  }

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%"),
      new TableScanProcessor());
    opRules.put(new RuleRegExp("R2",
      GroupByOperator.getOperatorName() + "%.*" + FileSinkOperator.getOperatorName() + "%"),
      new FileSinkProcessor());
    Dispatcher disp = new NullScanTaskDispatcher(pctx, opRules);
    GraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);
    return pctx;
  }
}
