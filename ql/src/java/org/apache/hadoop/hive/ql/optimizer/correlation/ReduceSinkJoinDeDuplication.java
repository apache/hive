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

package org.apache.hadoop.hive.ql.optimizer.correlation;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
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
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

/**
 * Optimization to check whether any ReduceSink operator in the plan can be
 * simplified so data is not shuffled/sorted if it is already shuffled/sorted.
 *
 * This optimization is executed after join algorithm selection logic has run,
 * and it is intended to optimize new cases that cannot be optimized when
 * {@link ReduceSinkDeDuplication} runs because some physical algorithms have
 * not been selected. Instead of removing ReduceSink operators from the plan,
 * they will be tagged, and then the execution plan compiler might take action,
 * e.g., on Tez, ReduceSink operators that just need to forward data will be
 * translated into a ONE-TO-ONE edge. The parallelism degree of these ReduceSink
 * operators might be adjusted, as a ReduceSink operator that just forwards data
 * cannot alter the degree of parallelism of the previous task.
 */
public class ReduceSinkJoinDeDuplication extends Transform {

  protected static final Logger LOG = LoggerFactory.getLogger(ReduceSinkJoinDeDuplication.class);

  protected ParseContext pGraphContext;

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    pGraphContext = pctx;

    ReduceSinkJoinDeDuplicateProcCtx cppCtx = new ReduceSinkJoinDeDuplicateProcCtx(pGraphContext);

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", ReduceSinkOperator.getOperatorName() + "%"),
        ReduceSinkJoinDeDuplicateProcFactory.getReducerMapJoinProc());

    Dispatcher disp = new DefaultRuleDispatcher(
        ReduceSinkJoinDeDuplicateProcFactory.getDefaultProc(), opRules, cppCtx);
    GraphWalker ogw = new ForwardWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pGraphContext;
  }

  protected class ReduceSinkJoinDeDuplicateProcCtx extends AbstractCorrelationProcCtx {

    public ReduceSinkJoinDeDuplicateProcCtx(ParseContext pctx) {
      super(pctx);
    }
  }

  static class ReduceSinkJoinDeDuplicateProcFactory {

    public static NodeProcessor getReducerMapJoinProc() {
      return new ReducerProc();
    }

    public static NodeProcessor getDefaultProc() {
      return new DefaultProc();
    }
  }

  /*
   * do nothing.
   */
  static class DefaultProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      return null;
    }
  }

  static class ReducerProc implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ReduceSinkJoinDeDuplicateProcCtx dedupCtx = (ReduceSinkJoinDeDuplicateProcCtx) procCtx;
      ReduceSinkOperator cRS = (ReduceSinkOperator) nd;
      if (cRS.getConf().isForwarding()) {
        // Already set
        return false;
      }
      if (cRS.getConf().getKeyCols().isEmpty()) {
        // Not supported
        return false;
      }
      boolean onlyPartitioning = false;
      Operator<?> cRSChild = cRS.getChildOperators().get(0);
      if (cRSChild instanceof MapJoinOperator ||
              cRSChild instanceof CommonMergeJoinOperator) {
        // If it is a MapJoin or MergeJoin, we make sure that they are on
        // the reduce side, otherwise we bail out
        for (Operator<?> parent: cRSChild.getParentOperators()) {
          if (!(parent instanceof ReduceSinkOperator)) {
            // MapJoin and SMBJoin not supported
            return false;
          }
        }
        if (cRSChild instanceof MapJoinOperator) {
          onlyPartitioning = true;
        }
      }

      int maxNumReducers = cRS.getConf().getNumReducers();
      ReduceSinkOperator pRS;
      if (onlyPartitioning) {
        pRS = CorrelationUtilities.findFirstPossibleParent(
            cRS, ReduceSinkOperator.class, dedupCtx.trustScript());
      } else {
        pRS = CorrelationUtilities.findFirstPossibleParentPreserveSortOrder(
            cRS, ReduceSinkOperator.class, dedupCtx.trustScript());
      }
      if (pRS != null) {
        Operator<?> pRSChild = pRS.getChildOperators().get(0);
        if (pRSChild instanceof MapJoinOperator) {
          // Handle MapJoin specially and check for all its children
          MapJoinOperator pRSChildMJ = (MapJoinOperator) pRSChild;
          // In this case, both should be DHJ operators as pRSChildMJ can only guarantee
          // partitioned input, not sorted.
          if (!pRSChildMJ.getConf().isDynamicPartitionHashJoin() ||
                  !(cRSChild instanceof MapJoinOperator) ||
                  !((MapJoinOperator) cRSChild).getConf().isDynamicPartitionHashJoin()) {
            return false;
          }
          ImmutableList.Builder<ReduceSinkOperator> l = ImmutableList.builder();
          for (Operator<?> parent: pRSChild.getParentOperators()) {
            ReduceSinkOperator rsOp = (ReduceSinkOperator) parent;
            l.add(rsOp);
            if (rsOp.getConf().getNumReducers() > maxNumReducers) {
              maxNumReducers = rsOp.getConf().getNumReducers();
            }
          }
          if (ReduceSinkDeDuplicationUtils.strictMerge(cRS, l.build())) {
            LOG.debug("Set {} to forward data", cRS);
            cRS.getConf().setForwarding(true);
            propagateMaxNumReducers(dedupCtx, cRS, maxNumReducers);
            return true;
          }
        } else if (pRS.getChildOperators().get(0) instanceof CommonMergeJoinOperator) {
          // Handle MergeJoin specially and check for all its children
          ImmutableList.Builder<ReduceSinkOperator> l = ImmutableList.builder();
          for (Operator<?> parent: pRSChild.getParentOperators()) {
            if (!(parent instanceof ReduceSinkOperator)) {
              // SMBJoin not supported
              return false;
            }
            ReduceSinkOperator rsOp = (ReduceSinkOperator) parent;
            l.add(rsOp);
            if (rsOp.getConf().getNumReducers() > maxNumReducers) {
              maxNumReducers = rsOp.getConf().getNumReducers();
            }
          }
          if (ReduceSinkDeDuplicationUtils.strictMerge(cRS, l.build())) {
            LOG.debug("Set {} to forward data", cRS);
            cRS.getConf().setForwarding(true);
            propagateMaxNumReducers(dedupCtx, cRS, maxNumReducers);
            return true;
          }
        } else {
          // Rest of cases
          if (pRS.getConf().getNumReducers() > maxNumReducers) {
            maxNumReducers = pRS.getConf().getNumReducers();
          }
          if (ReduceSinkDeDuplicationUtils.strictMerge(cRS, pRS)) {
            LOG.debug("Set {} to forward data", cRS);
            cRS.getConf().setForwarding(true);
            propagateMaxNumReducers(dedupCtx, cRS, maxNumReducers);
            return true;
          }
        }
      }
      return false;
    }

    private static void propagateMaxNumReducers(ReduceSinkJoinDeDuplicateProcCtx dedupCtx,
            ReduceSinkOperator rsOp, int maxNumReducers) throws SemanticException {
      if (rsOp == null) {
        // Bail out
        return;
      }
      if (rsOp.getChildOperators().get(0) instanceof MapJoinOperator ||
              rsOp.getChildOperators().get(0) instanceof CommonMergeJoinOperator) {
        for (Operator<?> p : rsOp.getChildOperators().get(0).getParentOperators()) {
          ReduceSinkOperator pRSOp = (ReduceSinkOperator) p;
          pRSOp.getConf().setReducerTraits(EnumSet.of(ReducerTraits.FIXED));
          pRSOp.getConf().setNumReducers(maxNumReducers);
          LOG.debug("Set {} to FIXED parallelism: {}", pRSOp, maxNumReducers);
          if (pRSOp.getConf().isForwarding()) {
            ReduceSinkOperator newRSOp =
                CorrelationUtilities.findFirstPossibleParent(
                    pRSOp, ReduceSinkOperator.class, dedupCtx.trustScript());
            propagateMaxNumReducers(dedupCtx, newRSOp, maxNumReducers);
          }
        }
      } else {
        rsOp.getConf().setReducerTraits(EnumSet.of(ReducerTraits.FIXED));
        rsOp.getConf().setNumReducers(maxNumReducers);
        LOG.debug("Set {} to FIXED parallelism: {}", rsOp, maxNumReducers);
        if (rsOp.getConf().isForwarding()) {
          ReduceSinkOperator newRSOp =
              CorrelationUtilities.findFirstPossibleParent(
                  rsOp, ReduceSinkOperator.class, dedupCtx.trustScript());
          propagateMaxNumReducers(dedupCtx, newRSOp, maxNumReducers);
        }
      }
    }
  }
}
