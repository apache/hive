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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVECONVERTJOIN;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASK;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * If two reducer sink operators share the same partition/sort columns and order,
 * they can be merged. This should happen after map join optimization because map
 * join optimization will remove reduce sink operators.
 *
 * This optimizer removes/replaces child-RS (not parent) which is safer way for DefaultGraphWalker.
 */
public class ReduceSinkDeDuplication extends Transform {

  protected static final Logger LOG = LoggerFactory.getLogger(ReduceSinkDeDuplication.class);

  private static final String RS = ReduceSinkOperator.getOperatorName();
  private static final String GBY = GroupByOperator.getOperatorName();
  private static final String JOIN = JoinOperator.getOperatorName();

  protected ParseContext pGraphContext;

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    pGraphContext = pctx;

    // generate pruned column list for all relevant operators
    ReduceSinkDeduplicateProcCtx cppCtx = new ReduceSinkDeduplicateProcCtx(pGraphContext);

    // for auto convert map-joins, it not safe to dedup in here (todo)
    boolean mergeJoins = !pctx.getConf().getBoolVar(HIVECONVERTJOIN) &&
        !pctx.getConf().getBoolVar(HIVECONVERTJOINNOCONDITIONALTASK) &&
        !pctx.getConf().getBoolVar(ConfVars.HIVE_CONVERT_JOIN_BUCKET_MAPJOIN_TEZ) &&
        !pctx.getConf().getBoolVar(ConfVars.HIVEDYNAMICPARTITIONHASHJOIN);

    // If multiple rules can be matched with same cost, last rule will be choosen as a processor
    // see DefaultRuleDispatcher#dispatch()
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(new RuleRegExp("R1", RS + "%.*%" + RS + "%"),
        ReduceSinkDeduplicateProcFactory.getReducerReducerProc());
    opRules.put(new RuleRegExp("R2", RS + "%" + GBY + "%.*%" + RS + "%"),
        ReduceSinkDeduplicateProcFactory.getGroupbyReducerProc());
    if (mergeJoins) {
      opRules.put(new RuleRegExp("R3", JOIN + "%.*%" + RS + "%"),
          ReduceSinkDeduplicateProcFactory.getJoinReducerProc());
    }
    // TODO RS+JOIN

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SemanticDispatcher disp = new DefaultRuleDispatcher(ReduceSinkDeduplicateProcFactory
        .getDefaultProc(), opRules, cppCtx);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pGraphContext;
  }

  protected class ReduceSinkDeduplicateProcCtx extends AbstractCorrelationProcCtx {

    public ReduceSinkDeduplicateProcCtx(ParseContext pctx) {
      super(pctx);
    }
  }

  static class ReduceSinkDeduplicateProcFactory {

    public static SemanticNodeProcessor getReducerReducerProc() {
      return new ReducerReducerProc();
    }

    public static SemanticNodeProcessor getGroupbyReducerProc() {
      return new GroupbyReducerProc();
    }

    public static SemanticNodeProcessor getJoinReducerProc() {
      return new JoinReducerProc();
    }

    public static SemanticNodeProcessor getDefaultProc() {
      return new DefaultProc();
    }
  }

  /*
   * do nothing.
   */
  static class DefaultProc implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      return null;
    }
  }

  public abstract static class AbsctractReducerReducerProc implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ReduceSinkDeduplicateProcCtx dedupCtx = (ReduceSinkDeduplicateProcCtx) procCtx;
      if (dedupCtx.hasBeenRemoved((Operator<?>) nd)) {
        return false;
      }
      ReduceSinkOperator cRS = (ReduceSinkOperator) nd;
      Operator<?> child = CorrelationUtilities.getSingleChild(cRS);
      if (child instanceof JoinOperator) {
        return false; // not supported
      }
      if (child instanceof GroupByOperator) {
        GroupByOperator cGBY = (GroupByOperator) child;
        if (!CorrelationUtilities.hasGroupingSet(cRS) && !cGBY.getConf().isGroupingSetsPresent()) {
          return process(cRS, cGBY, dedupCtx);
        }
        return false;
      }
      if (child instanceof SelectOperator) {
        return process(cRS, dedupCtx);
      }
      return false;
    }

    protected abstract Object process(ReduceSinkOperator cRS, ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException;

    protected abstract Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx) throws SemanticException;
  }

  static class GroupbyReducerProc extends AbsctractReducerReducerProc {

    // given a group by operator this determines if that group by belongs to semi-join branch
    // note that this works only for second last group by in semi-join branch (X-GB-RS-GB-RS)
    private boolean isSemiJoinBranch(final GroupByOperator gOp, ReduceSinkDeduplicateProcCtx dedupCtx) {
      for(int i=0; i<gOp.getChildren().size(); i++) {
        if(gOp.getChildren().get(i) instanceof  ReduceSinkOperator) {
          ReduceSinkOperator rsOp = (ReduceSinkOperator)gOp.getChildren().get(i);
          if(dedupCtx.getPctx().getRsToSemiJoinBranchInfo().containsKey(rsOp)) {
            return true;
          }
        }
      }
      return false;
    }

    // pRS-pGBY-cRS
    @Override
    public Object process(ReduceSinkOperator cRS, ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      GroupByOperator pGBY =
          CorrelationUtilities.findPossibleParent(
              cRS, GroupByOperator.class, dedupCtx.trustScript());
      if (pGBY == null) {
        return false;
      }
      if(isSemiJoinBranch(pGBY, dedupCtx)) {
        return false;
      }
      ReduceSinkOperator pRS =
          CorrelationUtilities.findPossibleParent(
              pGBY, ReduceSinkOperator.class, dedupCtx.trustScript());
      if (pRS != null && ReduceSinkDeDuplicationUtils.merge(cRS, pRS, dedupCtx.minReducer())) {
        CorrelationUtilities.replaceReduceSinkWithSelectOperator(
            cRS, dedupCtx.getPctx(), dedupCtx);
        pRS.getConf().setDeduplicated(true);
        return true;
      }
      return false;
    }

    // pRS-pGBY-cRS-cGBY
    @Override
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      Operator<?> start = CorrelationUtilities.getStartForGroupBy(cRS, dedupCtx);
      GroupByOperator pGBY =
          CorrelationUtilities.findPossibleParent(
              start, GroupByOperator.class, dedupCtx.trustScript());
      if (pGBY == null) {
        return false;
      }
      if(isSemiJoinBranch(cGBY, dedupCtx)) {
        return false;
      }
      ReduceSinkOperator pRS =
          CorrelationUtilities.getSingleParent(pGBY, ReduceSinkOperator.class);
      if (pRS != null && ReduceSinkDeDuplicationUtils.merge(cRS, pRS, dedupCtx.minReducer())) {
        CorrelationUtilities.removeReduceSinkForGroupBy(
            cRS, cGBY, dedupCtx.getPctx(), dedupCtx);
        pRS.getConf().setDeduplicated(true);
        return true;
      }
      return false;
    }
  }

  static class JoinReducerProc extends AbsctractReducerReducerProc {

    // pRS-pJOIN-cRS
    @Override
    public Object process(ReduceSinkOperator cRS, ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      JoinOperator pJoin =
          CorrelationUtilities.findPossibleParent(cRS, JoinOperator.class, dedupCtx.trustScript());
      if (pJoin != null && ReduceSinkDeDuplicationUtils.merge(cRS, pJoin, dedupCtx.minReducer())) {
        pJoin.getConf().setFixedAsSorted(true);
        CorrelationUtilities.replaceReduceSinkWithSelectOperator(
            cRS, dedupCtx.getPctx(), dedupCtx);
        ReduceSinkOperator pRS =
            CorrelationUtilities.findPossibleParent(
                pJoin, ReduceSinkOperator.class, dedupCtx.trustScript());
        if (pRS != null) {
          pRS.getConf().setDeduplicated(true);
        }
        return true;
      }
      return false;
    }

    // pRS-pJOIN-cRS-cGBY
    @Override
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      Operator<?> start = CorrelationUtilities.getStartForGroupBy(cRS, dedupCtx);
      JoinOperator pJoin =
          CorrelationUtilities.findPossibleParent(
              start, JoinOperator.class, dedupCtx.trustScript());
      if (pJoin != null && ReduceSinkDeDuplicationUtils.merge(cRS, pJoin, dedupCtx.minReducer())) {
        pJoin.getConf().setFixedAsSorted(true);
        CorrelationUtilities.removeReduceSinkForGroupBy(
            cRS, cGBY, dedupCtx.getPctx(), dedupCtx);
        ReduceSinkOperator pRS =
            CorrelationUtilities.findPossibleParent(
                pJoin, ReduceSinkOperator.class, dedupCtx.trustScript());
        if (pRS != null) {
          pRS.getConf().setDeduplicated(true);
        }
        return true;
      }
      return false;
    }
  }

  static class ReducerReducerProc extends AbsctractReducerReducerProc {

    // pRS-cRS
    @Override
    public Object process(ReduceSinkOperator cRS, ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      ReduceSinkOperator pRS =
          CorrelationUtilities.findPossibleParent(
              cRS, ReduceSinkOperator.class, dedupCtx.trustScript());
      if (pRS != null) {
        // Try extended deduplication
        if (ReduceSinkDeDuplicationUtils.aggressiveDedup(cRS, pRS, dedupCtx)) {
          return true;
        }
        // Normal deduplication
        if (ReduceSinkDeDuplicationUtils.merge(cRS, pRS, dedupCtx.minReducer())) {
          CorrelationUtilities.replaceReduceSinkWithSelectOperator(
              cRS, dedupCtx.getPctx(), dedupCtx);
          pRS.getConf().setDeduplicated(true);
          return true;
        }
      }
      return false;
    }

    // pRS-cRS-cGBY
    @Override
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      Operator<?> start = CorrelationUtilities.getStartForGroupBy(cRS, dedupCtx);
      ReduceSinkOperator pRS =
          CorrelationUtilities.findPossibleParent(
              start, ReduceSinkOperator.class, dedupCtx.trustScript());
      if (pRS != null && ReduceSinkDeDuplicationUtils.merge(cRS, pRS, dedupCtx.minReducer())) {
        if (dedupCtx.getPctx().getConf().getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)) {
          return false;
        }
        CorrelationUtilities.removeReduceSinkForGroupBy(cRS, cGBY, dedupCtx.getPctx(), dedupCtx);
        pRS.getConf().setDeduplicated(true);
        return true;
      }
      return false;
    }
  }
}
