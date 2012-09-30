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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBExpr;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

/**
 * Implementation of correlation optimizer. The optimization is based on
 * the paper "YSmart: Yet Another SQL-to-MapReduce Translator"
 * (Rubao Lee, Tian Luo, Yin Huai, Fusheng Wang, Yongqiang He, and Xiaodong Zhang)
 * (http://www.cse.ohio-state.edu/hpcs/WWW/HTML/publications/papers/TR-11-7.pdf).
 * This optimizer first detects three kinds of
 * correlations, Input Correlation (IC), Transit Correlation (TC) and Job Flow Correlation (JFC),
 * and then merge correlated MapReduce-jobs (MR-jobs) into one MR-job.
 * Since opColumnExprMap, opParseCtx, opRowResolver may be changed by
 * other optimizers,
 * currently, correlation optimizer has two phases. The first phase is the first transformation in
 * the Optimizer. In the first phase, original opColumnExprMap, opParseCtx, opRowResolver
 * will be recorded. Then, the second phase (the last transformation before SimpleFetchOptimizer)
 * will perform correlation detection and query plan tree transformation.
 *
 * For the definitions of correlations, see the original paper of YSmart.
 *
 * Rules for merging correlated MR-jobs implemented in this correlation
 * optimizer are:
 * 1. If an MR-job for a Join operation has the same partitioning keys with its all
 * preceding MR-jobs, correlation optimizer merges these MR-jobs into one MR-job.
 * 2. If an MR-job for a GroupBy and Aggregation operation has the same partitioning keys
 * with its preceding MR-job, correlation optimizer merges these two MR-jobs into one MR-job.
 *
 * Note: In the current implementation, if correlation optimizer detects MR-jobs of a sub-plan tree
 * are correlated, it transforms this sub-plan tree to a single MR-job when the input of this
 * sub-plan tree is not a temporary table. Otherwise, the current implementation will ignore this
 * sub-plan tree.
 *
 * There are several future work that will enhance the correlation optimizer.
 * Here are four examples:
 * 1. Add a new rule that is if two MR-jobs share the same
 * partitioning keys and they have common input tables, merge these two MR-jobs into a single
 * MR-job.
 * 2. The current implementation detects MR-jobs which have the same partitioning keys
 * as correlated MR-jobs. However, the condition of same partitioning keys can be relaxed to use
 * common partitioning keys.
 * 3. The current implementation cannot optimize MR-jobs for the
 * aggregation functions with a distinct keyword, which should be supported in the future
 * implementation of the correlation optimizer.
 * 4. Optimize queries involve self-join.
 */

public class CorrelationOptimizer implements Transform {

  static final private Log LOG = LogFactory.getLog(CorrelationOptimizer.class.getName());
  private final Map<String, String> aliastoTabName;
  private final Map<String, Table> aliastoTab;

  public CorrelationOptimizer() {
    super();
    aliastoTabName = new HashMap<String, String>();
    aliastoTab = new HashMap<String, Table>();
    pGraphContext = null;
  }

  private boolean initializeAliastoTabNameMapping(QB qb) {
    // If any sub-query's qb is null, CorrelationOptimizer will not optimize this query.
    // e.g. auto_join27.q
    if (qb == null) {
      return false;
    }
    boolean ret = true;
    for (String alias : qb.getAliases()) {
      aliastoTabName.put(alias, qb.getTabNameForAlias(alias));
      aliastoTab.put(alias, qb.getMetaData().getSrcForAlias(alias));
    }
    for (String subqalias : qb.getSubqAliases()) {
      QBExpr qbexpr = qb.getSubqForAlias(subqalias);
      ret = ret && initializeAliastoTabNameMapping(qbexpr.getQB());
    }
    return ret;
  }

  protected ParseContext pGraphContext;
  private Map<Operator<? extends OperatorDesc>, OpParseContext> opParseCtx;
  private final Map<Operator<? extends OperatorDesc>, OpParseContext> originalOpParseCtx =
      new LinkedHashMap<Operator<? extends OperatorDesc>, OpParseContext>();
  private final Map<Operator<? extends OperatorDesc>, RowResolver> originalOpRowResolver =
      new LinkedHashMap<Operator<? extends OperatorDesc>, RowResolver>();
  private final Map<Operator<? extends OperatorDesc>, Map<String, ExprNodeDesc>> originalOpColumnExprMap =
      new LinkedHashMap<Operator<? extends OperatorDesc>, Map<String, ExprNodeDesc>>();

  private boolean isPhase1 = true;
  private boolean abort = false;

  private Map<ReduceSinkOperator, GroupByOperator> groupbyNonMapSide2MapSide;
  private Map<GroupByOperator, ReduceSinkOperator> groupbyMapSide2NonMapSide;

  /**
   * Transform the query tree. Firstly, find out correlations between operations.
   * Then, group these operators in groups
   *
   * @param pactx
   *          current parse context
   * @throws SemanticException
   */
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    if (isPhase1) {
      pGraphContext = pctx;
      opParseCtx = pctx.getOpParseCtx();

      CorrelationNodePhase1ProcCtx phase1ProcCtx = new CorrelationNodePhase1ProcCtx();
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      Dispatcher disp = new DefaultRuleDispatcher(getPhase1DefaultProc(), opRules,
          phase1ProcCtx);
      GraphWalker ogw = new DefaultGraphWalker(disp);

      // Create a list of topOp nodes
      List<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(pGraphContext.getTopOps().values());
      ogw.startWalking(topNodes, null);
      isPhase1 = false;
      abort = phase1ProcCtx.fileSinkOperatorCount > 1;
    } else {
      /*
       * Types of correlations:
       * 1) Input Correlation: Multiple nodes have input correlation
       * (IC) if their input relation sets are not disjoint;
       * 2) Transit Correlation: Multiple nodes have transit correlation
       * (TC) if they have not only input correlation, but
       * also the same partition key;
       * 3) Job Flow Correlation: A node has job flow correlation
       * (JFC) with one of its child nodes if it has the same
       * partition key as that child node.
       */

      pGraphContext = pctx;
      if (abort) {
        //TODO: handle queries with multiple FileSinkOperators;
        LOG.info("Abort. Reasons are ...");
        LOG.info("-- Currently, a query with multiple FileSinkOperators are not supported.");
        return pGraphContext;
      }


      opParseCtx = pctx.getOpParseCtx();

      groupbyNonMapSide2MapSide = pctx.getGroupbyNonMapSide2MapSide();
      groupbyMapSide2NonMapSide = pctx.getGroupbyMapSide2NonMapSide();

      QB qb = pGraphContext.getQB();
      abort = !initializeAliastoTabNameMapping(qb);
      if (abort) {
        LOG.info("Abort. Reasons are ...");
        LOG.info("-- This query or its sub-queries has a null qb.");
        return pGraphContext;
      }

      // 0: Replace all map-side group by pattern (GBY-RS-GBY) to
      // non-map-side group by pattern (RS-GBY) if necessary
      if (pGraphContext.getConf().getBoolVar(HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE)) {
        for (Entry<GroupByOperator, ReduceSinkOperator> entry:
          groupbyMapSide2NonMapSide.entrySet()) {
          GroupByOperator mapSidePatternStart = entry.getKey();
          GroupByOperator mapSidePatternEnd = (GroupByOperator) mapSidePatternStart
              .getChildOperators().get(0).getChildOperators().get(0);
          ReduceSinkOperator nonMapSidePatternStart = entry.getValue();
          GroupByOperator nonMapSidePatternEnd = (GroupByOperator) nonMapSidePatternStart
              .getChildOperators().get(0);

          List<Operator<? extends OperatorDesc>> parents = mapSidePatternStart.getParentOperators();
          List<Operator<? extends OperatorDesc>> children = mapSidePatternEnd.getChildOperators();

          nonMapSidePatternStart.setParentOperators(parents);
          nonMapSidePatternEnd.setChildOperators(children);

          for (Operator<? extends OperatorDesc> parent: parents) {
            parent.replaceChild(mapSidePatternStart, nonMapSidePatternStart);
          }
          for (Operator<? extends OperatorDesc> child: children) {
            child.replaceParent(mapSidePatternEnd, nonMapSidePatternEnd);
          }
          addOperatorInfo(nonMapSidePatternStart);
          addOperatorInfo(nonMapSidePatternEnd);
        }
      }

      // 1: detect correlations
      CorrelationNodeProcCtx correlationCtx = new CorrelationNodeProcCtx();

      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      opRules.put(new RuleRegExp("R1", ReduceSinkOperator.getOperatorName() + "%"),
          new CorrelationNodeProc());

      Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, correlationCtx);
      GraphWalker ogw = new DefaultGraphWalker(disp);

      // Create a list of topOp nodes
      List<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(pGraphContext.getTopOps().values());
      ogw.startWalking(topNodes, null);
      abort = correlationCtx.isAbort();
      int correlationsAppliedCount = 0;
      if (abort) {
        LOG.info("Abort. Reasons are ...");
        for (String reason: correlationCtx.getAbortReasons()) {
          LOG.info("-- " + reason);
        }
      } else {
        // 2: transform the query plan tree
        LOG.info("Begain query plan transformation based on intra-query correlations. " +
            correlationCtx.getCorrelations().size() + " correlation(s) to be applied");
        for (IntraQueryCorrelation correlation : correlationCtx.getCorrelations()) {
          boolean ret = CorrelationOptimizerUtils.applyCorrelation(
              correlation, pGraphContext, originalOpColumnExprMap, originalOpRowResolver,
              groupbyNonMapSide2MapSide, originalOpParseCtx);
          if (ret) {
            correlationsAppliedCount++;
          }
        }
      }

      // 3: if no correlation applied, replace all non-map-side group by pattern (GBY-RS-GBY) to
      // map-side group by pattern (RS-GBY) if necessary
      if (correlationsAppliedCount == 0 &&
          pGraphContext.getConf().getBoolVar(HiveConf.ConfVars.HIVEMAPSIDEAGGREGATE)) {
        for (Entry<ReduceSinkOperator, GroupByOperator> entry:
          groupbyNonMapSide2MapSide.entrySet()) {
          GroupByOperator mapSidePatternStart = entry.getValue();
          GroupByOperator mapSidePatternEnd = (GroupByOperator) mapSidePatternStart
              .getChildOperators().get(0).getChildOperators().get(0);
          ReduceSinkOperator nonMapSidePatternStart = entry.getKey();
          GroupByOperator nonMapSidePatternEnd = (GroupByOperator) nonMapSidePatternStart
              .getChildOperators().get(0);

          List<Operator<? extends OperatorDesc>> parents = nonMapSidePatternStart.getParentOperators();
          List<Operator<? extends OperatorDesc>> children = nonMapSidePatternEnd.getChildOperators();

          mapSidePatternStart.setParentOperators(parents);
          mapSidePatternEnd.setChildOperators(children);

          for (Operator<? extends OperatorDesc> parent: parents) {
            parent.replaceChild(nonMapSidePatternStart, mapSidePatternStart);
          }
          for (Operator<? extends OperatorDesc> child: children) {
            child.replaceParent(nonMapSidePatternEnd, mapSidePatternEnd);
          }
        }
      }
      LOG.info("Finish query plan transformation based on intra-query correlations. " +
          correlationsAppliedCount + " correlation(s) actually be applied");
    }
    return pGraphContext;
  }

  private void addOperatorInfo(Operator<? extends OperatorDesc> op) {
    OpParseContext opCtx = opParseCtx.get(op);
    if (op.getColumnExprMap() != null) {
      if (!originalOpColumnExprMap.containsKey(op)) {
        originalOpColumnExprMap.put(op, op.getColumnExprMap());
      }
    }
    if (opCtx != null) {
      if (!originalOpParseCtx.containsKey(op)) {
        originalOpParseCtx.put(op, opCtx);
      }
      if (opCtx.getRowResolver() != null) {
        if (!originalOpRowResolver.containsKey(op)) {
          originalOpRowResolver.put(op, opCtx.getRowResolver());
        }
      }
    }
  }

  private NodeProcessor getPhase1DefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
        addOperatorInfo(op);

        if (op.getName().equals(FileSinkOperator.getOperatorName())) {
          ((CorrelationNodePhase1ProcCtx)procCtx).fileSinkOperatorCount++;
        }
        return null;
      }
    };
  }

  private class CorrelationNodeProc implements NodeProcessor {

    public ReduceSinkOperator findNextChildReduceSinkOperator(ReduceSinkOperator rsop) {
      Operator<? extends OperatorDesc> op = rsop.getChildOperators().get(0);
      while (!op.getName().equals(ReduceSinkOperator.getOperatorName())) {
        if (op.getName().equals(FileSinkOperator.getOperatorName())) {
          return null;
        }
        assert op.getChildOperators().size() <= 1;
        op = op.getChildOperators().get(0);
      }
      return (ReduceSinkOperator) op;
    }

    private void analyzeReduceSinkOperatorsOfJoinOperator(JoinCondDesc[] joinConds,
        List<Operator<? extends OperatorDesc>> rsOps, Operator<? extends OperatorDesc> curentRsOps,
        Set<ReduceSinkOperator> correlatedRsOps) {
      if (correlatedRsOps.contains((ReduceSinkOperator)curentRsOps)) {
        return;
      }

      correlatedRsOps.add((ReduceSinkOperator)curentRsOps);

      int pos = rsOps.indexOf(curentRsOps);
      for (int i=0; i<joinConds.length; i++) {
        JoinCondDesc joinCond = joinConds[i];
        int type = joinCond.getType();
        if (pos == joinCond.getLeft()) {
          if (type == JoinDesc.INNER_JOIN || type == JoinDesc.LEFT_OUTER_JOIN) {
            Operator<? extends OperatorDesc> newCurrentRsOps = rsOps.get(joinCond.getRight());
            analyzeReduceSinkOperatorsOfJoinOperator(joinConds, rsOps, newCurrentRsOps,
                correlatedRsOps);
          }
        } else if (pos == joinCond.getRight()) {
          if (type == JoinDesc.INNER_JOIN || type == JoinDesc.RIGHT_OUTER_JOIN) {
            Operator<? extends OperatorDesc> newCurrentRsOps = rsOps.get(joinCond.getLeft());
            analyzeReduceSinkOperatorsOfJoinOperator(joinConds, rsOps, newCurrentRsOps,
                correlatedRsOps);
          }
        }
      }
    }

    private Set<ReduceSinkOperator> findCorrelatedReduceSinkOperators(
        Operator<? extends OperatorDesc> op, Set<String> keyColumns,
        IntraQueryCorrelation correlation) throws SemanticException {

      LOG.info("now detecting operator " + op.getIdentifier() + " " + op.getName());

      Set<ReduceSinkOperator> correlatedReduceSinkOps = new HashSet<ReduceSinkOperator>();
      if (op.getParentOperators() == null) {
        return correlatedReduceSinkOps;
      }
      if (originalOpColumnExprMap.get(op) == null && !(op instanceof ReduceSinkOperator)) {
        for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
          correlatedReduceSinkOps.addAll(findCorrelatedReduceSinkOperators(
              parent, keyColumns, correlation));
        }
      } else if (originalOpColumnExprMap.get(op) != null && !(op instanceof ReduceSinkOperator)) {
        Set<String> newKeyColumns = new HashSet<String>();
        for (String keyColumn : keyColumns) {
          ExprNodeDesc col = originalOpColumnExprMap.get(op).get(keyColumn);
          if (col instanceof ExprNodeColumnDesc) {
            newKeyColumns.add(((ExprNodeColumnDesc) col).getColumn());
          }
        }

        if (op.getName().equals(CommonJoinOperator.getOperatorName())) {
          Set<String> tableNeedToCheck = new HashSet<String>();
          for (String keyColumn : keyColumns) {
            for (ColumnInfo cinfo : originalOpParseCtx.get(op).getRowResolver().getColumnInfos()) {
              if (keyColumn.equals(cinfo.getInternalName())) {
                tableNeedToCheck.add(cinfo.getTabAlias());
              }
            }
          }
          Set<ReduceSinkOperator> correlatedRsOps = new HashSet<ReduceSinkOperator>();
          for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
            Set<String> tableNames =
                originalOpParseCtx.get(parent).getRowResolver().getTableNames();
            for (String tbl : tableNames) {
              if (tableNeedToCheck.contains(tbl)) {
                correlatedRsOps.addAll(findCorrelatedReduceSinkOperators(parent,
                    newKeyColumns, correlation));
              }
            }
          }

          // Right now, if any ReduceSinkOperator of this JoinOperator is not correlated, we will
          // not optimize this query
          if (correlatedRsOps.size() == op.getParentOperators().size()) {
            correlatedReduceSinkOps.addAll(correlatedRsOps);
          } else {
            correlatedReduceSinkOps.clear();
          }
        } else {
          for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
            correlatedReduceSinkOps.addAll(findCorrelatedReduceSinkOperators(
                parent, newKeyColumns, correlation));
          }
        }
      } else if (originalOpColumnExprMap.get(op) != null && op instanceof ReduceSinkOperator) {
        Set<String> newKeyColumns = new HashSet<String>();
        for (String keyColumn : keyColumns) {
          ExprNodeDesc col = originalOpColumnExprMap.get(op).get(keyColumn);
          if (col instanceof ExprNodeColumnDesc) {
            newKeyColumns.add(((ExprNodeColumnDesc) col).getColumn());
          }
        }

        ReduceSinkOperator rsop = (ReduceSinkOperator) op;
        Set<String> thisKeyColumns = new HashSet<String>();
        for (ExprNodeDesc key : rsop.getConf().getKeyCols()) {
          if (key instanceof ExprNodeColumnDesc) {
            thisKeyColumns.add(((ExprNodeColumnDesc) key).getColumn());
          }
        }

        boolean isCorrelated = false;
        Set<String> intersection = new HashSet<String>(newKeyColumns);
        intersection.retainAll(thisKeyColumns);
        // TODO: should use if intersection is empty to evaluate if two corresponding operators are
        // correlated
        isCorrelated = (intersection.size() == thisKeyColumns.size() && !intersection.isEmpty());

        ReduceSinkOperator nextChildReduceSinkOperator = findNextChildReduceSinkOperator(rsop);
        // Since we start the search from those reduceSinkOperator at bottom (near FileSinkOperator),
        // we can always find a reduceSinkOperator at a lower level
        assert nextChildReduceSinkOperator != null;
        if (isCorrelated) {
          if (nextChildReduceSinkOperator.getChildOperators().get(0).getName().equals(
              CommonJoinOperator.getOperatorName())) {
            if (intersection.size() != nextChildReduceSinkOperator.getConf().getKeyCols().size() ||
                intersection.size() != rsop.getConf().getKeyCols().size()) {
              // Right now, we can only handle identical join keys.
              isCorrelated = false;
            }
          }
        }

        if (isCorrelated) {
          LOG.info("Operator " + op.getIdentifier() + " " + op.getName() + " is correlated");
          LOG.info("--keys of this operator: " + thisKeyColumns.toString());
          LOG.info("--keys of child operator: " + keyColumns.toString());
          LOG.info("--keys of child operator mapped to this operator:" + newKeyColumns.toString());
          if (((Operator<? extends OperatorDesc>) (op.getChildOperators().get(0))).getName()
              .equals(CommonJoinOperator.getOperatorName())) {
            JoinOperator joinOp = (JoinOperator)op.getChildOperators().get(0);
            JoinCondDesc[] joinConds = joinOp.getConf().getConds();
            List<Operator<? extends OperatorDesc>> rsOps = joinOp.getParentOperators();
            Set<ReduceSinkOperator> correlatedRsOps = new HashSet<ReduceSinkOperator>();
            analyzeReduceSinkOperatorsOfJoinOperator(joinConds, rsOps, op, correlatedRsOps);
            correlatedReduceSinkOps.addAll(correlatedRsOps);
          } else {
            correlatedReduceSinkOps.add(rsop);
          }
          // this if block is useful when we use "isCorrelated = !(intersection.isEmpty());" for
          // the evaluation of isCorrelated
          if (nextChildReduceSinkOperator.getChildOperators().get(0).getName().equals(
              GroupByOperator.getOperatorName()) &&
              (intersection.size() < nextChildReduceSinkOperator.getConf().getKeyCols().size())) {
            LOG.info("--found a RS-GBY pattern that needs to be replaced to GBY-RS-GBY patterns. "
                + " The number of common keys is "
                + intersection.size()
                + ", and the number of keys of next group by operator"
                + nextChildReduceSinkOperator.getConf().getKeyCols().size());
            correlation.addToRSGBYToBeReplacedByGBYRSGBY(nextChildReduceSinkOperator);
          }
        } else {
          LOG.info("Operator " + op.getIdentifier() + " " + op.getName() + " is not correlated");
          LOG.info("--keys of this operator: " + thisKeyColumns.toString());
          LOG.info("--keys of child operator: " + keyColumns.toString());
          LOG.info("--keys of child operator mapped to this operator:" + newKeyColumns.toString());
          correlatedReduceSinkOps.clear();
          correlation.getRSGBYToBeReplacedByGBYRSGBY().clear();
        }
      } else {
        LOG.error("ReduceSinkOperator " + op.getIdentifier() + " does not have ColumnExprMap");
        throw new SemanticException("CorrelationOptimizer cannot optimize this plan. " +
            "ReduceSinkOperator " + op.getIdentifier()
            + " does not have ColumnExprMap");
      }
      return correlatedReduceSinkOps;
    }

    private Set<ReduceSinkOperator> exploitJFC(ReduceSinkOperator op,
      CorrelationNodeProcCtx correlationCtx, IntraQueryCorrelation correlation)
      throws SemanticException {

      correlationCtx.addWalked(op);
      correlation.addToAllReduceSinkOperators(op);

      Set<ReduceSinkOperator> reduceSinkOperators = new HashSet<ReduceSinkOperator>();

      boolean shouldDetect = true;

      List<ExprNodeDesc> keys = op.getConf().getKeyCols();
      Set<String> keyColumns = new HashSet<String>();
      for (ExprNodeDesc key : keys) {
        if (!(key instanceof ExprNodeColumnDesc)) {
          shouldDetect = false;
        } else {
          keyColumns.add(((ExprNodeColumnDesc) key).getColumn());
        }
      }

      if (shouldDetect) {
        Set<ReduceSinkOperator> newReduceSinkOperators = new HashSet<ReduceSinkOperator>();
        for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
          LOG.info("Operator " + op.getIdentifier()
              + ": start detecting correlation from this operator");
          LOG.info("--keys of this operator: " + keyColumns.toString());
          Set<ReduceSinkOperator> correlatedReduceSinkOperators =
              findCorrelatedReduceSinkOperators(parent, keyColumns, correlation);
          if (correlatedReduceSinkOperators.size() == 0) {
            newReduceSinkOperators.add(op);
          } else {
            for (ReduceSinkOperator rsop : correlatedReduceSinkOperators) {

              // For two ReduceSinkOperators, we say the one closer to FileSinkOperators is up and
              // another one is down

              if (!correlation.getUp2downRSops().containsKey(op)) {
                correlation.getUp2downRSops().put(op, new ArrayList<ReduceSinkOperator>());
              }
              correlation.getUp2downRSops().get(op).add(rsop);

              if (!correlation.getDown2upRSops().containsKey(rsop)) {
                correlation.getDown2upRSops().put(rsop, new ArrayList<ReduceSinkOperator>());
              }
              correlation.getDown2upRSops().get(rsop).add(op);
              Set<ReduceSinkOperator> exploited = exploitJFC(rsop, correlationCtx,
                  correlation);
              if (exploited.size() == 0) {
                newReduceSinkOperators.add(rsop);
              } else {
                newReduceSinkOperators.addAll(exploited);
              }
            }
          }
        }
        reduceSinkOperators.addAll(newReduceSinkOperators);
      }
      return reduceSinkOperators;
    }

    private TableScanOperator findTableScanOPerator(Operator<? extends OperatorDesc> startPoint) {
      Operator<? extends OperatorDesc> thisOp = startPoint.getParentOperators().get(0);
      while (true) {
        if (thisOp.getName().equals(ReduceSinkOperator.getOperatorName())) {
          return null;
        } else if (thisOp.getName().equals(TableScanOperator.getOperatorName())) {
          return (TableScanOperator) thisOp;
        } else {
          if (thisOp.getParentOperators() != null) {
            thisOp = thisOp.getParentOperators().get(0);
          } else {
            break;
          }
        }
      }
      return null;
    }

    private void annotateOpPlan(IntraQueryCorrelation correlation) {
      Map<ReduceSinkOperator, Integer> bottomReduceSink2OperationPath =
          new HashMap<ReduceSinkOperator, Integer>();
      int indx = 0;
      for (ReduceSinkOperator rsop : correlation.getBottomReduceSinkOperators()) {
        if (!bottomReduceSink2OperationPath.containsKey(rsop)) {
          bottomReduceSink2OperationPath.put(rsop, indx);
          for (ReduceSinkOperator peerRSop : CorrelationOptimizerUtils
              .findPeerReduceSinkOperators(rsop)) {
            if (correlation.getBottomReduceSinkOperators().contains(peerRSop)) {
              bottomReduceSink2OperationPath.put(peerRSop, indx);
            }
          }
          indx++;
        }
      }
      correlation.setBottomReduceSink2OperationPathMap(bottomReduceSink2OperationPath);
    }

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {

      CorrelationNodeProcCtx correlationCtx = (CorrelationNodeProcCtx) ctx;

      ReduceSinkOperator op = (ReduceSinkOperator) nd;

      if (correlationCtx.isWalked(op)) {
        return null;
      }

      LOG.info("Walk to operator " + ((Operator) nd).getIdentifier() + " "
          + ((Operator) nd).getName());
      addOperatorInfo((Operator<? extends OperatorDesc>) nd);

      if (op.getConf().getKeyCols().size() == 0 ||
          (!op.getChildOperators().get(0).getName().equals(CommonJoinOperator.getOperatorName()) &&
              !op.getChildOperators().get(0).getName().equals(GroupByOperator.getOperatorName()))) {
        correlationCtx.addWalked(op);
        return null;
      }

      // 1: find out correlation
      IntraQueryCorrelation correlation = new IntraQueryCorrelation();
      List<ReduceSinkOperator> peerReduceSinkOperators =
          CorrelationOptimizerUtils.findPeerReduceSinkOperators(op);
      List<ReduceSinkOperator> bottomReduceSinkOperators = new ArrayList<ReduceSinkOperator>();
      for (ReduceSinkOperator rsop : peerReduceSinkOperators) {
        Set<ReduceSinkOperator> thisBottomReduceSinkOperators = exploitJFC(rsop,
            correlationCtx, correlation);
        if (thisBottomReduceSinkOperators.size() == 0) {
          thisBottomReduceSinkOperators.add(rsop);
        } else {
          boolean isClear = false;
          // bottom ReduceSinkOperators are those ReduceSinkOperators which are close to
          // TableScanOperators
          for (ReduceSinkOperator bottomRsop : thisBottomReduceSinkOperators) {
            TableScanOperator tsop = findTableScanOPerator(bottomRsop);
            if (tsop == null) {
              isClear = true; // currently the optimizer can only optimize correlations involving
              // source tables (input tables)
            } else {
              // bottom ReduceSinkOperators are those ReduceSinkOperators which are close to
              // FileSinkOperators
              if (!correlation.getTop2TSops().containsKey(rsop)) {
                correlation.getTop2TSops().put(rsop, new ArrayList<TableScanOperator>());
              }
              correlation.getTop2TSops().get(rsop).add(tsop);

              if (!correlation.getBottom2TSops().containsKey(bottomRsop)) {
                correlation.getBottom2TSops().put(bottomRsop, new ArrayList<TableScanOperator>());
              }
              correlation.getBottom2TSops().get(bottomRsop).add(tsop);
            }
          }
          if (isClear) {
            thisBottomReduceSinkOperators.clear();
            thisBottomReduceSinkOperators.add(rsop);
          }
        }
        bottomReduceSinkOperators.addAll(thisBottomReduceSinkOperators);
      }

      if (!peerReduceSinkOperators.containsAll(bottomReduceSinkOperators)) {
        LOG.info("has job flow correlation");
        correlation.setJobFlowCorrelation(true);
        correlation.setJFCCorrelation(peerReduceSinkOperators, bottomReduceSinkOperators);
        annotateOpPlan(correlation);
      }

      if (correlation.hasJobFlowCorrelation()) {
        boolean hasICandTC = findICandTC(correlation);
        LOG.info("has input correlation and transit correlation? " + hasICandTC);
        correlation.setInputCorrelation(hasICandTC);
        correlation.setTransitCorrelation(hasICandTC);
        boolean hasSelfJoin = hasSelfJoin(correlation);
        LOG.info("has self-join? " + hasSelfJoin);
        correlation.setInvolveSelfJoin(hasSelfJoin);
        // TODO: support self-join involved cases. For self-join related operation paths, after the
        // correlation dispatch operator, each path should be filtered by a filter operator
        if (!hasSelfJoin) {
          LOG.info("correlation detected");
          correlationCtx.addCorrelation(correlation);
        } else {
          LOG.info("correlation discarded. The current optimizer cannot optimize it");
        }
      }
      correlationCtx.addWalkedAll(peerReduceSinkOperators);
      return null;
    }

    private boolean hasSelfJoin(IntraQueryCorrelation correlation) {
      boolean hasSelfJoin = false;
      for (Entry<String, List<ReduceSinkOperator>> entry : correlation
          .getTable2CorrelatedRSops().entrySet()) {
        for (ReduceSinkOperator rsop : entry.getValue()) {
          Set<ReduceSinkOperator> intersection = new HashSet<ReduceSinkOperator>(
              CorrelationOptimizerUtils.findPeerReduceSinkOperators(rsop));
          intersection.retainAll(entry.getValue());
          // if self-join is involved
          if (intersection.size() > 1) {
            hasSelfJoin = true;
            return hasSelfJoin;
          }
        }
      }
      return hasSelfJoin;
    }

    private boolean findICandTC(IntraQueryCorrelation correlation) {

      boolean hasICandTC = false;
      Map<String, List<ReduceSinkOperator>> table2RSops =
          new HashMap<String, List<ReduceSinkOperator>>();
      Map<String, List<TableScanOperator>> table2TSops =
          new HashMap<String, List<TableScanOperator>>();

      for (Entry<ReduceSinkOperator, List<TableScanOperator>> entry : correlation
          .getBottom2TSops().entrySet()) {
        String tbl = aliastoTabName.get(entry.getValue().get(0).getConf().getAlias());
        if (!table2RSops.containsKey(tbl) && !table2TSops.containsKey(tbl)) {
          table2RSops.put(tbl, new ArrayList<ReduceSinkOperator>());
          table2TSops.put(tbl, new ArrayList<TableScanOperator>());
        }
        assert entry.getValue().size() == 1;
        table2RSops.get(tbl).add(entry.getKey());
        table2TSops.get(tbl).add(entry.getValue().get(0));
      }

      for (Entry<String, List<ReduceSinkOperator>> entry : table2RSops.entrySet()) {
        if (entry.getValue().size() > 1) {
          hasICandTC = true;
          break;
        }
      }
      correlation.setICandTCCorrelation(table2RSops, table2TSops);
      return hasICandTC;
    }
  }

  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx ctx, Object... nodeOutputs) throws SemanticException {
        LOG.info("Walk to operator " + ((Operator) nd).getIdentifier() + " "
            + ((Operator) nd).getName() + ". No actual work to do");
        CorrelationNodeProcCtx correlationCtx = (CorrelationNodeProcCtx) ctx;
        Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
        if (op.getName().equals(MapJoinOperator.getOperatorName())) {
          correlationCtx.setAbort(true);
          correlationCtx.getAbortReasons().add("Found MAPJOIN");
        }
        addOperatorInfo((Operator<? extends OperatorDesc>) nd);
        return null;
      }
    };
  }

  public class IntraQueryCorrelation {

    private final Map<ReduceSinkOperator, List<ReduceSinkOperator>> down2upRSops =
        new HashMap<ReduceSinkOperator, List<ReduceSinkOperator>>();
    private final Map<ReduceSinkOperator, List<ReduceSinkOperator>> up2downRSops =
        new HashMap<ReduceSinkOperator, List<ReduceSinkOperator>>();

    private final Map<ReduceSinkOperator, List<TableScanOperator>> top2TSops =
        new HashMap<ReduceSinkOperator, List<TableScanOperator>>();
    private final Map<ReduceSinkOperator, List<TableScanOperator>> bottom2TSops =
        new HashMap<ReduceSinkOperator, List<TableScanOperator>>();

    private List<ReduceSinkOperator> topReduceSinkOperators;
    private List<ReduceSinkOperator> bottomReduceSinkOperators;

    private Map<String, List<ReduceSinkOperator>> table2CorrelatedRSops;

    private Map<String, List<TableScanOperator>> table2CorrelatedTSops;

    private Map<ReduceSinkOperator, Integer> bottomReduceSink2OperationPathMap;

    private final Map<Integer, Map<Integer, List<Integer>>> dispatchConf =
        new HashMap<Integer, Map<Integer, List<Integer>>>(); // inputTag->(Child->outputTag)
    private final Map<Integer, Map<Integer, List<SelectDesc>>> dispatchValueSelectDescConf =
        new HashMap<Integer, Map<Integer, List<SelectDesc>>>(); // inputTag->(Child->SelectDesc)
    private final Map<Integer, Map<Integer, List<SelectDesc>>> dispatchKeySelectDescConf =
        new HashMap<Integer, Map<Integer, List<SelectDesc>>>(); // inputTag->(Child->SelectDesc)

    private final Set<ReduceSinkOperator> allReduceSinkOperators =
        new HashSet<ReduceSinkOperator>();

    // this set contains all ReduceSink-GroupBy operator-pairs that should be be replaced by
    // GroupBy-ReduceSink-GroupBy pattern.
    // the type of first GroupByOperator is hash type and this one will be used to group records.
    private final Set<ReduceSinkOperator> rSGBYToBeReplacedByGBYRSGBY =
        new HashSet<ReduceSinkOperator>();

    public void addToRSGBYToBeReplacedByGBYRSGBY(ReduceSinkOperator rsop) {
      rSGBYToBeReplacedByGBYRSGBY.add(rsop);
    }

    public Set<ReduceSinkOperator> getRSGBYToBeReplacedByGBYRSGBY() {
      return rSGBYToBeReplacedByGBYRSGBY;
    }

    public void addToAllReduceSinkOperators(ReduceSinkOperator rsop) {
      allReduceSinkOperators.add(rsop);
    }

    public Set<ReduceSinkOperator> getAllReduceSinkOperators() {
      return allReduceSinkOperators;
    }

    public Map<Integer, Map<Integer, List<Integer>>> getDispatchConf() {
      return dispatchConf;
    }

    public Map<Integer, Map<Integer, List<SelectDesc>>> getDispatchValueSelectDescConf() {
      return dispatchValueSelectDescConf;
    }

    public Map<Integer, Map<Integer, List<SelectDesc>>> getDispatchKeySelectDescConf() {
      return dispatchKeySelectDescConf;
    }

    public void addOperationPathToDispatchConf(Integer opPlan) {
      if (!dispatchConf.containsKey(opPlan)) {
        dispatchConf.put(opPlan, new HashMap<Integer, List<Integer>>());
      }
    }

    public Map<Integer, List<Integer>> getDispatchConfForOperationPath(Integer opPlan) {
      return dispatchConf.get(opPlan);
    }

    public void addOperationPathToDispatchValueSelectDescConf(Integer opPlan) {
      if (!dispatchValueSelectDescConf.containsKey(opPlan)) {
        dispatchValueSelectDescConf.put(opPlan, new HashMap<Integer, List<SelectDesc>>());
      }
    }

    public Map<Integer, List<SelectDesc>> getDispatchValueSelectDescConfForOperationPath(
        Integer opPlan) {
      return dispatchValueSelectDescConf.get(opPlan);
    }

    public void addOperationPathToDispatchKeySelectDescConf(Integer opPlan) {
      if (!dispatchKeySelectDescConf.containsKey(opPlan)) {
        dispatchKeySelectDescConf.put(opPlan, new HashMap<Integer, List<SelectDesc>>());
      }
    }

    public Map<Integer, List<SelectDesc>> getDispatchKeySelectDescConfForOperationPath(
        Integer opPlan) {
      return dispatchKeySelectDescConf.get(opPlan);
    }

    private boolean inputCorrelation = false;
    private boolean transitCorrelation = false;
    private boolean jobFlowCorrelation = false;

    public void setBottomReduceSink2OperationPathMap(
        Map<ReduceSinkOperator, Integer> bottomReduceSink2OperationPathMap) {
      this.bottomReduceSink2OperationPathMap = bottomReduceSink2OperationPathMap;
    }

    public Map<ReduceSinkOperator, Integer> getBottomReduceSink2OperationPathMap() {
      return bottomReduceSink2OperationPathMap;
    }

    public void setInputCorrelation(boolean inputCorrelation) {
      this.inputCorrelation = inputCorrelation;
    }

    public boolean hasInputCorrelation() {
      return inputCorrelation;
    }

    public void setTransitCorrelation(boolean transitCorrelation) {
      this.transitCorrelation = transitCorrelation;
    }

    public boolean hasTransitCorrelation() {
      return transitCorrelation;
    }

    public void setJobFlowCorrelation(boolean jobFlowCorrelation) {
      this.jobFlowCorrelation = jobFlowCorrelation;
    }

    public boolean hasJobFlowCorrelation() {
      return jobFlowCorrelation;
    }

    public Map<ReduceSinkOperator, List<TableScanOperator>> getTop2TSops() {
      return top2TSops;
    }

    public Map<ReduceSinkOperator, List<TableScanOperator>> getBottom2TSops() {
      return bottom2TSops;
    }

    public Map<ReduceSinkOperator, List<ReduceSinkOperator>> getDown2upRSops() {
      return down2upRSops;
    }

    public Map<ReduceSinkOperator, List<ReduceSinkOperator>> getUp2downRSops() {
      return up2downRSops;
    }

    public void setJFCCorrelation(List<ReduceSinkOperator> peerReduceSinkOperators,
        List<ReduceSinkOperator> bottomReduceSinkOperators) {
      this.topReduceSinkOperators = peerReduceSinkOperators;
      this.bottomReduceSinkOperators = bottomReduceSinkOperators;
    }


    public List<ReduceSinkOperator> getTopReduceSinkOperators() {
      return topReduceSinkOperators;
    }

    public List<ReduceSinkOperator> getBottomReduceSinkOperators() {
      return bottomReduceSinkOperators;
    }

    public void setICandTCCorrelation(Map<String, List<ReduceSinkOperator>> table2RSops,
        Map<String, List<TableScanOperator>> table2TSops) {
      this.table2CorrelatedRSops = table2RSops;
      this.table2CorrelatedTSops = table2TSops;
    }

    public Map<String, List<ReduceSinkOperator>> getTable2CorrelatedRSops() {
      return table2CorrelatedRSops;
    }

    public Map<String, List<TableScanOperator>> getTable2CorrelatedTSops() {
      return table2CorrelatedTSops;
    }

    private boolean isInvolveSelfJoin = false;

    public boolean isInvolveSelfJoin() {
      return isInvolveSelfJoin;
    }

    public void setInvolveSelfJoin(boolean isInvolveSelfJoin) {
      this.isInvolveSelfJoin = isInvolveSelfJoin;
    }

  }

  private class CorrelationNodePhase1ProcCtx implements NodeProcessorCtx {
    public int fileSinkOperatorCount = 0;
  }

  private class CorrelationNodeProcCtx implements NodeProcessorCtx {

    private boolean abort;

    private final List<String> abortReasons;

    private final Set<ReduceSinkOperator> walked;

    private final List<IntraQueryCorrelation> correlations;

    public CorrelationNodeProcCtx() {
      walked = new HashSet<ReduceSinkOperator>();
      correlations = new ArrayList<IntraQueryCorrelation>();
      abort = false;
      abortReasons = new ArrayList<String>();
    }

    public void setAbort(boolean abort) {
      this.abort = abort;
    }

    public boolean isAbort() {
      return abort;
    }

    public List<String> getAbortReasons() {
      return abortReasons;
    }

    public void addCorrelation(IntraQueryCorrelation correlation) {
      correlations.add(correlation);
    }

    public List<IntraQueryCorrelation> getCorrelations() {
      return correlations;
    }

    public boolean isWalked(ReduceSinkOperator op) {
      return walked.contains(op);
    }

    public void addWalked(ReduceSinkOperator op) {
      walked.add(op);
    }

    public void addWalkedAll(Collection<ReduceSinkOperator> c) {
      walked.addAll(c);
    }

  }

}
