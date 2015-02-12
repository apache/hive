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

package org.apache.hadoop.hive.ql.optimizer.correlation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.MapJoinProcessor;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.optimizer.physical.CommonJoinTaskDispatcher;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * Implementation of  Correlation Optimizer. This optimizer is based on
 * the paper "YSmart: Yet Another SQL-to-MapReduce Translator"
 * (Rubao Lee, Tian Luo, Yin Huai, Fusheng Wang, Yongqiang He, and Xiaodong Zhang)
 * (http://www.cse.ohio-state.edu/hpcs/WWW/HTML/publications/papers/TR-11-7.pdf).
 * Correlation Optimizer detects if ReduceSinkOperators share same keys.
 * Then, it will transform the query plan tree (operator tree) by exploiting
 * detected correlations. For details, see the original paper of YSmart.
 *
 * Test queries associated with this optimizer are correlationoptimizer1.q to
 * correlationoptimizer14.q
 */
public class CorrelationOptimizer implements Transform {

  private static final Log LOG = LogFactory.getLog(CorrelationOptimizer.class.getName());

  private boolean abort; // if correlation optimizer will not try to optimize this query

  private ParseContext pCtx;

  //Join operators which may be converted by CommonJoinResolver;
  private final Set<Operator<? extends OperatorDesc>> skipedJoinOperators;

  public CorrelationOptimizer() {
    super();
    pCtx = null;
    skipedJoinOperators = new HashSet<Operator<? extends OperatorDesc>>();
    abort = false;
  }

  private void findPossibleAutoConvertedJoinOperators() throws SemanticException {
    // Guess if CommonJoinResolver will work. If CommonJoinResolver may
    // convert a join operation, correlation optimizer will not merge that join.
    // TODO: If hive.auto.convert.join.noconditionaltask=true, for a JoinOperator
    // that has both intermediate tables and query input tables as input tables,
    // we should be able to guess if this JoinOperator will be converted to a MapJoin
    // based on hive.auto.convert.join.noconditionaltask.size.
    for (JoinOperator joinOp: pCtx.getJoinOps()) {
      boolean isAbleToGuess = true;
      boolean mayConvert = false;
      // Get total size and individual alias's size
      long aliasTotalKnownInputSize = 0;
      Map<String, Long> aliasToSize = new HashMap<String, Long>();
      Map<Integer, Set<String>> posToAliases = new HashMap<Integer, Set<String>>();
      for (int pos = 0; pos < joinOp.getNumParent(); pos++) {
        Operator<? extends OperatorDesc> op = joinOp.getParentOperators().get(pos);
        Set<TableScanOperator> topOps = CorrelationUtilities.findTableScanOperators(op);
        if (topOps.isEmpty()) {
          isAbleToGuess = false;
          break;
        }

        Set<String> aliases = new LinkedHashSet<String>();
        for (TableScanOperator tsop : topOps) {
          Table table = tsop.getConf().getTableMetadata();
          if (table == null) {
            // table should not be null.
            throw new SemanticException("The table of " +
                tsop.getName() + " " + tsop.getIdentifier() +
                " is null, which is not expected.");
          }
          String alias = tsop.getConf().getAlias();
          aliases.add(alias);

          Path p = table.getPath();
          ContentSummary resultCs = null;
          try {
            FileSystem fs = table.getPath().getFileSystem(pCtx.getConf());
            resultCs = fs.getContentSummary(p);
          } catch (IOException e) {
            LOG.warn("Encounter a error while querying content summary of table " +
                table.getCompleteName() + " from FileSystem. " +
                "Cannot guess if CommonJoinOperator will optimize " +
                joinOp.getName() + " " + joinOp.getIdentifier());
          }
          if (resultCs == null) {
            isAbleToGuess = false;
            break;
          }

          long size = resultCs.getLength();
          aliasTotalKnownInputSize += size;
          Long es = aliasToSize.get(alias);
          if(es == null) {
            es = new Long(0);
          }
          es += size;
          aliasToSize.put(alias, es);
        }
        posToAliases.put(pos, aliases);
      }

      if (!isAbleToGuess) {
        LOG.info("Cannot guess if CommonJoinOperator will optimize " +
            joinOp.getName() + " " + joinOp.getIdentifier());
        continue;
      }

      JoinDesc joinDesc = joinOp.getConf();
      Byte[] order = joinDesc.getTagOrder();
      int numAliases = order.length;
      Set<Integer> bigTableCandidates =
          MapJoinProcessor.getBigTableCandidates(joinDesc.getConds());
      if (bigTableCandidates.isEmpty()) {
        continue;
      }

      long ThresholdOfSmallTblSizeSum = HiveConf.getLongVar(pCtx.getConf(),
          HiveConf.ConfVars.HIVESMALLTABLESFILESIZE);
      for (int i = 0; i < numAliases; i++) {
        // this table cannot be big table
        if (!bigTableCandidates.contains(i)) {
          continue;
        }
        Set<String> aliases = posToAliases.get(i);
        long aliasKnownSize = Utilities.sumOf(aliasToSize, aliases);
        if (!CommonJoinTaskDispatcher.cannotConvert(aliasKnownSize,
            aliasTotalKnownInputSize, ThresholdOfSmallTblSizeSum)) {
          mayConvert = true;
        }
      }

      if (mayConvert) {
        LOG.info(joinOp.getName() + " " + joinOp.getIdentifier() +
            " may be converted to MapJoin by CommonJoinResolver");
        skipedJoinOperators.add(joinOp);
      }
    }
  }

  /**
   * Detect correlations and transform the query tree.
   *
   * @param pactx
   *          current parse context
   * @throws SemanticException
   */
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    pCtx = pctx;

    if (HiveConf.getBoolVar(pCtx.getConf(),HiveConf.ConfVars.HIVECONVERTJOIN)) {
      findPossibleAutoConvertedJoinOperators();
    }

    // detect correlations
    CorrelationNodeProcCtx corrCtx = new CorrelationNodeProcCtx(pCtx);
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", ReduceSinkOperator.getOperatorName() + "%"),
        new CorrelationNodeProc());

    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, corrCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topOp nodes
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    // We have finished tree walking (correlation detection).
    // We will first see if we need to abort (the operator tree has not been changed).
    // If not, we will start to transform the operator tree.
    abort = corrCtx.isAbort();
    if (abort) {
      LOG.info("Abort. Reasons are ...");
      for (String reason : corrCtx.getAbortReasons()) {
        LOG.info("-- " + reason);
      }
    } else {
      // transform the operator tree
      LOG.info("Begain query plan transformation based on intra-query correlations. " +
          corrCtx.getCorrelations().size() + " correlation(s) to be applied");
      for (IntraQueryCorrelation correlation : corrCtx.getCorrelations()) {
        QueryPlanTreeTransformation.applyCorrelation(pCtx, corrCtx, correlation);
      }
    }
    return pCtx;
  }

  private class CorrelationNodeProc implements NodeProcessor {

    private void analyzeReduceSinkOperatorsOfJoinOperator(JoinCondDesc[] joinConds,
        List<Operator<? extends OperatorDesc>> rsOps, Operator<? extends OperatorDesc> curentRsOp,
        Set<ReduceSinkOperator> correlatedRsOps) {
      if (correlatedRsOps.contains((ReduceSinkOperator) curentRsOp)) {
        return;
      }
      correlatedRsOps.add((ReduceSinkOperator) curentRsOp);

      int pos = rsOps.indexOf(curentRsOp);
      for (int i = 0; i < joinConds.length; i++) {
        JoinCondDesc joinCond = joinConds[i];
        int type = joinCond.getType();
        if (pos == joinCond.getLeft()) {
          if (type == JoinDesc.INNER_JOIN ||
              type == JoinDesc.LEFT_OUTER_JOIN ||
              type == JoinDesc.LEFT_SEMI_JOIN) {
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

    private boolean sameKeys(List<ExprNodeDesc> k1, List<ExprNodeDesc> k2) {
      if (k1.size() != k2.size()) {
        return false;
      }
      for (int i = 0; i < k1.size(); i++) {
        ExprNodeDesc expr1 = k1.get(i);
        ExprNodeDesc expr2 = k2.get(i);
        if (expr1 == null) {
          if (expr2 == null) {
            continue;
          } else {
            return false;
          }
        } else {
          if (!expr1.isSame(expr2)) {
            return false;
          }
        }
      }
      return true;
    }

    private boolean sameOrder(String order1, String order2) {
      if (order1 == null || order1.trim().equals("")) {
        if (order2 == null || order2.trim().equals("")) {
          return true;
        }
        return false;
      }
      if (order2 == null || order2.trim().equals("")) {
        return false;
      }
      order1 = order1.trim();
      order2 = order2.trim();
      if (!order1.equals(order2)) {
        return false;
      }
      return true;
    }
    /**
     * This method is used to recursively traverse the tree to find
     * ReduceSinkOperators which share the same key columns and partitioning
     * columns. Those ReduceSinkOperators are called correlated ReduceSinkOperaotrs.
     *
     * @param child The child of the current operator
     * @param childKeyCols The key columns from the child operator
     * @param childPartitionCols The partitioning columns from the child operator
     * @param childRSOrder The sorting order of key columns from the child operator
     * @param current The current operator we are visiting
     * @param correlation The object keeps tracking the correlation
     * @return
     * @throws SemanticException
     */
    private LinkedHashSet<ReduceSinkOperator> findCorrelatedReduceSinkOperators(
        Operator<? extends OperatorDesc> child,
        List<ExprNodeDesc> childKeyCols, List<ExprNodeDesc> childPartitionCols,
        String childRSOrder,
        Operator<? extends OperatorDesc> current,
        IntraQueryCorrelation correlation) throws SemanticException {

      LOG.info("now detecting operator " + current.getIdentifier() + " " + current.getName());
      LinkedHashSet<ReduceSinkOperator> correlatedReduceSinkOperators =
          new LinkedHashSet<ReduceSinkOperator>();
      if (skipedJoinOperators.contains(current)) {
        LOG.info(current.getName() + " " + current.getIdentifier() +
            " may be converted to MapJoin by " +
            "CommonJoinResolver. Correlation optimizer will not detect correlations" +
            "involved in this operator");
        return correlatedReduceSinkOperators;
      }
      if ((current.getParentOperators() == null) || (current.getParentOperators().isEmpty())) {
        return correlatedReduceSinkOperators;
      }
      if (current instanceof PTFOperator) {
        // Currently, we do not support PTF operator.
        LOG.info("Currently, correlation optimizer does not support PTF operator.");
        return correlatedReduceSinkOperators;
      }
      if (current instanceof UnionOperator) {
        // If we get a UnionOperator, right now, we only handle it when
        // we can find correlated ReduceSinkOperators from all inputs.
        LinkedHashSet<ReduceSinkOperator> corrRSs = new LinkedHashSet<ReduceSinkOperator>();
        for (Operator<? extends OperatorDesc> parent : current.getParentOperators()) {
          LinkedHashSet<ReduceSinkOperator> tmp =
              findCorrelatedReduceSinkOperators(
                  current, childKeyCols, childPartitionCols, childRSOrder, parent, correlation);
          if (tmp != null && tmp.size() > 0) {
            corrRSs.addAll(tmp);
          } else {
            return correlatedReduceSinkOperators;
          }
        }
        correlatedReduceSinkOperators.addAll(corrRSs);
        UnionOperator union = (UnionOperator)current;
        union.getConf().setAllInputsInSameReducer(true);
      } else if (current.getColumnExprMap() == null && !(current instanceof ReduceSinkOperator)) {
        for (Operator<? extends OperatorDesc> parent : current.getParentOperators()) {
          correlatedReduceSinkOperators.addAll(
              findCorrelatedReduceSinkOperators(
                  current, childKeyCols, childPartitionCols, childRSOrder, parent, correlation));
        }
      } else if (current.getColumnExprMap() != null && !(current instanceof ReduceSinkOperator)) {
        List<ExprNodeDesc> backtrackedKeyCols =
            ExprNodeDescUtils.backtrack(childKeyCols, child, current);
        List<ExprNodeDesc> backtrackedPartitionCols =
            ExprNodeDescUtils.backtrack(childPartitionCols, child, current);

        RowSchema rowSchema = current.getSchema();
        Set<String> tableNeedToCheck = new HashSet<String>();
        for (ExprNodeDesc expr: childKeyCols) {
          if (!(expr instanceof ExprNodeColumnDesc)) {
            return correlatedReduceSinkOperators;
          }
          String colName = ((ExprNodeColumnDesc)expr).getColumn();
          ColumnInfo columnInfo = rowSchema.getColumnInfo(colName);
          if (columnInfo != null) {
            tableNeedToCheck.add(columnInfo.getTabAlias());
          }
        }
        if (current instanceof JoinOperator) {
          boolean isCorrelated = true;
          int expectedNumCorrelatedRsops = current.getParentOperators().size();
          LinkedHashSet<ReduceSinkOperator> correlatedRsops = null;
          for (Operator<? extends OperatorDesc> parent : current.getParentOperators()) {
            Set<String> tableNames = parent.getSchema().getTableNames();
            for (String tbl : tableNames) {
              if (tableNeedToCheck.contains(tbl)) {
                correlatedRsops = findCorrelatedReduceSinkOperators(current,
                    backtrackedKeyCols, backtrackedPartitionCols,
                    childRSOrder, parent, correlation);
                if (correlatedRsops.size() != expectedNumCorrelatedRsops) {
                  isCorrelated = false;
                }
              }
            }
            if (!isCorrelated) {
              break;
            }
          }
          // If current is JoinOperaotr, we will stop to traverse the tree
          // when any of parent ReduceSinkOperaotr of this JoinOperator is
          // not considered as a correlated ReduceSinkOperator.
          if (isCorrelated && correlatedRsops != null) {
            correlatedReduceSinkOperators.addAll(correlatedRsops);
          } else {
            correlatedReduceSinkOperators.clear();
          }
        } else {
          for (Operator<? extends OperatorDesc> parent : current.getParentOperators()) {
            correlatedReduceSinkOperators.addAll(findCorrelatedReduceSinkOperators(
                current, backtrackedKeyCols, backtrackedPartitionCols, childRSOrder,
                parent, correlation));
          }
        }
      } else if (current.getColumnExprMap() != null && current instanceof ReduceSinkOperator) {
        ReduceSinkOperator rsop = (ReduceSinkOperator) current;
        List<ExprNodeDesc> backtrackedKeyCols =
            ExprNodeDescUtils.backtrack(childKeyCols, child, current);
        List<ExprNodeDesc> backtrackedPartitionCols =
            ExprNodeDescUtils.backtrack(childPartitionCols, child, current);
        List<ExprNodeDesc> rsKeyCols = rsop.getConf().getKeyCols();
        List<ExprNodeDesc> rsPartitionCols = rsop.getConf().getPartitionCols();

        // Two ReduceSinkOperators are correlated means that
        // they have same sorting columns (key columns), same partitioning columns,
        // same sorting orders, and no conflict on the numbers of reducers.
        // TODO: we should relax this condition
        // TODO: we need to handle aggregation functions with distinct keyword. In this case,
        // distinct columns will be added to the key columns.
        boolean isCorrelated = sameKeys(rsKeyCols, backtrackedKeyCols) &&
            sameOrder(rsop.getConf().getOrder(), childRSOrder) &&
            sameKeys(backtrackedPartitionCols, rsPartitionCols) &&
            correlation.adjustNumReducers(rsop.getConf().getNumReducers());
        GroupByOperator cGBY =
            CorrelationUtilities.getSingleChild(rsop, GroupByOperator.class);
        if (cGBY != null) {
          if (CorrelationUtilities.hasGroupingSet(rsop) ||
              cGBY.getConf().isGroupingSetsPresent()) {
            // Do not support grouping set right now
            isCorrelated = false;
          }
        }

        if (isCorrelated) {
          LOG.info("Operator " + current.getIdentifier() + " " +
              current.getName() + " is correlated");
          Operator<? extends OperatorDesc> childOperator =
              CorrelationUtilities.getSingleChild(current, true);
          if (childOperator instanceof JoinOperator) {
            JoinOperator joinOp = (JoinOperator) childOperator;
            JoinCondDesc[] joinConds = joinOp.getConf().getConds();
            List<Operator<? extends OperatorDesc>> rsOps = joinOp.getParentOperators();
            LinkedHashSet<ReduceSinkOperator> correlatedRsOps =
                new LinkedHashSet<ReduceSinkOperator>();
            analyzeReduceSinkOperatorsOfJoinOperator(joinConds, rsOps, current, correlatedRsOps);
            correlatedReduceSinkOperators.addAll(correlatedRsOps);
          } else {
            correlatedReduceSinkOperators.add(rsop);
          }
        } else {
          LOG.info("Operator " + current.getIdentifier() + " " +
              current.getName() + " is not correlated");
          correlatedReduceSinkOperators.clear();
        }
      } else {
        LOG.error("ReduceSinkOperator " + current.getIdentifier() + " does not have ColumnExprMap");
        throw new SemanticException("CorrelationOptimizer cannot optimize this plan. " +
            "ReduceSinkOperator " + current.getIdentifier()
            + " does not have ColumnExprMap");
      }
      return correlatedReduceSinkOperators;
    }

    /** Start to exploit Job Flow Correlation from op.
     * Example: here is the operator tree we have ...
     *       JOIN2
     *      /    \
     *     RS4   RS5
     *    /        \
     *   GBY1     JOIN1
     *    |       /    \
     *   RS1     RS2   RS3
     * The op will be RS4. If we can execute GBY1, JOIN1, and JOIN2 in
     * the same reducer. This method will return [RS1, RS2, RS3].
     * @param op
     * @param correlationCtx
     * @param correlation
     * @return
     * @throws SemanticException
     */
    private LinkedHashSet<ReduceSinkOperator> exploitJobFlowCorrelation(ReduceSinkOperator op,
        CorrelationNodeProcCtx correlationCtx, IntraQueryCorrelation correlation)
        throws SemanticException {
      correlationCtx.addWalked(op);
      correlation.addToAllReduceSinkOperators(op);
      boolean shouldDetect = true;
      LinkedHashSet<ReduceSinkOperator> reduceSinkOperators =
          new LinkedHashSet<ReduceSinkOperator>();
      List<ExprNodeDesc> keyCols = op.getConf().getKeyCols();
      List<ExprNodeDesc> partitionCols = op.getConf().getPartitionCols();
      for (ExprNodeDesc key : keyCols) {
        if (!(key instanceof ExprNodeColumnDesc)) {
          shouldDetect = false;
        }
      }
      for (ExprNodeDesc key : partitionCols) {
        if (!(key instanceof ExprNodeColumnDesc)) {
          shouldDetect = false;
        }
      }
      GroupByOperator cGBY =
          CorrelationUtilities.getSingleChild(op, GroupByOperator.class);
      if (cGBY != null) {
        if (CorrelationUtilities.hasGroupingSet(op) ||
            cGBY.getConf().isGroupingSetsPresent()) {
          // Do not support grouping set right now
          shouldDetect = false;
        }
      }

      if (shouldDetect) {
        LinkedHashSet<ReduceSinkOperator> newReduceSinkOperators =
            new LinkedHashSet<ReduceSinkOperator>();
        String sortOrder = op.getConf().getOrder();
        for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
          LOG.info("Operator " + op.getIdentifier()
              + ": start detecting correlation from this operator");
          LinkedHashSet<ReduceSinkOperator> correlatedReduceSinkOperators =
              findCorrelatedReduceSinkOperators(op, keyCols, partitionCols,
                  sortOrder, parent, correlation);
          if (correlatedReduceSinkOperators.size() == 0) {
            newReduceSinkOperators.add(op);
          } else {
            for (ReduceSinkOperator rsop : correlatedReduceSinkOperators) {
              LinkedHashSet<ReduceSinkOperator> exploited =
                  exploitJobFlowCorrelation(rsop, correlationCtx, correlation);
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

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
        Object... nodeOutputs) throws SemanticException {
      CorrelationNodeProcCtx corrCtx = (CorrelationNodeProcCtx) ctx;
      ReduceSinkOperator op = (ReduceSinkOperator) nd;
      // Check if we have visited this operator
      if (corrCtx.isWalked(op)) {
        return null;
      }

      LOG.info("Walk to operator " + op.getIdentifier() + " " + op.getName());

      Operator<? extends OperatorDesc> child = CorrelationUtilities.getSingleChild(op, true);
      if (!(child instanceof JoinOperator) && !(child instanceof GroupByOperator)) {
        corrCtx.addWalked(op);
        return null;
      }

      // detect correlations
      IntraQueryCorrelation correlation = new IntraQueryCorrelation(corrCtx.minReducer());
      List<ReduceSinkOperator> topReduceSinkOperators =
          CorrelationUtilities.findSiblingReduceSinkOperators(op);
      List<ReduceSinkOperator> bottomReduceSinkOperators = new ArrayList<ReduceSinkOperator>();
      // Adjust the number of reducers of this correlation based on
      // those top layer ReduceSinkOperators.
      for (ReduceSinkOperator rsop : topReduceSinkOperators) {
        if (!correlation.adjustNumReducers(rsop.getConf().getNumReducers())) {
          // If we have a conflict on the number of reducers, we will not optimize
          // this plan from here.
          corrCtx.addWalked(op);
          return null;
        }
      }
      for (ReduceSinkOperator rsop : topReduceSinkOperators) {
        LinkedHashSet<ReduceSinkOperator> thisBottomReduceSinkOperators =
            exploitJobFlowCorrelation(rsop, corrCtx, correlation);
        if (thisBottomReduceSinkOperators.size() == 0) {
          thisBottomReduceSinkOperators.add(rsop);
        }
        bottomReduceSinkOperators.addAll(thisBottomReduceSinkOperators);
      }

      if (!topReduceSinkOperators.containsAll(bottomReduceSinkOperators)) {
        LOG.info("has job flow correlation");
        correlation.setJobFlowCorrelation(true, bottomReduceSinkOperators);
      }

      if (correlation.hasJobFlowCorrelation()) {
        corrCtx.addCorrelation(correlation);
      } else {
        // Since we cannot merge operators into a single MR job from here,
        // we should remove ReduceSinkOperators added into walked in exploitJFC
        corrCtx.removeWalkedAll(correlation.getAllReduceSinkOperators());
      }

      corrCtx.addWalked(op);
      return null;
    }
  }

  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
          NodeProcessorCtx ctx, Object... nodeOutputs) throws SemanticException {
        Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
        LOG.info("Walk to operator " + op.getIdentifier() + " "
            + op.getName() + ". No actual work to do");
        CorrelationNodeProcCtx correlationCtx = (CorrelationNodeProcCtx) ctx;
        if (op.getName().equals(MapJoinOperator.getOperatorName())) {
          correlationCtx.setAbort(true);
          correlationCtx.getAbortReasons().add("Found MAPJOIN");
        }
        if (op.getName().equals(FileSinkOperator.getOperatorName())) {
          correlationCtx.incrementFileSinkOperatorCount();
        }
        return null;
      }
    };
  }

  protected class CorrelationNodeProcCtx extends AbstractCorrelationProcCtx {

    private boolean abort;
    private final List<String> abortReasons;

    private final Set<ReduceSinkOperator> walked;

    private final List<IntraQueryCorrelation> correlations;

    private int fileSinkOperatorCount;

    public CorrelationNodeProcCtx(ParseContext pctx) {
      super(pctx);
      walked = new HashSet<ReduceSinkOperator>();
      correlations = new ArrayList<IntraQueryCorrelation>();
      abort = false;
      abortReasons = new ArrayList<String>();
      fileSinkOperatorCount = 0;
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

    public void removeWalked(ReduceSinkOperator op) {
      walked.remove(op);
    }

    public void removeWalkedAll(Collection<ReduceSinkOperator> c) {
      walked.removeAll(c);
    }

    public void incrementFileSinkOperatorCount() {
      fileSinkOperatorCount++;
      if (fileSinkOperatorCount == 2) {
        abort = true;
        abortReasons.add(
            "-- Currently, a query with multiple FileSinkOperators are not supported.");
      }
    }
  }

}
