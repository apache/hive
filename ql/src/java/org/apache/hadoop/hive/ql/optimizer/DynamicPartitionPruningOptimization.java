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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
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
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.OptimizeTezProcContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

/**
 * This optimization looks for expressions of the kind "x IN (RS[n])". If such
 * an expression made it to a table scan operator and x is a partition column we
 * can use an existing join to dynamically prune partitions. This class sets up
 * the infrastructure for that.
 */
public class DynamicPartitionPruningOptimization implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(DynamicPartitionPruningOptimization.class
      .getName());

  public static class DynamicPartitionPrunerProc implements NodeProcessor {

    /**
     * process simply remembers all the dynamic partition pruning expressions
     * found
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprNodeDynamicListDesc desc = (ExprNodeDynamicListDesc) nd;
      DynamicPartitionPrunerContext context = (DynamicPartitionPrunerContext) procCtx;

      // Rule is searching for dynamic pruning expr. There's at least an IN
      // expression wrapping it.
      ExprNodeDesc parent = (ExprNodeDesc) stack.get(stack.size() - 2);
      ExprNodeDesc grandParent = stack.size() >= 3 ? (ExprNodeDesc) stack.get(stack.size() - 3) : null;

      context.addDynamicList(desc, parent, grandParent, (ReduceSinkOperator) desc.getSource());

      return context;
    }
  }

  private static class DynamicListContext {
    public ExprNodeDynamicListDesc desc;
    public ExprNodeDesc parent;
    public ExprNodeDesc grandParent;
    public ReduceSinkOperator generator;

    public DynamicListContext(ExprNodeDynamicListDesc desc, ExprNodeDesc parent,
        ExprNodeDesc grandParent, ReduceSinkOperator generator) {
      this.desc = desc;
      this.parent = parent;
      this.grandParent = grandParent;
      this.generator = generator;
    }
  }

  private static class DynamicPartitionPrunerContext implements NodeProcessorCtx,
      Iterable<DynamicListContext> {
    public List<DynamicListContext> dynLists = new ArrayList<DynamicListContext>();

    public void addDynamicList(ExprNodeDynamicListDesc desc, ExprNodeDesc parent,
        ExprNodeDesc grandParent, ReduceSinkOperator generator) {
      dynLists.add(new DynamicListContext(desc, parent, grandParent, generator));
    }

    @Override
    public Iterator<DynamicListContext> iterator() {
      return dynLists.iterator();
    }
  }

  private String extractColName(ExprNodeDesc root) {
    if (root instanceof ExprNodeColumnDesc) {
      return ((ExprNodeColumnDesc) root).getColumn();
    } else {
      if (root.getChildren() == null) {
        return null;
      }

      String column = null;
      for (ExprNodeDesc d: root.getChildren()) {
        String candidate = extractColName(d);
        if (column != null && candidate != null) {
          return null;
        } else if (candidate != null) {
          column = candidate;
        }
      }
      return column;
    }
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
      throws SemanticException {
    OptimizeTezProcContext context = (OptimizeTezProcContext) procCtx;
    ParseContext parseContext = context.parseContext;

    FilterOperator filter = (FilterOperator) nd;
    FilterDesc desc = filter.getConf();

    TableScanOperator ts = null;

    if (!parseContext.getConf().getBoolVar(ConfVars.TEZ_DYNAMIC_PARTITION_PRUNING)) {
      // nothing to do when the optimization is off
      return null;
    }

    DynamicPartitionPrunerContext removerContext = new DynamicPartitionPrunerContext();

    if (filter.getParentOperators().size() == 1
        && filter.getParentOperators().get(0) instanceof TableScanOperator) {
      ts = (TableScanOperator) filter.getParentOperators().get(0);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Parent: " + filter.getParentOperators().get(0));
      LOG.debug("Filter: " + desc.getPredicateString());
      LOG.debug("TableScan: " + ts);
    }

    // collect the dynamic pruning conditions
    removerContext.dynLists.clear();
    walkExprTree(desc.getPredicate(), removerContext);

    for (DynamicListContext ctx : removerContext) {
      String column = extractColName(ctx.parent);

      if (ts != null && column != null) {
        Table table = ts.getConf().getTableMetadata();

        if (table != null && table.isPartitionKey(column)) {
          String alias = ts.getConf().getAlias();
          PrunedPartitionList plist = parseContext.getPrunedPartitions(alias, ts);
          if (LOG.isDebugEnabled()) {
            LOG.debug("alias: " + alias);
            LOG.debug("pruned partition list: ");
            if (plist != null) {
              for (Partition p : plist.getPartitions()) {
                LOG.debug(p.getCompleteName());
              }
            }
          }
          if (plist == null || plist.getPartitions().size() != 0) {
            LOG.info("Dynamic partitioning: " + table.getCompleteName() + "." + column);
            generateEventOperatorPlan(ctx, parseContext, ts, column);
          } else {
            // all partitions have been statically removed
            LOG.debug("No partition pruning necessary.");
          }
        } else {
          LOG.debug("Column " + column + " is not a partition column");
        }
      }

      // we always remove the condition by replacing it with "true"
      ExprNodeDesc constNode = new ExprNodeConstantDesc(ctx.parent.getTypeInfo(), true);
      if (ctx.grandParent == null) {
        desc.setPredicate(constNode);
      } else {
        int i = ctx.grandParent.getChildren().indexOf(ctx.parent);
        ctx.grandParent.getChildren().remove(i);
        ctx.grandParent.getChildren().add(i, constNode);
      }
    }

    // if we pushed the predicate into the table scan we need to remove the
    // synthetic conditions there.
    cleanTableScanFilters(ts);

    return false;
  }

  private void cleanTableScanFilters(TableScanOperator ts) throws SemanticException {

    if (ts == null || ts.getConf() == null || ts.getConf().getFilterExpr() == null) {
      // nothing to do
      return;
    }

    DynamicPartitionPrunerContext removerContext = new DynamicPartitionPrunerContext();

    // collect the dynamic pruning conditions
    removerContext.dynLists.clear();
    walkExprTree(ts.getConf().getFilterExpr(), removerContext);

    for (DynamicListContext ctx : removerContext) {
      // remove the condition by replacing it with "true"
      ExprNodeDesc constNode = new ExprNodeConstantDesc(ctx.parent.getTypeInfo(), true);
      if (ctx.grandParent == null) {
        // we're the only node, just clear out the expression
        ts.getConf().setFilterExpr(null);
      } else {
        int i = ctx.grandParent.getChildren().indexOf(ctx.parent);
        ctx.grandParent.getChildren().remove(i);
        ctx.grandParent.getChildren().add(i, constNode);
      }
    }
  }

  private void generateEventOperatorPlan(DynamicListContext ctx, ParseContext parseContext,
      TableScanOperator ts, String column) {

    // we will put a fork in the plan at the source of the reduce sink
    Operator<? extends OperatorDesc> parentOfRS = ctx.generator.getParentOperators().get(0);

    // we need the expr that generated the key of the reduce sink
    ExprNodeDesc key = ctx.generator.getConf().getKeyCols().get(ctx.desc.getKeyIndex());

    // we also need the expr for the partitioned table
    ExprNodeDesc partKey = ctx.parent.getChildren().get(0);

    if (LOG.isDebugEnabled()) {
      LOG.debug("key expr: " + key);
      LOG.debug("partition key expr: " + partKey);
    }

    List<ExprNodeDesc> keyExprs = new ArrayList<ExprNodeDesc>();
    keyExprs.add(key);

    // group by requires "ArrayList", don't ask.
    ArrayList<String> outputNames = new ArrayList<String>();
    outputNames.add(HiveConf.getColumnInternalName(0));

    // project the relevant key column
    SelectDesc select = new SelectDesc(keyExprs, outputNames);
    SelectOperator selectOp =
        (SelectOperator) OperatorFactory.getAndMakeChild(select, parentOfRS);

    // do a group by on the list to dedup
    float groupByMemoryUsage =
        HiveConf.getFloatVar(parseContext.getConf(), HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY);
    float memoryThreshold =
        HiveConf.getFloatVar(parseContext.getConf(),
            HiveConf.ConfVars.HIVEMAPAGGRMEMORYTHRESHOLD);

    ArrayList<ExprNodeDesc> groupByExprs = new ArrayList<ExprNodeDesc>();
    ExprNodeDesc groupByExpr =
        new ExprNodeColumnDesc(key.getTypeInfo(), outputNames.get(0), null, false);
    groupByExprs.add(groupByExpr);

    GroupByDesc groupBy =
        new GroupByDesc(GroupByDesc.Mode.HASH, outputNames, groupByExprs,
            new ArrayList<AggregationDesc>(), false, groupByMemoryUsage, memoryThreshold,
            null, false, 0, true);

    GroupByOperator groupByOp =
        (GroupByOperator) OperatorFactory.getAndMakeChild(groupBy, selectOp);

    Map<String, ExprNodeDesc> colMap = new HashMap<String, ExprNodeDesc>();
    colMap.put(outputNames.get(0), groupByExpr);
    groupByOp.setColumnExprMap(colMap);

    // finally add the event broadcast operator
    DynamicPruningEventDesc eventDesc = new DynamicPruningEventDesc();
    eventDesc.setTableScan(ts);
    eventDesc.setTable(PlanUtils.getReduceValueTableDesc(PlanUtils
        .getFieldSchemasFromColumnList(keyExprs, "key")));
    eventDesc.setTargetColumnName(column);
    eventDesc.setPartKey(partKey);

    OperatorFactory.getAndMakeChild(eventDesc, groupByOp);
  }

  private Map<Node, Object> walkExprTree(ExprNodeDesc pred, NodeProcessorCtx ctx)
      throws SemanticException {

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
    exprRules.put(new RuleRegExp("R1", ExprNodeDynamicListDesc.class.getName() + "%"),
        new DynamicPartitionPrunerProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, exprRules, ctx);
    GraphWalker egw = new DefaultGraphWalker(disp);

    List<Node> startNodes = new ArrayList<Node>();
    startNodes.add(pred);

    HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
    egw.startWalking(startNodes, outputMap);
    return outputMap;
  }

}
