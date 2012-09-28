/**
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.util.StringUtils;

/**
 * This transformation does group by optimization. If the grouping key is a superset
 * of the bucketing and sorting keys of the underlying table in the same order, the
 * group by can be be performed on the map-side completely.
 */
public class GroupByOptimizer implements Transform {

  private static final Log LOG = LogFactory.getLog(GroupByOptimizer.class
      .getName());

  public GroupByOptimizer() {
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    HiveConf conf = pctx.getConf();

    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVEGROUPBYSKEW)) {
      // process group-by pattern
      opRules.put(new RuleRegExp("R1",
        GroupByOperator.getOperatorName() + "%" +
        ReduceSinkOperator.getOperatorName() + "%" +
        GroupByOperator.getOperatorName() + "%"),
        getMapSortedGroupbyProc(pctx));
    } else {
      // If hive.groupby.skewindata is set to true, the operator tree is as below
      opRules.put(new RuleRegExp("R2",
        GroupByOperator.getOperatorName() + "%" +
        ReduceSinkOperator.getOperatorName() + "%" +
        GroupByOperator.getOperatorName() + "%" +
        ReduceSinkOperator.getOperatorName() + "%" +
        GroupByOperator.getOperatorName() + "%"),
        getMapSortedGroupbySkewProc(pctx));
    }

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp =
      new DefaultRuleDispatcher(getDefaultProc(), opRules,
      new GroupByOptimizerContext(conf));
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pctx;
  }

  private NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack,
        NodeProcessorCtx procCtx, Object... nodeOutputs) throws SemanticException {
        return null;
      }
    };
  }

  private NodeProcessor getMapSortedGroupbyProc(ParseContext pctx) {
    return new SortGroupByProcessor(pctx);
  }

  private NodeProcessor getMapSortedGroupbySkewProc(ParseContext pctx) {
    return new SortGroupBySkewProcessor(pctx);
  }

  public enum GroupByOptimizerSortMatch {
    NO_MATCH, PARTIAL_MATCH, COMPLETE_MATCH
  };

  /**
   * SortGroupByProcessor.
   *
   */
  public class SortGroupByProcessor implements NodeProcessor {

    protected ParseContext pGraphContext;

    public SortGroupByProcessor(ParseContext pGraphContext) {
      this.pGraphContext = pGraphContext;
    }

    // Check if the group by operator has already been processed
    protected boolean checkGroupByOperatorProcessed(
      GroupByOptimizerContext groupBySortOptimizerContext,
      GroupByOperator groupByOp) {

      // The group by operator has already been processed
      if (groupBySortOptimizerContext.getListGroupByOperatorsProcessed().contains(groupByOp)) {
        return true;
      }

      groupBySortOptimizerContext.getListGroupByOperatorsProcessed().add(groupByOp);
      return false;
    }

    protected void processGroupBy(GroupByOptimizerContext ctx,
      Stack<Node> stack,
      GroupByOperator groupByOp,
      int depth) throws SemanticException {
      HiveConf hiveConf = ctx.getConf();
      GroupByOptimizerSortMatch match = checkSortGroupBy(stack, groupByOp);
      boolean useMapperSort =
        HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_MAP_GROUPBY_SORT);

      if (useMapperSort) {
        if (match == GroupByOptimizerSortMatch.COMPLETE_MATCH) {
          convertGroupByMapSideSortedGroupBy(groupByOp, depth);
        }
      }
      else if ((match == GroupByOptimizerSortMatch.PARTIAL_MATCH) ||
        (match == GroupByOptimizerSortMatch.COMPLETE_MATCH)) {
        groupByOp.getConf().setBucketGroup(true);
      }
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // GBY,RS,GBY... (top to bottom)
      GroupByOperator groupByOp = (GroupByOperator) stack.get(stack.size() - 3);

      GroupByOptimizerContext ctx = (GroupByOptimizerContext)procCtx;

      if (!checkGroupByOperatorProcessed(ctx, groupByOp)) {
        processGroupBy(ctx, stack, groupByOp, 2);
      }
      return null;
    }

    // Should this group by be converted to a map-side group by, because the grouping keys for
    // the base table for the group by matches the skewed keys
    protected GroupByOptimizerSortMatch checkSortGroupBy(Stack<Node> stack,
      GroupByOperator groupByOp)
      throws SemanticException {

      // if this is not a HASH groupby, return
      if (groupByOp.getConf().getMode() != GroupByDesc.Mode.HASH) {
        return GroupByOptimizerSortMatch.NO_MATCH;
      }

      // Check all the operators in the stack. Currently, only SELECTs and FILTERs
      // are allowed. A interface 'supportMapSideGroupBy has been added for the same
      Operator<? extends OperatorDesc> currOp = groupByOp;
      currOp = currOp.getParentOperators().get(0);

      while (true) {
        if (currOp.getParentOperators() == null) {
          break;
        }

        if ((currOp.getParentOperators().size() > 1) ||
            (!currOp.columnNamesRowResolvedCanBeObtained())) {
          return GroupByOptimizerSortMatch.NO_MATCH;
        }

        currOp = currOp.getParentOperators().get(0);
      }

      // currOp now points to the top-most tablescan operator
      TableScanOperator tableScanOp = (TableScanOperator)currOp;
      int stackPos = 0;
      assert stack.get(0) == tableScanOp;

      // Create a mapping from the group by columns to the table columns
      Map<String, String> tableColsMapping = new HashMap<String, String>();
      Set<String> constantCols = new HashSet<String>();
      Table table = pGraphContext.getTopToTable().get(currOp);
      for (FieldSchema col : table.getAllCols()) {
        tableColsMapping.put(col.getName(), col.getName());
      }

      while (currOp != groupByOp) {
        Operator<? extends OperatorDesc> processOp = currOp;
        Set<String> newConstantCols = new HashSet<String>();
        currOp = (Operator<? extends OperatorDesc>)(stack.get(++stackPos));

        // Filters don't change the column names - so, no need to do anything for them
        if (processOp instanceof SelectOperator) {
          SelectOperator selectOp = (SelectOperator)processOp;
          SelectDesc selectDesc = selectOp.getConf();

          if (selectDesc.isSelStarNoCompute()) {
            continue;
          }

          // Only columns and constants can be selected
          for (int pos = 0; pos < selectDesc.getColList().size(); pos++) {
            String outputColumnName = selectDesc.getOutputColumnNames().get(pos);
            if (constantCols.contains(outputColumnName)) {
              tableColsMapping.remove(outputColumnName);
              newConstantCols.add(outputColumnName);
              continue;
            }

            ExprNodeDesc selectColList = selectDesc.getColList().get(pos);
            if (selectColList instanceof ExprNodeColumnDesc) {
              String newValue =
                tableColsMapping.get(((ExprNodeColumnDesc) selectColList).getColumn());
              tableColsMapping.put(outputColumnName, newValue);
            }
            else {
              tableColsMapping.remove(outputColumnName);
              if ((selectColList instanceof ExprNodeConstantDesc) ||
                  (selectColList instanceof ExprNodeNullDesc)) {
                newConstantCols.add(outputColumnName);
              }
            }
          }

          constantCols = newConstantCols;
        }
      }

      boolean sortGroupBy = true;
      // compute groupby columns from groupby keys
      List<String> groupByCols = new ArrayList<String>();
      // If the group by expression is anything other than a list of columns,
      // the sorting property is not obeyed
      for (ExprNodeDesc expr : groupByOp.getConf().getKeys()) {
        if (expr instanceof ExprNodeColumnDesc) {
          String groupByKeyColumn = ((ExprNodeColumnDesc)expr).getColumn();
          // ignore if it is a constant
          if (constantCols.contains(groupByKeyColumn)) {
            continue;
          }
          else {
            if (tableColsMapping.containsKey(groupByKeyColumn)) {
              groupByCols.add(tableColsMapping.get(groupByKeyColumn));
            }
            else {
              return GroupByOptimizerSortMatch.NO_MATCH;
            }
          }
        }
        // Constants and nulls are OK
        else if ((expr instanceof ExprNodeConstantDesc) ||
          (expr instanceof ExprNodeNullDesc)) {
          continue;
        } else {
          return GroupByOptimizerSortMatch.NO_MATCH;
        }
      }

      if (!table.isPartitioned()) {
        List<String> sortCols = Utilities.getColumnNamesFromSortCols(table.getSortCols());
        return matchSortColumns(groupByCols, sortCols);
      } else {
        PrunedPartitionList partsList = null;
        try {
          partsList = pGraphContext.getOpToPartList().get(tableScanOp);
          if (partsList == null) {
            partsList = PartitionPruner.prune(table,
              pGraphContext.getOpToPartPruner().get(tableScanOp),
              pGraphContext.getConf(),
              table.getTableName(),
              pGraphContext.getPrunedPartitions());
            pGraphContext.getOpToPartList().put(tableScanOp, partsList);
          }
        } catch (HiveException e) {
          LOG.error(StringUtils.stringifyException(e));
          throw new SemanticException(e.getMessage(), e);
        }

        GroupByOptimizerSortMatch currentMatch = GroupByOptimizerSortMatch.COMPLETE_MATCH;
        for (Partition part : partsList.getNotDeniedPartns()) {
          List<String> sortCols = part.getSortColNames();
          GroupByOptimizerSortMatch match = matchSortColumns(groupByCols, sortCols);
          if (match == GroupByOptimizerSortMatch.NO_MATCH) {
            return match;
          }

          if (match == GroupByOptimizerSortMatch.PARTIAL_MATCH) {
            currentMatch = match;
          }
        }
        return currentMatch;
      }
    }

    /**
     * Given the group by keys, sort columns, this method
     * determines if we can use sorted group by or not.
     * We can use map-side sort group by group by columns match the sorted columns
     * in exactly the same order.
     *
     * @param groupByCols
     * @param sortCols
     * @return
     * @throws SemanticException
     */
    private GroupByOptimizerSortMatch matchSortColumns(
      List<String> groupByCols,
      List<String> sortCols) throws SemanticException {

      if (sortCols == null || sortCols.size() == 0) {
        return GroupByOptimizerSortMatch.NO_MATCH;
      }

      int num = sortCols.size() <  groupByCols.size() ? sortCols.size() : groupByCols.size();
      for (int i = 0; i < num; i++) {
        if (!sortCols.get(i).equals(groupByCols.get(i))) {
          return GroupByOptimizerSortMatch.NO_MATCH;
        }
      }

      return sortCols.size() == groupByCols.size() ?
        GroupByOptimizerSortMatch.COMPLETE_MATCH : GroupByOptimizerSortMatch.PARTIAL_MATCH;
    }

    // Convert the group by to a map-side group by
    // The operators specified by depth and removed from the tree.
    protected void convertGroupByMapSideSortedGroupBy(GroupByOperator groupByOp, int depth) {
      if (groupByOp.removeChildren(depth)) {
        // Use bucketized hive input format - that makes sure that one mapper reads the entire file
        groupByOp.setUseBucketizedHiveInputFormat(true);
        groupByOp.getConf().setMode(GroupByDesc.Mode.FINAL);
      }
    }
  }

  /**
   * SortGroupByProcessor.
   *
   */
  public class SortGroupBySkewProcessor extends SortGroupByProcessor {
    public SortGroupBySkewProcessor(ParseContext pGraphContext) {
      super(pGraphContext);
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // GBY,RS,GBY,RS,GBY... (top to bottom)
      GroupByOperator groupByOp = (GroupByOperator) stack.get(stack.size() - 5);
      GroupByOptimizerContext ctx = (GroupByOptimizerContext)procCtx;

      if (!checkGroupByOperatorProcessed(ctx, groupByOp)) {
        processGroupBy(ctx, stack, groupByOp, 4);
      }
      return null;
    }
  }

  public class GroupByOptimizerContext implements NodeProcessorCtx {
    List<GroupByOperator> listGroupByOperatorsProcessed;
    HiveConf conf;

    public GroupByOptimizerContext(HiveConf conf) {
      this.conf = conf;
      listGroupByOperatorsProcessed = new ArrayList<GroupByOperator>();
    }

    public List<GroupByOperator> getListGroupByOperatorsProcessed() {
      return listGroupByOperatorsProcessed;
    }

    public void setListGroupByOperatorsProcessed(
      List<GroupByOperator> listGroupByOperatorsProcessed) {
      this.listGroupByOperatorsProcessed = listGroupByOperatorsProcessed;
    }

    public HiveConf getConf() {
      return conf;
    }

    public void setConf(HiveConf conf) {
      this.conf = conf;
    }
  }
}
