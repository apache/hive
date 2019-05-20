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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
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
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;

/**
 * This transformation does group by optimization. If the grouping key is a superset
 * of the bucketing and sorting keys of the underlying table in the same order, the
 * group by can be be performed on the map-side completely.
 */
public class GroupByOptimizer extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(GroupByOptimizer.class
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
    List<Node> topNodes = new ArrayList<Node>();
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

  private enum ColumnOrderMatch {
    NO_MATCH, PREFIX_COL1_MATCH, PREFIX_COL2_MATCH, COMPLETE_MATCH
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
      GroupByDesc groupByOpDesc = groupByOp.getConf();

      boolean removeReduceSink = false;
      boolean optimizeDistincts = false;
      boolean setBucketGroup = false;

      // Dont remove the operator for distincts
      if (useMapperSort &&
          (match == GroupByOptimizerSortMatch.COMPLETE_MATCH)) {
        if (!groupByOpDesc.isDistinct()) {
          removeReduceSink = true;
        }
        else if (!HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEGROUPBYSKEW)) {
          // Optimize the query: select count(distinct keys) from T, where
          // T is bucketized and sorted by T
          // Partial aggregation can be done by the mappers in this scenario

          List<ExprNodeDesc> keys =
              ((GroupByOperator)
              (groupByOp.getChildOperators().get(0).getChildOperators().get(0)))
                  .getConf().getKeys();
          if ((keys == null) || (keys.isEmpty())) {
            optimizeDistincts = true;
          }
        }
      }

      if ((match == GroupByOptimizerSortMatch.PARTIAL_MATCH) ||
          (match == GroupByOptimizerSortMatch.COMPLETE_MATCH)) {
        setBucketGroup = true;
      }

      if (removeReduceSink) {
        convertGroupByMapSideSortedGroupBy(hiveConf, groupByOp, depth);
      }
      else if (optimizeDistincts && !HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED)) {
        pGraphContext.getQueryProperties().setHasMapGroupBy(true);
        ReduceSinkOperator reduceSinkOp =
            (ReduceSinkOperator)groupByOp.getChildOperators().get(0);
        GroupByDesc childGroupByDesc =
            ((GroupByOperator)
            (reduceSinkOp.getChildOperators().get(0))).getConf();

        for (int pos = 0; pos < childGroupByDesc.getAggregators().size(); pos++) {
          AggregationDesc aggr = childGroupByDesc.getAggregators().get(pos);
          // Partial aggregation is not done for distincts on the mapper
          // However, if the data is bucketed/sorted on the distinct key, partial aggregation
          // can be performed on the mapper.
          if (aggr.getDistinct()) {
            ArrayList<ExprNodeDesc> parameters = new ArrayList<ExprNodeDesc>();
            ExprNodeDesc param = aggr.getParameters().get(0);
            assert param instanceof ExprNodeColumnDesc;
            ExprNodeColumnDesc paramC = (ExprNodeColumnDesc) param;
            paramC.setIsPartitionColOrVirtualCol(false);
            paramC.setColumn("VALUE._col" + pos);
            parameters.add(paramC);
            aggr.setParameters(parameters);
            aggr.setDistinct(false);
            aggr.setMode(Mode.FINAL);
          }
        }
        // Partial aggregation is performed on the mapper, no distinct processing at the reducer
        childGroupByDesc.setDistinct(false);
        groupByOpDesc.setDontResetAggrsDistinct(true);
        groupByOpDesc.setBucketGroup(true);
        groupByOp.setUseBucketizedHiveInputFormat(true);
        // no distinct processing at the reducer
        // A query like 'select count(distinct key) from T' is transformed into
        // 'select count(key) from T' as far as the reducer is concerned.
        reduceSinkOp.getConf().setDistinctColumnIndices(new ArrayList<List<Integer>>());
      }
      else if (setBucketGroup) {
        groupByOpDesc.setBucketGroup(true);
      }
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // GBY,RS,GBY... (top to bottom)
      GroupByOperator groupByOp = (GroupByOperator) stack.get(stack.size() - 3);

      GroupByOptimizerContext ctx = (GroupByOptimizerContext) procCtx;

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
        if ((currOp.getParentOperators() == null) || (currOp.getParentOperators().isEmpty())) {
          break;
        }

        if ((currOp.getParentOperators().size() > 1) ||
            (!currOp.columnNamesRowResolvedCanBeObtained())) {
          return GroupByOptimizerSortMatch.NO_MATCH;
        }

        currOp = currOp.getParentOperators().get(0);
      }

      // currOp now points to the top-most tablescan operator
      TableScanOperator tableScanOp = (TableScanOperator) currOp;
      int stackPos = 0;
      assert stack.get(0) == tableScanOp;

      // Create a mapping from the group by columns to the table columns
      Map<String, String> tableColsMapping = new HashMap<String, String>();
      Set<String> constantCols = new HashSet<String>();
      Table table = tableScanOp.getConf().getTableMetadata();
      for (FieldSchema col : table.getAllCols()) {
        tableColsMapping.put(col.getName(), col.getName());
      }

      while (currOp != groupByOp) {
        Operator<? extends OperatorDesc> processOp = currOp;
        Set<String> newConstantCols = new HashSet<String>();
        currOp = (Operator<? extends OperatorDesc>) (stack.get(++stackPos));

        // Filters don't change the column names - so, no need to do anything for them
        if (processOp instanceof SelectOperator) {
          SelectOperator selectOp = (SelectOperator) processOp;
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

            ExprNodeDesc selectCol = selectDesc.getColList().get(pos);
            if (selectCol instanceof ExprNodeColumnDesc) {
              String newValue =
                  tableColsMapping.get(((ExprNodeColumnDesc) selectCol).getColumn());
              tableColsMapping.put(outputColumnName, newValue);
            }
            else {
              tableColsMapping.remove(outputColumnName);
              if (selectCol instanceof ExprNodeConstantDesc) {
                // Lets see if this constant was folded because of optimization.
                String origCol = ((ExprNodeConstantDesc) selectCol).getFoldedFromCol();
                if (origCol != null) {
                  tableColsMapping.put(outputColumnName, origCol);
                } else {
                  newConstantCols.add(outputColumnName);
                }
              }
            }
          }

          constantCols = newConstantCols;
        }
      }

      // compute groupby columns from groupby keys
      List<String> groupByCols = new ArrayList<String>();
      // If the group by expression is anything other than a list of columns,
      // the sorting property is not obeyed
      for (ExprNodeDesc expr : groupByOp.getConf().getKeys()) {
        if (expr instanceof ExprNodeColumnDesc) {
          String groupByKeyColumn = ((ExprNodeColumnDesc) expr).getColumn();
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
        else if (expr instanceof ExprNodeConstantDesc) {
          continue;
        } else {
          return GroupByOptimizerSortMatch.NO_MATCH;
        }
      }

      if (!table.isPartitioned()) {
        List<String> sortCols = Utilities.getColumnNamesFromSortCols(table.getSortCols());
        List<String> bucketCols = table.getBucketCols();
        return matchBucketSortCols(groupByCols, bucketCols, sortCols);
      } else {
        PrunedPartitionList partsList =
            pGraphContext.getPrunedPartitions(table.getTableName(), tableScanOp);

        List<Partition> notDeniedPartns = partsList.getNotDeniedPartns();

        GroupByOptimizerSortMatch currentMatch =
            notDeniedPartns.isEmpty() ? GroupByOptimizerSortMatch.NO_MATCH :
                notDeniedPartns.size() > 1 ? GroupByOptimizerSortMatch.PARTIAL_MATCH :
                    GroupByOptimizerSortMatch.COMPLETE_MATCH;
        for (Partition part : notDeniedPartns) {
          List<String> sortCols = part.getSortColNames();
          List<String> bucketCols = part.getBucketCols();
          GroupByOptimizerSortMatch match = matchBucketSortCols(groupByCols, bucketCols, sortCols);
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

    /*
     * Return how the list of columns passed in match.
     * Return NO_MATCH if either of the list is empty or null, or if there is a mismatch.
     * For eg: ([], []), ([], ["a"]), (["a"],["b"]) and (["a", "b"], ["a","c"]) return NO_MATCH
     *
     * Return COMPLETE_MATCH if both the lists are non-empty and are same
     * Return PREFIX_COL1_MATCH if list1 is a strict subset of list2 and
     * return PREFIX_COL2_MATCH if list2 is a strict subset of list1
     *
     * For eg: (["a"], ["a"]), (["a"], ["a", "b"]) and (["a", "b"], ["a"]) return
     * COMPLETE_MATCH, PREFIX_COL1_MATCH and PREFIX_COL2_MATCH respectively.
     */
    private ColumnOrderMatch matchColumnOrder(List<String> cols1, List<String> cols2) {
      int numCols1 = cols1 == null ? 0 : cols1.size();
      int numCols2 = cols2 == null ? 0 : cols2.size();

      if (numCols1 == 0 || numCols2 == 0) {
        return ColumnOrderMatch.NO_MATCH;
      }

      for (int pos = 0; pos < Math.min(numCols1, numCols2); pos++) {
        if (!cols1.get(pos).equals(cols2.get(pos))) {
          return ColumnOrderMatch.NO_MATCH;
        }
      }

      return (numCols1 == numCols2) ?
          ColumnOrderMatch.COMPLETE_MATCH :
          ((numCols1 < numCols2) ? ColumnOrderMatch.PREFIX_COL1_MATCH :
              ColumnOrderMatch.PREFIX_COL2_MATCH);
    }

    /**
     * Given the group by keys, bucket columns and sort columns, this method
     * determines if we can use sorted group by or not.
     *
     * @param groupByCols
     * @param bucketCols
     * @param sortCols
     * @return
     * @throws SemanticException
     */
    private GroupByOptimizerSortMatch matchBucketSortCols(
        List<String> groupByCols,
        List<String> bucketCols,
        List<String> sortCols) throws SemanticException {

      /*
       * >> Super set of
       * If the grouping columns are a,b,c and the sorting columns are a,b
       * grouping columns >> sorting columns
       * (or grouping columns are a superset of sorting columns)
       *
       * Similarly << means subset of
       *
       * No intersection between Sort Columns and BucketCols:
       *
       * 1. Sort Cols = Group By Cols ---> Partial Match
       * 2. Group By Cols >> Sort By Cols --> No Match
       * 3. Group By Cols << Sort By Cols --> Partial Match
       *
       * BucketCols <= SortCols (bucket columns is either same or a prefix of sort columns)
       *
       * 1. Sort Cols = Group By Cols ---> Complete Match
       * 2. Group By Cols >> Sort By Cols --> No Match
       * 3. Group By Cols << Sort By Cols --> Complete Match if Group By Cols >= BucketCols
       * --> Partial Match otherwise
       *
       * BucketCols >> SortCols (bucket columns is a superset of sorting columns)
       *
       * 1. group by cols <= sort cols --> partial match
       * 2. group by cols >> sort cols --> no match
       *
       * One exception to this rule is:
       * If GroupByCols == SortCols and all bucketing columns are part of sorting columns
       * (in any order), it is a complete match
       */
      ColumnOrderMatch bucketSortColsMatch = matchColumnOrder(bucketCols, sortCols);
      ColumnOrderMatch sortGroupByColsMatch = matchColumnOrder(sortCols, groupByCols);
      switch (sortGroupByColsMatch) {
      case NO_MATCH:
        return GroupByOptimizerSortMatch.NO_MATCH;
      case COMPLETE_MATCH:
        return ((bucketCols != null) && !bucketCols.isEmpty() &&
            sortCols.containsAll(bucketCols)) ?
            GroupByOptimizerSortMatch.COMPLETE_MATCH : GroupByOptimizerSortMatch.PARTIAL_MATCH;
      case PREFIX_COL1_MATCH:
        return GroupByOptimizerSortMatch.NO_MATCH;
      case PREFIX_COL2_MATCH:
        return ((bucketSortColsMatch == ColumnOrderMatch.NO_MATCH) ||
            (bucketCols.size() > groupByCols.size())) ?
            GroupByOptimizerSortMatch.PARTIAL_MATCH :
            GroupByOptimizerSortMatch.COMPLETE_MATCH;
      }
      return GroupByOptimizerSortMatch.NO_MATCH;
    }

    // Convert the group by to a map-side group by
    // The operators specified by depth and removed from the tree.
    protected void convertGroupByMapSideSortedGroupBy(
        HiveConf conf, GroupByOperator groupByOp, int depth) {
      pGraphContext.getQueryProperties().setHasMapGroupBy(true);

      if (removeChildren(groupByOp, depth)) {
        // Use bucketized hive input format - that makes sure that one mapper reads the entire file
        groupByOp.setUseBucketizedHiveInputFormat(true);
        groupByOp.getConf().setMode(GroupByDesc.Mode.FINAL);
      }
    }

    // Remove the operators till a certain depth.
    // Return true if the remove was successful, false otherwise
    public boolean removeChildren(Operator<? extends OperatorDesc> currOp, int depth) {
      Operator<? extends OperatorDesc> inputOp = currOp;
      for (int i = 0; i < depth; i++) {
        // If there are more than 1 children at any level, don't do anything
        if ((currOp.getChildOperators() == null) || (currOp.getChildOperators().isEmpty())
            || (currOp.getChildOperators().size() > 1)) {
          return false;
        }
        currOp = currOp.getChildOperators().get(0);
      }

      // add selectOp to match the schema
      // after that, inputOp is the parent of selOp.
      for (Operator<? extends OperatorDesc> op : inputOp.getChildOperators()) {
        op.getParentOperators().clear();
      }
      inputOp.getChildOperators().clear();
      Operator<? extends OperatorDesc> selOp = genOutputSelectForGroupBy(inputOp, currOp);

      // update the childOp of selectOp
      selOp.setChildOperators(currOp.getChildOperators());

      // update the parentOp
      for (Operator<? extends OperatorDesc> op : currOp.getChildOperators()) {
        op.replaceParent(currOp, selOp);
      }
      return true;
    }

    private Operator<? extends OperatorDesc> genOutputSelectForGroupBy(
        Operator<? extends OperatorDesc> parentOp, Operator<? extends OperatorDesc> currOp) {
      assert (parentOp.getSchema().getSignature().size() == currOp.getSchema().getSignature().size());
      Iterator<ColumnInfo> pIter = parentOp.getSchema().getSignature().iterator();
      Iterator<ColumnInfo> cIter = currOp.getSchema().getSignature().iterator();
      List<ExprNodeDesc> columns = new ArrayList<ExprNodeDesc>();
      List<String> colName = new ArrayList<String>();
      Map<String, ExprNodeDesc> columnExprMap = new HashMap<String, ExprNodeDesc>();
      while (pIter.hasNext()) {
        ColumnInfo pInfo = pIter.next();
        ColumnInfo cInfo = cIter.next();
        ExprNodeDesc column = new ExprNodeColumnDesc(pInfo.getType(), pInfo.getInternalName(),
            pInfo.getTabAlias(), pInfo.getIsVirtualCol(), pInfo.isSkewedCol());
        columns.add(column);
        colName.add(cInfo.getInternalName());
        columnExprMap.put(cInfo.getInternalName(), column);
      }
      return OperatorFactory.getAndMakeChild(new SelectDesc(columns, colName),
          new RowSchema(currOp.getSchema().getSignature()), columnExprMap, parentOp);
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
      GroupByOptimizerContext ctx = (GroupByOptimizerContext) procCtx;

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
