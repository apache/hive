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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
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
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

/**
 *this transformation does bucket group by optimization.
 */
public class GroupByOptimizer implements Transform {

  private static final Log LOG = LogFactory.getLog(GroupByOptimizer.class
      .getName());

  public GroupByOptimizer() {
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    GroupByOptProcCtx groupByOptimizeCtx = new GroupByOptProcCtx();

    // process group-by pattern
    opRules.put(new RuleRegExp("R1", "GBY%RS%GBY%"),
        getMapAggreSortedGroupbyProc(pctx));

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules,
        groupByOptimizeCtx);
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

  private NodeProcessor getMapAggreSortedGroupbyProc(ParseContext pctx) {
    return new BucketGroupByProcessor(pctx);
  }

  /**
   * BucketGroupByProcessor.
   *
   */
  public class BucketGroupByProcessor implements NodeProcessor {

    protected ParseContext pGraphContext;

    public BucketGroupByProcessor(ParseContext pGraphContext) {
      this.pGraphContext = pGraphContext;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      // GBY,RS,GBY... (top to bottom)
      GroupByOperator op = (GroupByOperator) stack.get(stack.size() - 3);
      checkBucketGroupBy(op);
      return null;
    }

    private void checkBucketGroupBy(GroupByOperator curr)
        throws SemanticException {

      // if this is not a HASH groupby, return
      if (curr.getConf().getMode() != GroupByDesc.Mode.HASH) {
        return;
      }

      Set<String> tblNames = pGraphContext.getGroupOpToInputTables().get(curr);
      if (tblNames == null || tblNames.size() == 0) {
        return;
      }

      boolean bucketGroupBy = true;
      GroupByDesc desc = curr.getConf();
      List<ExprNodeDesc> groupByKeys = new LinkedList<ExprNodeDesc>();
      groupByKeys.addAll(desc.getKeys());
      // compute groupby columns from groupby keys
      List<String> groupByCols = new ArrayList<String>();
      while (groupByKeys.size() > 0) {
        ExprNodeDesc node = groupByKeys.remove(0);
        if (node instanceof ExprNodeColumnDesc) {
          groupByCols.addAll(node.getCols());
        } else if ((node instanceof ExprNodeConstantDesc)
            || (node instanceof ExprNodeNullDesc)) {
          // nothing
        } else if (node instanceof ExprNodeFieldDesc) {
          groupByKeys.add(0, ((ExprNodeFieldDesc) node).getDesc());
          continue;
        } else if (node instanceof ExprNodeGenericFuncDesc) {
          ExprNodeGenericFuncDesc udfNode = ((ExprNodeGenericFuncDesc) node);
          GenericUDF udf = udfNode.getGenericUDF();
          if (!FunctionRegistry.isDeterministic(udf)) {
            return;
          }
          groupByKeys.addAll(0, udfNode.getChildExprs());
        } else {
          return;
        }
      }

      if (groupByCols.size() == 0) {
        return;
      }

      for (String table : tblNames) {
        Operator<? extends Serializable> topOp = pGraphContext.getTopOps().get(
            table);
        if (topOp == null || (!(topOp instanceof TableScanOperator))) {
          // this is in a sub-query.
          // In future, we need to infer subq's columns propery. For example
          // "select key, count(1) 
          // from (from clustergroupbyselect key, value where ds='210') group by key, 3;",
          // even though the group by op is in a subquery, it can be changed to
          // bucket groupby.
          return;
        }
        TableScanOperator ts = (TableScanOperator) topOp;
        Table destTable = pGraphContext.getTopToTable().get(ts);
        if (destTable == null) {
          return;
        }
        if (!destTable.isPartitioned()) {
          List<String> bucketCols = destTable.getBucketCols();
          List<String> sortCols = Utilities
              .getColumnNamesFromSortCols(destTable.getSortCols());
          bucketGroupBy = matchBucketOrSortedColumns(groupByCols, bucketCols,
              sortCols);
          if (!bucketGroupBy) {
            return;
          }
        } else {
          PrunedPartitionList partsList = null;
          try {
            partsList = PartitionPruner.prune(destTable, pGraphContext
                .getOpToPartPruner().get(ts), pGraphContext.getConf(), table,
                pGraphContext.getPrunedPartitions());
          } catch (HiveException e) {
            // Has to use full name to make sure it does not conflict with
            // org.apache.commons.lang.StringUtils
            LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
            throw new SemanticException(e.getMessage(), e);
          }
          List<Partition> parts = new ArrayList<Partition>();
          parts.addAll(partsList.getConfirmedPartns());
          parts.addAll(partsList.getUnknownPartns());
          for (Partition part : parts) {
            List<String> bucketCols = part.getBucketCols();
            List<String> sortCols = part.getSortColNames();
            bucketGroupBy = matchBucketOrSortedColumns(groupByCols, bucketCols,
                sortCols);
            if (!bucketGroupBy) {
              return;
            }
          }
        }
      }

      curr.getConf().setBucketGroup(bucketGroupBy);
    }

    /**
     * Given the group by keys, bucket columns, sort column, this method
     * determines if we can use sorted group by or not.
     * 
     * We use bucket columns only when the sorted column set is empty and if all
     * group by columns are contained in bucket columns.
     * 
     * If we can can not determine by looking at bucketed columns and the table
     * has sort columns, we resort to sort columns. We can use bucket group by
     * if the groupby column set is an exact prefix match of sort columns.
     * 
     * @param groupByCols
     * @param bucketCols
     * @param sortCols
     * @return
     * @throws SemanticException
     */
    private boolean matchBucketOrSortedColumns(List<String> groupByCols,
        List<String> bucketCols, List<String> sortCols) throws SemanticException {
      boolean ret = false;

      if (sortCols == null || sortCols.size() == 0) {
        ret = matchBucketColumns(groupByCols, bucketCols);
      }

      if (!ret && sortCols != null && sortCols.size() >= groupByCols.size()) {
        // check sort columns, if groupByCols is a prefix subset of sort
        // columns, we will use sorted group by. For example, if data is sorted
        // by column a, b, c, and a query wants to group by b,a, we will use
        // sorted group by. But if the query wants to groupby b,c, then sorted
        // group by can not be used.
        int num = groupByCols.size();
        for (int i = 0; i < num; i++) {
          if (sortCols.indexOf(groupByCols.get(i)) > (num - 1)) {
            return false;
          }
        }
        return true;
      }

      return ret;
    }

    /*
     * All group by columns should be contained in the bucket column set. And
     * the number of group by columns should be equal to number of bucket
     * columns.
     */
    private boolean matchBucketColumns(List<String> grpCols,
        List<String> tblBucketCols) throws SemanticException {

      if (tblBucketCols == null || tblBucketCols.size() == 0
          || grpCols.size() == 0 || grpCols.size() != tblBucketCols.size()) {
        return false;
      }

      for (int i = 0; i < grpCols.size(); i++) {
        String tblCol = grpCols.get(i);
        if (!tblBucketCols.contains(tblCol)) {
          return false;
        }
      }
      return true;
    }
  }

  /**
   * GroupByOptProcCtx.
   *
   */
  public class GroupByOptProcCtx implements NodeProcessorCtx {
  }
}
