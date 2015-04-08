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

package org.apache.hadoop.hive.ql.optimizer.metainfo.annotation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.AbstractBucketJoinProc;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OpTraits;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/*
 * This class populates the following operator traits for the entire operator tree:
 * 1. Bucketing columns.
 * 2. Table
 * 3. Pruned partitions
 *
 * Bucketing columns refer to not to the bucketing columns from the table object but instead
 * to the dynamic 'bucketing' done by operators such as reduce sinks and group-bys.
 * All the operators have a translation from their input names to the output names corresponding
 * to the bucketing column. The colExprMap that is a part of every operator is used in this
 * transformation.
 *
 * The table object is used for the base-case in map-reduce when deciding to perform a bucket
 * map join. This object is used in the BucketMapJoinProc to find if number of files for the
 * table correspond to the number of buckets specified in the meta data.
 *
 * The pruned partition information has the same purpose as the table object at the moment.
 *
 * The traits of sorted-ness etc. can be populated as well for future optimizations to make use of.
 */

public class OpTraitsRulesProcFactory {

  public static class DefaultRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>)nd;
      op.setOpTraits(op.getParentOperators().get(0).getOpTraits());
      return null;
    }

  }

  /*
   * Reduce sink operator is the de-facto operator
   * for determining keyCols (emit keys of a map phase)
   */
  public static class ReduceSinkRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      ReduceSinkOperator rs = (ReduceSinkOperator)nd;
      List<String> bucketCols = new ArrayList<String>();
      if (rs.getColumnExprMap() != null) {
        for (ExprNodeDesc exprDesc : rs.getConf().getKeyCols()) {
          for (Entry<String, ExprNodeDesc> entry : rs.getColumnExprMap().entrySet()) {
            if (exprDesc.isSame(entry.getValue())) {
              bucketCols.add(entry.getKey());
            }
          }
        }
      }

      List<List<String>> listBucketCols = new ArrayList<List<String>>();
      listBucketCols.add(bucketCols);
      int numBuckets = -1;
      OpTraits parentOpTraits = rs.getParentOperators().get(0).getConf().getTraits();
      if (parentOpTraits != null) {
        numBuckets = parentOpTraits.getNumBuckets();
      }
      OpTraits opTraits = new OpTraits(listBucketCols, numBuckets, listBucketCols);
      rs.setOpTraits(opTraits);
      return null;
    }
  }

  /*
   * Table scan has the table object and pruned partitions that has information
   * such as bucketing, sorting, etc. that is used later for optimization.
   */
  public static class TableScanRule implements NodeProcessor {

    public boolean checkBucketedTable(Table tbl, ParseContext pGraphContext,
        PrunedPartitionList prunedParts) throws SemanticException {

      if (tbl.isPartitioned()) {
        List<Partition> partitions = prunedParts.getNotDeniedPartns();
        // construct a mapping of (Partition->bucket file names) and (Partition -> bucket number)
        if (!partitions.isEmpty()) {
          for (Partition p : partitions) {
            List<String> fileNames =
                AbstractBucketJoinProc.getBucketFilePathsOfPartition(p.getDataLocation(),
                    pGraphContext);
            // The number of files for the table should be same as number of
            // buckets.
            int bucketCount = p.getBucketCount();

            if (fileNames.size() != 0 && fileNames.size() != bucketCount) {
              return false;
            }
          }
        }
      } else {

        List<String> fileNames =
            AbstractBucketJoinProc.getBucketFilePathsOfPartition(tbl.getDataLocation(),
                pGraphContext);
        Integer num = new Integer(tbl.getNumBuckets());

        // The number of files for the table should be same as number of buckets.
        if (fileNames.size() != 0 && fileNames.size() != num) {
          return false;
        }
      }

      return true;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator ts = (TableScanOperator)nd;
      AnnotateOpTraitsProcCtx opTraitsCtx = (AnnotateOpTraitsProcCtx)procCtx;
      Table table = ts.getConf().getTableMetadata();
      PrunedPartitionList prunedPartList = null;
      try {
        prunedPartList =
            opTraitsCtx.getParseContext().getPrunedPartitions(ts.getConf().getAlias(), ts);
      } catch (HiveException e) {
        prunedPartList = null;
      }
      boolean isBucketed = checkBucketedTable(table,
          opTraitsCtx.getParseContext(), prunedPartList);
      List<List<String>> bucketColsList = new ArrayList<List<String>>();
      List<List<String>> sortedColsList = new ArrayList<List<String>>();
      int numBuckets = -1;
      if (isBucketed) {
        bucketColsList.add(table.getBucketCols());
        numBuckets = table.getNumBuckets();
        List<String> sortCols = new ArrayList<String>();
        for (Order colSortOrder : table.getSortCols()) {
          sortCols.add(colSortOrder.getCol());
        }
        sortedColsList.add(sortCols);
      }
      // num reduce sinks hardcoded to 0 because TS has no parents
      OpTraits opTraits = new OpTraits(bucketColsList, numBuckets, sortedColsList);
      ts.setOpTraits(opTraits);
      return null;
    }
  }

  /*
   * Group-by re-orders the keys emitted hence, the keyCols would change.
   */
  public static class GroupByRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator gbyOp = (GroupByOperator)nd;
      List<String> gbyKeys = new ArrayList<String>();
      for (ExprNodeDesc exprDesc : gbyOp.getConf().getKeys()) {
        for (Entry<String, ExprNodeDesc> entry : gbyOp.getColumnExprMap().entrySet()) {
          if (exprDesc.isSame(entry.getValue())) {
            gbyKeys.add(entry.getKey());
          }
        }
      }

      List<List<String>> listBucketCols = new ArrayList<List<String>>();
      listBucketCols.add(gbyKeys);
      OpTraits opTraits = new OpTraits(listBucketCols, -1, listBucketCols);
      gbyOp.setOpTraits(opTraits);
      return null;
    }
  }

  public static class SelectRule implements NodeProcessor {

    public List<List<String>> getConvertedColNames(
        List<List<String>> parentColNames, SelectOperator selOp) {
      List<List<String>> listBucketCols = new ArrayList<List<String>>();
      if (selOp.getColumnExprMap() != null) {
        if (parentColNames != null) {
          for (List<String> colNames : parentColNames) {
            List<String> bucketColNames = new ArrayList<String>();
            for (String colName : colNames) {
              for (Entry<String, ExprNodeDesc> entry : selOp.getColumnExprMap().entrySet()) {
                if (entry.getValue() instanceof ExprNodeColumnDesc) {
                  if (((ExprNodeColumnDesc) (entry.getValue())).getColumn().equals(colName)) {
                    bucketColNames.add(entry.getKey());
                  }
                }
              }
            }
            listBucketCols.add(bucketColNames);
          }
        }
      }

      return listBucketCols;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator selOp = (SelectOperator) nd;
      List<List<String>> parentBucketColNames =
          selOp.getParentOperators().get(0).getOpTraits().getBucketColNames();

      List<List<String>> listBucketCols = null;
      List<List<String>> listSortCols = null;
      if (selOp.getColumnExprMap() != null) {
        if (parentBucketColNames != null) {
          listBucketCols = getConvertedColNames(parentBucketColNames, selOp);
        }
        List<List<String>> parentSortColNames =
            selOp.getParentOperators().get(0).getOpTraits().getSortCols();
        if (parentSortColNames != null) {
          listSortCols = getConvertedColNames(parentSortColNames, selOp);
        }
      }

      int numBuckets = -1;
      OpTraits parentOpTraits = selOp.getParentOperators().get(0).getOpTraits();
      if (parentOpTraits != null) {
        numBuckets = parentOpTraits.getNumBuckets();
      }
      OpTraits opTraits = new OpTraits(listBucketCols, numBuckets, listSortCols);
      selOp.setOpTraits(opTraits);
      return null;
    }
  }

  public static class JoinRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      JoinOperator joinOp = (JoinOperator) nd;
      List<List<String>> bucketColsList = new ArrayList<List<String>>();
      List<List<String>> sortColsList = new ArrayList<List<String>>();
      byte pos = 0;
      int numReduceSinks = 0; // will be set to the larger of the parents
      for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
        if (!(parentOp instanceof ReduceSinkOperator)) {
          // can be mux operator
          break;
        }
        ReduceSinkOperator rsOp = (ReduceSinkOperator) parentOp;
        if (rsOp.getOpTraits() == null) {
          ReduceSinkRule rsRule = new ReduceSinkRule();
          rsRule.process(rsOp, stack, procCtx, nodeOutputs);
        }
        OpTraits parentOpTraits = rsOp.getOpTraits();
        bucketColsList.add(getOutputColNames(joinOp, parentOpTraits.getBucketColNames(), pos));
        sortColsList.add(getOutputColNames(joinOp, parentOpTraits.getSortCols(), pos));
        pos++;
      }

      joinOp.setOpTraits(new OpTraits(bucketColsList, -1, bucketColsList));
      return null;
    }

    private List<String> getOutputColNames(JoinOperator joinOp, List<List<String>> parentColNames,
        byte pos) {
      if (parentColNames != null) {
        List<String> bucketColNames = new ArrayList<String>();

        // guaranteed that there is only 1 list within this list because
        // a reduce sink always brings down the bucketing cols to a single list.
        // may not be true with correlation operators (mux-demux)
        List<String> colNames = parentColNames.get(0);
        for (String colName : colNames) {
          for (ExprNodeDesc exprNode : joinOp.getConf().getExprs().get(pos)) {
            if (exprNode instanceof ExprNodeColumnDesc) {
              if (((ExprNodeColumnDesc) (exprNode)).getColumn().equals(colName)) {
                for (Entry<String, ExprNodeDesc> entry : joinOp.getColumnExprMap().entrySet()) {
                  if (entry.getValue().isSame(exprNode)) {
                    bucketColNames.add(entry.getKey());
                    // we have found the colName
                    break;
                  }
                }
              } else {
                // continue on to the next exprNode to find a match
                continue;
              }
              // we have found the colName. No need to search more exprNodes.
              break;
            }
          }
        }

        return bucketColNames;
      }

      // no col names in parent
      return null;
    }
  }

  /*
   * When we have operators that have multiple parents, it is not clear which
   * parent's traits we need to propagate forward.
   */
  public static class MultiParentRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> operator = (Operator<? extends OperatorDesc>) nd;
      OpTraits opTraits = new OpTraits(null, -1, null);
      operator.setOpTraits(opTraits);
      return null;
    }
  }

  public static NodeProcessor getTableScanRule() {
    return new TableScanRule();
  }

  public static NodeProcessor getReduceSinkRule() {
    return new ReduceSinkRule();
  }

  public static NodeProcessor getSelectRule() {
    return new SelectRule();
  }

  public static NodeProcessor getDefaultRule() {
    return new DefaultRule();
  }

  public static NodeProcessor getMultiParentRule() {
    return new MultiParentRule();
  }

  public static NodeProcessor getGroupByRule() {
    return new GroupByRule();
  }

  public static NodeProcessor getJoinRule() {
    return new JoinRule();
  }
}
