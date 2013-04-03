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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
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
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

/**
 * This transformation does optimization for enforcing bucketing and sorting.
 * For a query of the form:
 * insert overwrite table T1 select * from T2;
 * where T1 and T2 are bucketized/sorted on the same keys, we don't need a reducer to
 * enforce bucketing and sorting.
 */
public class BucketingSortingReduceSinkOptimizer implements Transform {

  private static final Log LOG = LogFactory.getLog(BucketingSortingReduceSinkOptimizer.class
      .getName());

  public BucketingSortingReduceSinkOptimizer() {
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    HiveConf conf = pctx.getConf();

    // process reduce sink added by hive.enforce.bucketing or hive.enforce.sorting
    opRules.put(new RuleRegExp("R1",
        ReduceSinkOperator.getOperatorName() + "%" +
            ExtractOperator.getOperatorName() + "%" +
            FileSinkOperator.getOperatorName() + "%"),
        getBucketSortReduceSinkProc(pctx));

    // The dispatcher fires the processor corresponding to the closest matching rule
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, null);
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

  private NodeProcessor getBucketSortReduceSinkProc(ParseContext pctx) {
    return new BucketSortReduceSinkProcessor(pctx);
  }

  /**
   * BucketSortReduceSinkProcessor.
   *
   */
  public class BucketSortReduceSinkProcessor implements NodeProcessor {

    protected ParseContext pGraphContext;

    public BucketSortReduceSinkProcessor(ParseContext pGraphContext) {
      this.pGraphContext = pGraphContext;
    }

    // Get the bucket positions for the table
    private List<Integer> getBucketPositions(List<String> tabBucketCols,
        List<FieldSchema> tabCols) {
      List<Integer> posns = new ArrayList<Integer>();
      for (String bucketCol : tabBucketCols) {
        int pos = 0;
        for (FieldSchema tabCol : tabCols) {
          if (bucketCol.equals(tabCol.getName())) {
            posns.add(pos);
            break;
          }
          pos++;
        }
      }
      return posns;
    }

    // Get the sort positions and sort order for the table
    private List<ObjectPair<Integer, Integer>> getSortPositions(List<Order> tabSortCols,
        List<FieldSchema> tabCols) {
      List<ObjectPair<Integer, Integer>> posns = new ArrayList<ObjectPair<Integer, Integer>>();
      for (Order sortCol : tabSortCols) {
        int pos = 0;
        for (FieldSchema tabCol : tabCols) {
          if (sortCol.getCol().equals(tabCol.getName())) {
            posns.add(new ObjectPair<Integer, Integer>(pos, sortCol.getOrder()));
            break;
          }
          pos++;
        }
      }
      return posns;
    }

    // Return true if the parition is bucketed/sorted by the specified positions
    // The number of buckets, the sort order should also match along with the
    // columns which are bucketed/sorted
    private boolean checkPartition(Partition partition,
        List<Integer> bucketPositionsDest,
        List<ObjectPair<Integer, Integer>> sortPositionsDest,
        int numBucketsDest) {
      // The bucketing and sorting positions should exactly match
      int numBuckets = partition.getBucketCount();
      if (numBucketsDest != numBuckets) {
        return false;
      }

      List<Integer> partnBucketPositions =
          getBucketPositions(partition.getBucketCols(), partition.getTable().getCols());
      List<ObjectPair<Integer, Integer>> partnSortPositions =
          getSortPositions(partition.getSortCols(), partition.getTable().getCols());
      return bucketPositionsDest.equals(partnBucketPositions) &&
          sortPositionsDest.equals(partnSortPositions);
    }

    // Return true if the table is bucketed/sorted by the specified positions
    // The number of buckets, the sort order should also match along with the
    // columns which are bucketed/sorted
    private boolean checkTable(Table table,
        List<Integer> bucketPositionsDest,
        List<ObjectPair<Integer, Integer>> sortPositionsDest,
        int numBucketsDest) {
      // The bucketing and sorting positions should exactly match
      int numBuckets = table.getNumBuckets();
      if (numBucketsDest != numBuckets) {
        return false;
      }

      List<Integer> tableBucketPositions =
          getBucketPositions(table.getBucketCols(), table.getCols());
      List<ObjectPair<Integer, Integer>> tableSortPositions =
          getSortPositions(table.getSortCols(), table.getCols());
      return bucketPositionsDest.equals(tableBucketPositions) &&
          sortPositionsDest.equals(tableSortPositions);
    }

    private void storeBucketPathMapping(TableScanOperator tsOp, FileStatus[] srcs) {
      Map<String, Integer> bucketFileNameMapping = new HashMap<String, Integer>();
      for (int pos = 0; pos < srcs.length; pos++) {
        bucketFileNameMapping.put(srcs[pos].getPath().getName(), pos);
      }
      tsOp.getConf().setBucketFileNameMapping(bucketFileNameMapping);
    }

    // Remove the reduceSinkOperator.
    // The optimizer will automatically convert it to a map-only job.
    private void removeReduceSink(ReduceSinkOperator rsOp,
        TableScanOperator tsOp,
        FileSinkOperator fsOp,
        FileStatus[] srcs) {
      if (srcs == null) {
        return;
      }

      removeReduceSink(rsOp, tsOp, fsOp);
      // Store the mapping -> path, bucket number
      // This is needed since for the map-only job, any mapper can process any file.
      // For eg: if mapper 1 is processing the file corresponding to bucket 2, it should
      // also output the file correspodning to bucket 2 of the output.
      storeBucketPathMapping(tsOp, srcs);
    }

    // Remove the reduce sink operator
    // Use bucketized hive input format so that one mapper processes exactly one file
    private void removeReduceSink(ReduceSinkOperator rsOp,
        TableScanOperator tsOp,
        FileSinkOperator fsOp) {
      Operator<? extends OperatorDesc> parRSOp = rsOp.getParentOperators().get(0);
      parRSOp.getChildOperators().set(0, fsOp);
      fsOp.getParentOperators().set(0, parRSOp);
      fsOp.getConf().setMultiFileSpray(false);
      fsOp.getConf().setTotalFiles(1);
      fsOp.getConf().setNumFiles(1);
      tsOp.setUseBucketizedHiveInputFormat(true);
    }

    private int findColumnPosition(List<FieldSchema> cols, String colName) {
      int pos = 0;
      for (FieldSchema col : cols) {
        if (colName.equals(col.getName())) {
          return pos;
        }
        pos++;
      }
      return -1;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // If the reduce sink has not been introduced due to bucketing/sorting, ignore it
      FileSinkOperator fsOp = (FileSinkOperator) nd;
      ExtractOperator exOp = (ExtractOperator) fsOp.getParentOperators().get(0);
      ReduceSinkOperator rsOp = (ReduceSinkOperator) exOp.getParentOperators().get(0);

      List<ReduceSinkOperator> rsOps = pGraphContext
          .getReduceSinkOperatorsAddedByEnforceBucketingSorting();
      // nothing to do
      if ((rsOps != null) && (!rsOps.contains(rsOp))) {
        return null;
      }

      // Support for dynamic partitions can be added later
      if (fsOp.getConf().getDynPartCtx() != null) {
        return null;
      }

      // No conversion is possible for the reduce keys
      for (ExprNodeDesc keyCol : rsOp.getConf().getKeyCols()) {
        if (!(keyCol instanceof ExprNodeColumnDesc)) {
          return null;
        }
      }

      Table destTable = pGraphContext.getFsopToTable().get(fsOp);
      if (destTable == null) {
        return null;
      }

      // Get the positions for sorted and bucketed columns
      // For sorted columns, also get the order (ascending/descending) - that should
      // also match for this to be converted to a map-only job.
      List<Integer> bucketPositions =
          getBucketPositions(destTable.getBucketCols(), destTable.getCols());
      List<ObjectPair<Integer, Integer>> sortPositions =
          getSortPositions(destTable.getSortCols(), destTable.getCols());

      // Only selects and filters are allowed
      Operator<? extends OperatorDesc> op = rsOp;
      // TableScan will also be followed by a Select Operator. Find the expressions for the
      // bucketed/sorted columns for the destination table
      List<ExprNodeColumnDesc> sourceTableBucketCols = new ArrayList<ExprNodeColumnDesc>();
      List<ExprNodeColumnDesc> sourceTableSortCols = new ArrayList<ExprNodeColumnDesc>();

      while (true) {
        if (op.getParentOperators().size() > 1) {
          return null;
        }

        op = op.getParentOperators().get(0);
        if (!(op instanceof TableScanOperator) &&
            !(op instanceof FilterOperator) &&
            !(op instanceof SelectOperator)) {
          return null;
        }

        // nothing to be done for filters - the output schema does not change.
        if (op instanceof TableScanOperator) {
          Table srcTable = pGraphContext.getTopToTable().get(op);

          // Find the positions of the bucketed columns in the table corresponding
          // to the select list.
          // Consider the following scenario:
          // T1(key, value1, value2) bucketed/sorted by key into 2 buckets
          // T2(dummy, key, value1, value2) bucketed/sorted by key into 2 buckets
          // A query like: insert overwrite table T2 select 1, key, value1, value2 from T1
          // should be optimized.

          // Start with the destination: T2, bucketed/sorted position is [1]
          // At the source T1, the column corresponding to that position is [key], which
          // maps to column [0] of T1, which is also bucketed/sorted into the same
          // number of buckets
          List<Integer> newBucketPositions = new ArrayList<Integer>();
          for (int pos = 0; pos < bucketPositions.size(); pos++) {
            ExprNodeColumnDesc col = sourceTableBucketCols.get(pos);
            String colName = col.getColumn();
            int bucketPos = findColumnPosition(srcTable.getCols(), colName);
            if (bucketPos < 0) {
              return null;
            }
            newBucketPositions.add(bucketPos);
          }

          // Find the positions/order of the sorted columns in the table corresponding
          // to the select list.
          List<ObjectPair<Integer, Integer>> newSortPositions =
              new ArrayList<ObjectPair<Integer, Integer>>();
          for (int pos = 0; pos < sortPositions.size(); pos++) {
            ExprNodeColumnDesc col = sourceTableSortCols.get(pos);
            String colName = col.getColumn();
            int sortPos = findColumnPosition(srcTable.getCols(), colName);
            if (sortPos < 0) {
              return null;
            }
            newSortPositions.add(
                new ObjectPair<Integer, Integer>(sortPos, sortPositions.get(pos).getSecond()));
          }


          if (srcTable.isPartitioned()) {
            PrunedPartitionList prunedParts = pGraphContext.getOpToPartList().get(op);
            List<Partition> partitions = prunedParts.getNotDeniedPartns();

            // Support for dynamic partitions can be added later
            // The following is not optimized:
            // insert overwrite table T1(ds='1', hr) select key, value, hr from T2 where ds = '1';
            // where T1 and T2 are bucketed by the same keys and partitioned by ds. hr
            if ((partitions == null) || (partitions.isEmpty()) || (partitions.size() > 1)) {
              return null;
            }
            for (Partition partition : partitions) {
              if (!checkPartition(partition, newBucketPositions, newSortPositions,
                  pGraphContext.getFsopToTable().get(fsOp).getNumBuckets())) {
                return null;
              }
            }

            removeReduceSink(rsOp, (TableScanOperator) op, fsOp,
                partitions.get(0).getSortedPaths());
            return null;
          }
          else {
            if (!checkTable(srcTable, newBucketPositions, newSortPositions,
                pGraphContext.getFsopToTable().get(fsOp).getNumBuckets())) {
              return null;
            }

            removeReduceSink(rsOp, (TableScanOperator) op, fsOp, srcTable.getSortedPaths());
            return null;
          }
        }
        // None of the operators is changing the positions
        else if (op instanceof SelectOperator) {
          SelectOperator selectOp = (SelectOperator) op;
          SelectDesc selectDesc = selectOp.getConf();

          // There may be multiple selects - chose the one closest to the table
          sourceTableBucketCols.clear();
          sourceTableSortCols.clear();

          // Only columns can be selected for both sorted and bucketed positions
          for (int pos : bucketPositions) {
            ExprNodeDesc selectColList = selectDesc.getColList().get(pos);
            if (!(selectColList instanceof ExprNodeColumnDesc)) {
              return null;
            }
            sourceTableBucketCols.add((ExprNodeColumnDesc) selectColList);
          }

          for (ObjectPair<Integer, Integer> pos : sortPositions) {
            ExprNodeDesc selectColList = selectDesc.getColList().get(pos.getFirst());
            if (!(selectColList instanceof ExprNodeColumnDesc)) {
              return null;
            }
            sourceTableSortCols.add((ExprNodeColumnDesc) selectColList);
          }
        }
      }
    }
  }
}
