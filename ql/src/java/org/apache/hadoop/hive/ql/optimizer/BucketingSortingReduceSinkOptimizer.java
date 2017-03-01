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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.io.AcidUtils;
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
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This transformation does optimization for enforcing bucketing and sorting.
 * For a query of the form:
 * insert overwrite table T1 select * from T2;
 * where T1 and T2 are bucketized/sorted on the same keys, we don't need a reducer to
 * enforce bucketing and sorting.
 *
 * It also optimizes queries of the form:
 * insert overwrite table T1
 * select * from T1 join T2 on T1.key = T2.key
 * where T1, T2 and T3 are bucketized/sorted on the same key 'key', we don't need a reducer
 * to enforce bucketing and sorting
 */
public class BucketingSortingReduceSinkOptimizer extends Transform {

  public BucketingSortingReduceSinkOptimizer() {
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    // process reduce sink added by hive.enforce.bucketing or hive.enforce.sorting
    opRules.put(new RuleRegExp("R1",
        ReduceSinkOperator.getOperatorName() + "%" +
            SelectOperator.getOperatorName() + "%" +
            FileSinkOperator.getOperatorName() + "%"),
        getBucketSortReduceSinkProc(pctx));

    // The dispatcher fires the processor corresponding to the closest matching rule
    Dispatcher disp = new DefaultRuleDispatcher(getDefaultProc(), opRules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of top nodes
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
    private final Logger LOG = LoggerFactory.getLogger(BucketSortReduceSinkProcessor.class);
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
    // The sort order contains whether the sorting is happening ascending or descending
    private List<Integer> getSortPositions(
        List<Order> tabSortCols,
        List<FieldSchema> tabCols) {
      List<Integer> sortPositions = new ArrayList<Integer>();
      for (Order sortCol : tabSortCols) {
        int pos = 0;
        for (FieldSchema tabCol : tabCols) {
          if (sortCol.getCol().equals(tabCol.getName())) {
            sortPositions.add(pos);
            break;
          }
          pos++;
        }
      }
      return sortPositions;
    }

    private List<Integer> getSortOrder(
        List<Order> tabSortCols,
        List<FieldSchema> tabCols) {
      List<Integer> sortOrders = new ArrayList<Integer>();
      for (Order sortCol : tabSortCols) {
        for (FieldSchema tabCol : tabCols) {
          if (sortCol.getCol().equals(tabCol.getName())) {
            sortOrders.add(sortCol.getOrder());
            break;
          }
        }
      }
      return sortOrders;
    }

    // Return true if the partition is bucketed/sorted by the specified positions
    // The number of buckets, the sort order should also match along with the
    // columns which are bucketed/sorted
    private boolean checkPartition(Partition partition,
        List<Integer> bucketPositionsDest,
        List<Integer> sortPositionsDest,
        List<Integer> sortOrderDest,
        int numBucketsDest) {
      // The bucketing and sorting positions should exactly match
      int numBuckets = partition.getBucketCount();
      if (numBucketsDest != numBuckets) {
        return false;
      }

      List<Integer> partnBucketPositions =
          getBucketPositions(partition.getBucketCols(), partition.getTable().getCols());
      List<Integer> sortPositions =
          getSortPositions(partition.getSortCols(), partition.getTable().getCols());
      List<Integer> sortOrder =
          getSortOrder(partition.getSortCols(), partition.getTable().getCols());
      return bucketPositionsDest.equals(partnBucketPositions) &&
          sortPositionsDest.equals(sortPositions) &&
          sortOrderDest.equals(sortOrder);
    }

    // Return true if the table is bucketed/sorted by the specified positions
    // The number of buckets, the sort order should also match along with the
    // columns which are bucketed/sorted
    private boolean checkTable(Table table,
        List<Integer> bucketPositionsDest,
        List<Integer> sortPositionsDest,
        List<Integer> sortOrderDest,
        int numBucketsDest) {
      // The bucketing and sorting positions should exactly match
      int numBuckets = table.getNumBuckets();
      if (numBucketsDest != numBuckets) {
        return false;
      }

      List<Integer> tableBucketPositions =
          getBucketPositions(table.getBucketCols(), table.getCols());
      List<Integer> sortPositions =
          getSortPositions(table.getSortCols(), table.getCols());
      List<Integer> sortOrder =
          getSortOrder(table.getSortCols(), table.getCols());
      return bucketPositionsDest.equals(tableBucketPositions) &&
          sortPositionsDest.equals(sortPositions) &&
          sortOrderDest.equals(sortOrder);
    }

    // Store the bucket path to bucket number mapping in the table scan operator.
    // Although one mapper per file is used (BucketizedInputHiveInput), it is possible that
    // any mapper can pick up any file (depending on the size of the files). The bucket number
    // corresponding to the input file is stored to name the output bucket file appropriately.
    private void storeBucketPathMapping(TableScanOperator tsOp, FileStatus[] srcs) {
      Map<String, Integer> bucketFileNameMapping = new HashMap<String, Integer>();
      for (int pos = 0; pos < srcs.length; pos++) {
        if (ShimLoader.getHadoopShims().isDirectory(srcs[pos])) {
          throw new RuntimeException("Was expecting '" + srcs[pos].getPath() + "' to be bucket file.");
        }
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
      // also output the file corresponding to bucket 2 of the output.
      storeBucketPathMapping(tsOp, srcs);
    }

    // Remove the reduce sink operator
    // Use BucketizedHiveInputFormat so that one mapper processes exactly one file
    private void removeReduceSink(ReduceSinkOperator rsOp,
        TableScanOperator tsOp,
        FileSinkOperator fsOp) {
      Operator<? extends OperatorDesc> parRSOp = rsOp.getParentOperators().get(0);
      parRSOp.getChildOperators().set(0, fsOp);
      fsOp.getParentOperators().set(0, parRSOp);
      fsOp.getConf().setMultiFileSpray(false);
      fsOp.getConf().setTotalFiles(1);
      fsOp.getConf().setNumFiles(1);
      fsOp.getConf().setRemovedReduceSinkBucketSort(true);
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

    // The output columns for the destination table should match with the join keys
    // This is to handle queries of the form:
    // insert overwrite table T3
    // select T1.key, T1.key2, UDF(T1.value, T2.value)
    // from T1 join T2 on T1.key = T2.key and T1.key2 = T2.key2
    // where T1, T2 and T3 are bucketized/sorted on key and key2
    // Assuming T1 is the table on which the mapper is run, the following is true:
    // . The number of buckets for T1 and T3 should be same
    // . The bucketing/sorting columns for T1, T2 and T3 should be same
    // . The sort order of T1 should match with the sort order for T3.
    // . If T1 is partitioned, only a single partition of T1 can be selected.
    // . The select list should contain with (T1.key, T1.key2) or (T2.key, T2.key2)
    // . After the join, only selects and filters are allowed.
    private boolean validateSMBJoinKeys(SMBJoinDesc smbJoinDesc,
        List<ExprNodeColumnDesc> sourceTableBucketCols,
        List<ExprNodeColumnDesc> sourceTableSortCols,
        List<Integer> sortOrder) {
      // The sort-merge join creates the output sorted and bucketized by the same columns.
      // This can be relaxed in the future if there is a requirement.
      if (!sourceTableBucketCols.equals(sourceTableSortCols)) {
        return false;
      }

      // Get the total number of columns selected, and for each output column, store the
      // base table it points to. For
      // insert overwrite table T3
      // select T1.key, T1.key2, UDF(T1.value, T2.value)
      // from T1 join T2 on T1.key = T2.key and T1.key2 = T2.key2
      // the following arrays are created
      // [0, 0, 0, 1] --> [T1, T1, T1, T2] (table mapping)
      // [0, 1, 2, 0] --> [T1.0, T1.1, T1.2, T2.0] (table columns mapping)
      Byte[] tagOrder = smbJoinDesc.getTagOrder();
      Map<Byte, List<Integer>> retainList = smbJoinDesc.getRetainList();
      int totalNumberColumns = 0;
      for (Byte tag : tagOrder) {
        totalNumberColumns += retainList.get(tag).size();
      }

      byte[] columnTableMappings = new byte[totalNumberColumns];
      int[] columnNumberMappings = new int[totalNumberColumns];
      int currentColumnPosition = 0;
      for (Byte tag : tagOrder) {
        for (int pos = 0; pos < retainList.get(tag).size(); pos++) {
          columnTableMappings[currentColumnPosition] = tag;
          columnNumberMappings[currentColumnPosition] = pos;
          currentColumnPosition++;
        }
      }

      // All output columns used for bucketing/sorting of the destination table should
      // belong to the same input table
      //   insert overwrite table T3
      //   select T1.key, T2.key2, UDF(T1.value, T2.value)
      //   from T1 join T2 on T1.key = T2.key and T1.key2 = T2.key2
      // is not optimized, whereas the insert is optimized if the select list is either changed to
      // (T1.key, T1.key2, UDF(T1.value, T2.value)) or (T2.key, T2.key2, UDF(T1.value, T2.value))
      // Get the input table and make sure the keys match
      List<String> outputColumnNames = smbJoinDesc.getOutputColumnNames();
      byte tableTag = -1;
      int[] columnNumbersExprList = new int[sourceTableBucketCols.size()];
      int currentColPosition = 0;
      for (ExprNodeColumnDesc bucketCol : sourceTableBucketCols) {
        String colName = bucketCol.getColumn();
        int colNumber = outputColumnNames.indexOf(colName);
        if (colNumber < 0) {
          return false;
        }
        if (tableTag < 0) {
          tableTag = columnTableMappings[colNumber];
        }
        else if (tableTag != columnTableMappings[colNumber]) {
          return false;
        }
        columnNumbersExprList[currentColPosition++] = columnNumberMappings[colNumber];
      }

      List<ExprNodeDesc> allExprs = smbJoinDesc.getExprs().get(tableTag);
      List<ExprNodeDesc> keysSelectedTable = smbJoinDesc.getKeys().get(tableTag);
      currentColPosition = 0;
      for (ExprNodeDesc keySelectedTable : keysSelectedTable) {
        if (!(keySelectedTable instanceof ExprNodeColumnDesc)) {
          return false;
        }
        if (!allExprs.get(columnNumbersExprList[currentColPosition++]).isSame(keySelectedTable)) {
          return false;
        }
      }

      return true;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // We should not use this optimization if sorted dynamic partition optimizer is used,
      // as RS will be required.
      if (pGraphContext.isReduceSinkAddedBySortedDynPartition()) {
        LOG.info("Reduce Sink is added by Sorted Dynamic Partition Optimizer. Bailing out of" +
            " Bucketing Sorting Reduce Sink Optimizer");
        return null;
      }

      // If the reduce sink has not been introduced due to bucketing/sorting, ignore it
      FileSinkOperator fsOp = (FileSinkOperator) nd;
      ReduceSinkOperator rsOp = (ReduceSinkOperator) fsOp.getParentOperators().get(0).getParentOperators().get(0);

      List<ReduceSinkOperator> rsOps = pGraphContext
          .getReduceSinkOperatorsAddedByEnforceBucketingSorting();
      // nothing to do
      if ((rsOps != null) && (!rsOps.contains(rsOp))) {
        return null;
      }

      // Don't do this optimization with updates or deletes
      if (fsOp.getConf().getWriteType() == AcidUtils.Operation.UPDATE ||
        fsOp.getConf().getWriteType() == AcidUtils.Operation.DELETE) {
        return null;
      }

      if(stack.get(0) instanceof TableScanOperator) {
        TableScanOperator tso = ((TableScanOperator)stack.get(0));
        if(AcidUtils.isAcidTable(tso.getConf().getTableMetadata())) {
          /*ACID tables have complex directory layout and require merging of delta files
          * on read thus we should not try to read bucket files directly*/
          return null;
        }
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

      Table destTable = fsOp.getConf().getTable();
      if (destTable == null) {
        return null;
      }
      int numBucketsDestination = destTable.getNumBuckets();

      // Get the positions for sorted and bucketed columns
      // For sorted columns, also get the order (ascending/descending) - that should
      // also match for this to be converted to a map-only job.
      // Get the positions for sorted and bucketed columns
      // For sorted columns, also get the order (ascending/descending) - that should
      // also match for this to be converted to a map-only job.
      List<Integer> bucketPositions =
          getBucketPositions(destTable.getBucketCols(), destTable.getCols());
      List<Integer> sortPositions =
          getSortPositions(destTable.getSortCols(), destTable.getCols());
      List<Integer> sortOrder =
          getSortOrder(destTable.getSortCols(), destTable.getCols());
      boolean useBucketSortPositions = true;

      // Only selects and filters are allowed
      Operator<? extends OperatorDesc> op = rsOp;
      // TableScan will also be followed by a Select Operator. Find the expressions for the
      // bucketed/sorted columns for the destination table
      List<ExprNodeColumnDesc> sourceTableBucketCols = new ArrayList<ExprNodeColumnDesc>();
      List<ExprNodeColumnDesc> sourceTableSortCols = new ArrayList<ExprNodeColumnDesc>();
      op = op.getParentOperators().get(0);

      while (true) {
        if (!(op instanceof TableScanOperator) &&
            !(op instanceof FilterOperator) &&
            !(op instanceof SelectOperator) &&
            !(op instanceof SMBMapJoinOperator)) {
          return null;
        }

        if (op instanceof SMBMapJoinOperator) {
          // Bucketing and sorting keys should exactly match
          if (!(bucketPositions.equals(sortPositions))) {
            return null;
          }
          SMBMapJoinOperator smbOp = (SMBMapJoinOperator) op;
          SMBJoinDesc smbJoinDesc = smbOp.getConf();
          int posBigTable = smbJoinDesc.getPosBigTable();

          // join keys dont match the bucketing keys
          List<ExprNodeDesc> keysBigTable = smbJoinDesc.getKeys().get((byte) posBigTable);
          if (keysBigTable.size() != bucketPositions.size()) {
            return null;
          }

          if (!validateSMBJoinKeys(smbJoinDesc, sourceTableBucketCols,
              sourceTableSortCols, sortOrder)) {
            return null;
          }

          sourceTableBucketCols.clear();
          sourceTableSortCols.clear();
          useBucketSortPositions = false;

          for (ExprNodeDesc keyBigTable : keysBigTable) {
            if (!(keyBigTable instanceof ExprNodeColumnDesc)) {
              return null;
            }
            sourceTableBucketCols.add((ExprNodeColumnDesc) keyBigTable);
            sourceTableSortCols.add((ExprNodeColumnDesc) keyBigTable);
          }

          // since it is a sort-merge join, only follow the big table
          op = op.getParentOperators().get(posBigTable);
        } else {
          // nothing to be done for filters - the output schema does not change.
          if (op instanceof TableScanOperator) {
            assert !useBucketSortPositions;
            TableScanOperator ts = (TableScanOperator) op;
            Table srcTable = ts.getConf().getTableMetadata();

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
            List<Integer> newSortPositions = new ArrayList<Integer>();
            for (int pos = 0; pos < sortPositions.size(); pos++) {
              ExprNodeColumnDesc col = sourceTableSortCols.get(pos);
              String colName = col.getColumn();
              int sortPos = findColumnPosition(srcTable.getCols(), colName);
              if (sortPos < 0) {
                return null;
              }
              newSortPositions.add(sortPos);
            }

            if (srcTable.isPartitioned()) {
              PrunedPartitionList prunedParts =
                  pGraphContext.getPrunedPartitions(srcTable.getTableName(), ts);
              List<Partition> partitions = prunedParts.getNotDeniedPartns();

              // Support for dynamic partitions can be added later
              // The following is not optimized:
              // insert overwrite table T1(ds='1', hr) select key, value, hr from T2 where ds = '1';
              // where T1 and T2 are bucketed by the same keys and partitioned by ds. hr
              if ((partitions == null) || (partitions.isEmpty()) || (partitions.size() > 1)) {
                return null;
              }
              for (Partition partition : partitions) {
                if (!checkPartition(partition, newBucketPositions, newSortPositions, sortOrder,
                    numBucketsDestination)) {
                  return null;
                }
              }

              removeReduceSink(rsOp, (TableScanOperator) op, fsOp,
                  partitions.get(0).getSortedPaths());
              return null;
            }
            else {
              if (!checkTable(srcTable, newBucketPositions, newSortPositions, sortOrder,
                  numBucketsDestination)) {
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

            // Iterate backwards, from the destination table to the top of the tree
            // Based on the output column names, get the new columns.
            if (!useBucketSortPositions) {
              bucketPositions.clear();
              sortPositions.clear();
              List<String> outputColumnNames = selectDesc.getOutputColumnNames();

              for (ExprNodeColumnDesc col : sourceTableBucketCols) {
                String colName = col.getColumn();
                int colPos = outputColumnNames.indexOf(colName);
                if (colPos < 0) {
                  return null;
                }
                bucketPositions.add(colPos);
              }

              for (ExprNodeColumnDesc col : sourceTableSortCols) {
                String colName = col.getColumn();
                int colPos = outputColumnNames.indexOf(colName);
                if (colPos < 0) {
                  return null;
                }
                sortPositions.add(colPos);
              }
            }

            // There may be multiple selects - chose the one closest to the table
            sourceTableBucketCols.clear();
            sourceTableSortCols.clear();

            if (selectDesc.getColList().size() < bucketPositions.size()) {
             // Some columns in select are pruned. This may happen if those are constants.
              return null;
            }
            // Only columns can be selected for both sorted and bucketed positions
            for (int pos : bucketPositions) {
              if (pos >= selectDesc.getColList().size()) {
                // e.g., INSERT OVERWRITE TABLE temp1 SELECT  c0,  c0 FROM temp2;
                // In such a case Select Op will only have one instance of c0 and RS would have two.
                // So, locating bucketCol in such cases will generate error. So, bail out.
                return null;
              }
              ExprNodeDesc selectColList = selectDesc.getColList().get(pos);
              if (!(selectColList instanceof ExprNodeColumnDesc)) {
                return null;
              }
              sourceTableBucketCols.add((ExprNodeColumnDesc) selectColList);
            }

            for (int pos : sortPositions) {
              if (pos >= selectDesc.getColList().size()) {
                return null;
              }
              ExprNodeDesc selectColList = selectDesc.getColList().get(pos);
              if (!(selectColList instanceof ExprNodeColumnDesc)) {
                return null;
              }
              sourceTableSortCols.add((ExprNodeColumnDesc) selectColList);
            }

            useBucketSortPositions = false;
          }
          op = op.getParentOperators().get(0);
        }
      }
    }
  }
}
