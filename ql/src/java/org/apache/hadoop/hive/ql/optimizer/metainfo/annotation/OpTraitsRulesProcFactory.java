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

package org.apache.hadoop.hive.ql.optimizer.metainfo.annotation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.*;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;

import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.AbstractBucketJoinProc;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.ptf.PTFExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.PartitionDef;

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

  public static class DefaultRule implements SemanticNodeProcessor {

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
  public static class ReduceSinkRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      ReduceSinkOperator rs = (ReduceSinkOperator)nd;

      int numReduceSinks = 1;
      OpTraits parentOpTraits = rs.getParentOperators().get(0).getOpTraits();
      if (parentOpTraits != null) {
        numReduceSinks += parentOpTraits.getNumReduceSinks();
      }

      List<String> bucketCols = new ArrayList<>();
      if (parentOpTraits != null &&
              parentOpTraits.getBucketColNames() != null) {
        if (parentOpTraits.getBucketColNames().size() > 0) {
          for (List<String> cols : parentOpTraits.getBucketColNames()) {
            for (String col : cols) {
              for (Entry<String, ExprNodeDesc> entry : rs.getColumnExprMap().entrySet()) {
                // Make sure this entry is in key columns.
                boolean isKey = false;
                for (ExprNodeDesc keyDesc : rs.getConf().getKeyCols()) {
                  if (keyDesc.isSame(entry.getValue())) {
                    isKey = true;
                    break;
                  }
                }

                // skip if not a key
                if (!isKey) {
                  continue;
                }
                // Fetch the column expression. There should be atleast one.
                Multimap<Integer, ExprNodeColumnDesc> colMap = ArrayListMultimap.create();
                boolean found = false;
                ExprNodeDescUtils.getExprNodeColumnDesc(entry.getValue(), colMap);
                for (Integer hashCode : colMap.keySet()) {
                  Collection<ExprNodeColumnDesc> exprs = colMap.get(hashCode);
                  for (ExprNodeColumnDesc expr : exprs) {
                    if (expr.getColumn().equals(col)) {
                      bucketCols.add(entry.getKey());
                      found = true;
                      break;
                    }
                  }
                  if (found) {
                    break;
                  }
                }
                if (found) {
                  break;
                }
              } // column exprmap.
            } // cols
          }
        } else {
          // fallback to old mechanism which serves SMB Joins.
          for (ExprNodeDesc exprDesc : rs.getConf().getKeyCols()) {
            for (Entry<String, ExprNodeDesc> entry : rs.getColumnExprMap().entrySet()) {
              if (exprDesc.isSame(entry.getValue())) {
                bucketCols.add(entry.getKey());
              }
            }
          }
        }
      }

      final List<List<String>> listBucketCols = new ArrayList<>();
      final List<CustomBucketFunction> bucketFunctions = new ArrayList<>();
      int numBuckets = -1;
      if (parentOpTraits == null || !parentOpTraits.hasCustomBucketFunction()) {
        // No CustomBucketFunctions
        listBucketCols.add(bucketCols);
        bucketFunctions.add(null);
        if (parentOpTraits != null) {
          numBuckets = parentOpTraits.getNumBuckets();
        }
      } else if (parentOpTraits.getCustomBucketFunctions().size() > 1) {
        // We don't know how to merge multiple custom bucket functions. Reset bucket attributes
        Preconditions.checkState(parentOpTraits.getBucketColNames().size() > 1);
        listBucketCols.add(Collections.emptyList());
        bucketFunctions.add(null);
      } else {
        Preconditions.checkState(parentOpTraits.getBucketColNames().size() == 1);
        final Map<String, String> inputToOutput = rs.getColumnExprMap().entrySet().stream()
            .filter(entry -> entry.getValue() instanceof ExprNodeColumnDesc)
            .filter(entry -> rs.getConf().getKeyCols().stream().anyMatch(keyDesc -> keyDesc.isSame(entry.getValue())))
            .collect(Collectors.toMap(
                entry -> ((ExprNodeColumnDesc) entry.getValue()).getColumn(),
                Entry::getKey,
                (a, b) -> a)
            );
        final List<String> parentBucketColNames = parentOpTraits.getBucketColNames().get(0);
        final boolean[] retainedColumns = new boolean[parentBucketColNames.size()];
        final List<String> rsBucketColNames = new ArrayList<>();
        for (int i = 0; i < parentBucketColNames.size(); i++) {
          final String rsColumnName = inputToOutput.get(parentBucketColNames.get(i));
          if (rsColumnName != null) {
            retainedColumns[i] = true;
            rsBucketColNames.add(rsColumnName);
          }
        }
        final Optional<CustomBucketFunction> rsBucketFunction =
            parentOpTraits.getCustomBucketFunctions().get(0).select(retainedColumns);
        if (rsBucketFunction.isPresent()) {
          listBucketCols.add(rsBucketColNames);
          bucketFunctions.add(rsBucketFunction.get());
        } else {
          listBucketCols.add(Collections.emptyList());
          bucketFunctions.add(null);
        }
      }

      OpTraits opTraits = new OpTraits(listBucketCols, bucketFunctions, numBuckets,
          listBucketCols, numReduceSinks);
      rs.setOpTraits(opTraits);
      return null;
    }
  }

  /*
   * Table scan has the table object and pruned partitions that has information
   * such as bucketing, sorting, etc. that is used later for optimization.
   */
  public static class TableScanRule implements SemanticNodeProcessor {

    public boolean checkBucketedTable(Table tbl, ParseContext pGraphContext,
        PrunedPartitionList prunedParts) throws SemanticException {

      final int numBuckets = tbl.getNumBuckets();
      if (numBuckets <= 0) {
        return false;
      }

      // Tez can handle unpopulated buckets
      if (!HiveConf.getVar(pGraphContext.getConf(), HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
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
              if (fileNames.size() != 0 && fileNames.size() != numBuckets) {
                return false;
              }
            }
          }
        } else {

          List<String> fileNames =
                  AbstractBucketJoinProc.getBucketFilePathsOfPartition(tbl.getDataLocation(),
                          pGraphContext);
          // The number of files for the table should be same as number of buckets.
          if (fileNames.size() != 0 && fileNames.size() != numBuckets) {
            return false;
          }
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

      final List<List<String>> bucketColsList = new ArrayList<>();
      final List<List<String>> sortedColsList = new ArrayList<>();
      final List<CustomBucketFunction> bucketFunctions = new ArrayList<>();
      int numBuckets = -1;
      if (table.getStorageHandler() != null
          && table.getStorageHandler().supportsPartitionAwareOptimization(table)
          && HiveConf.getVar(opTraitsCtx.getConf(), HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
        final PartitionAwareOptimizationCtx ctx =
            table.getStorageHandler().createPartitionAwareOptimizationContext(table);
        bucketColsList.add(ctx.getBucketFunction().getSourceColumnNames());
        bucketFunctions.add(ctx.getBucketFunction());
      } else if (checkBucketedTable(table, opTraitsCtx.getParseContext(), prunedPartList)) {
        bucketColsList.add(table.getBucketCols());
        numBuckets = table.getNumBuckets();
        List<String> sortCols = new ArrayList<>();
        for (Order colSortOrder : table.getSortCols()) {
          sortCols.add(colSortOrder.getCol());
        }
        sortedColsList.add(sortCols);
        bucketFunctions.add(null);
      }

      // num reduce sinks hardcoded to 0 because TS has no parents
      OpTraits opTraits = new OpTraits(bucketColsList, bucketFunctions, numBuckets,
          sortedColsList, 0);
      ts.setOpTraits(opTraits);
      return null;
    }
  }

  /*
   * Group-by re-orders the keys emitted hence, the keyCols would change.
   */
  public static class GroupByRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      GroupByOperator gbyOp = (GroupByOperator)nd;
      List<String> gbyKeys = new ArrayList<>();
      for (ExprNodeDesc exprDesc : gbyOp.getConf().getKeys()) {
        for (Entry<String, ExprNodeDesc> entry : gbyOp.getColumnExprMap().entrySet()) {
          if (exprDesc.isSame(entry.getValue())) {
            gbyKeys.add(entry.getKey());
          }
        }
      }

      List<List<String>> listBucketCols = new ArrayList<>();
      int numReduceSinks = 0;
      OpTraits parentOpTraits = gbyOp.getParentOperators().get(0).getOpTraits();
      if (parentOpTraits != null) {
        numReduceSinks = parentOpTraits.getNumReduceSinks();
      }
      listBucketCols.add(gbyKeys);
      List<CustomBucketFunction> bucketFunctions = Collections.singletonList(null);
      OpTraits opTraits = new OpTraits(listBucketCols, bucketFunctions, -1, listBucketCols,
          numReduceSinks);
      gbyOp.setOpTraits(opTraits);
      return null;
    }
  }


  /*
   * PTFOperator re-orders the keys just like Group By Operator does.
   */
  public static class PTFRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
      PTFOperator ptfOp = (PTFOperator) nd;
      List<String> partitionKeys = new ArrayList<>();

      PartitionDef partition = ptfOp.getConf().getFuncDef().getPartition();
      if (partition != null && partition.getExpressions() != null) {
        // Go through each expression in PTF window function.
        // All the expressions must be on columns, else we put empty list.
        for (PTFExpressionDef expression : partition.getExpressions()) {
          ExprNodeDesc exprNode = expression.getExprNode();
          if (!(exprNode instanceof ExprNodeColumnDesc)) {
            // clear out the list and bail out
            partitionKeys.clear();
            break;
          }

          partitionKeys.add(exprNode.getExprString());
        }
      }

      List<List<String>> listBucketCols = new ArrayList<>();
      int numReduceSinks = 0;
      OpTraits parentOptraits = ptfOp.getParentOperators().get(0).getOpTraits();
      if (parentOptraits != null) {
        numReduceSinks = parentOptraits.getNumReduceSinks();
      }

      listBucketCols.add(partitionKeys);
      final List<CustomBucketFunction> bucketFunctions = Collections.singletonList(null);
      OpTraits opTraits = new OpTraits(listBucketCols, bucketFunctions, -1, listBucketCols,
          numReduceSinks);
      ptfOp.setOpTraits(opTraits);
      return null;
    }
  }

  public static class SelectRule implements SemanticNodeProcessor {

    // For bucket columns
    // If the projected columns are compatible with the bucketing requirement, put them in the bucket cols
    // else, add empty list.
    private void putConvertedColNamesForBucket(
        List<List<String>> parentColNamesList, List<CustomBucketFunction> parentBucketFunctions, SelectOperator selOp,
        List<List<String>> newBucketColNamesList, List<CustomBucketFunction> newBucketFunctions) {
      Preconditions.checkState(parentColNamesList.size() == parentBucketFunctions.size());
      for (int i = 0; i < parentColNamesList.size(); i++) {
        List<String> colNames = parentColNamesList.get(i);

        List<String> newBucketColNames = new ArrayList<>();
        boolean[] retainedColumns = new boolean[colNames.size()];
        boolean allFound = true;
        for (int j = 0; j < colNames.size(); j++) {
          final String colName = colNames.get(j);
          Optional<String> newColName = resolveNewColName(colName, selOp);
          if (newColName.isPresent()) {
            retainedColumns[j] = true;
            newBucketColNames.add(newColName.get());
          } else {
            retainedColumns[j] = false;
            allFound = false;
          }
        }

        CustomBucketFunction bucketFunction = parentBucketFunctions.get(i);
        if (allFound) {
          newBucketColNamesList.add(newBucketColNames);
          newBucketFunctions.add(bucketFunction);
        } else if (bucketFunction == null) {
          // Hive's native bucketing is effective only when all the bucketing columns are used
          newBucketColNamesList.add(new ArrayList<>());
          newBucketFunctions.add(null);
        } else {
          Optional<CustomBucketFunction> newBucketFunction = bucketFunction.select(retainedColumns);
          if (newBucketFunction.isPresent()) {
            newBucketColNamesList.add(newBucketColNames);
            newBucketFunctions.add(newBucketFunction.get());
          } else {
            newBucketColNamesList.add(new ArrayList<>());
            newBucketFunctions.add(null);
          }
        }
      }
    }

    // For sort columns
    // Keep the subset of all the columns as long as order is maintained.
    private List<List<String>> getConvertedColNamesForSort(List<List<String>> parentColNames, SelectOperator selOp) {
      List<List<String>> listBucketCols = new ArrayList<>();
      for (List<String> colNames : parentColNames) {
        List<String> bucketColNames = new ArrayList<>();
        for (String colName : colNames) {
          Optional<String> newColName = resolveNewColName(colName, selOp);
          if (newColName.isPresent()) {
            bucketColNames.add(newColName.get());
          } else {
            // Bail out on first missed column.
            break;
          }
        }
        listBucketCols.add(bucketColNames);
      }

      return listBucketCols;
    }

    private Optional<String> resolveNewColName(String parentColName, SelectOperator selOp) {
      for (Entry<String, ExprNodeDesc> entry : selOp.getColumnExprMap().entrySet()) {
        if ((entry.getValue() instanceof ExprNodeColumnDesc) &&
            (((ExprNodeColumnDesc) (entry.getValue())).getColumn().equals(parentColName))) {
          return Optional.of(entry.getKey());
        }
      }
      return Optional.empty();
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      SelectOperator selOp = (SelectOperator) nd;
      OpTraits parentOpTraits = selOp.getParentOperators().get(0).getOpTraits();
      List<List<String>> parentBucketColNames = parentOpTraits.getBucketColNames();

      List<List<String>> listBucketCols = null;
      List<CustomBucketFunction> bucketFunctions = null;
      List<List<String>> listSortCols = null;
      if (selOp.getColumnExprMap() != null) {
        if (parentBucketColNames != null) {
          listBucketCols = new ArrayList<>();
          bucketFunctions = new ArrayList<>();
          putConvertedColNamesForBucket(parentBucketColNames, parentOpTraits.getCustomBucketFunctions(), selOp,
              listBucketCols, bucketFunctions);
        }
        List<List<String>> parentSortColNames = parentOpTraits.getSortCols();
        if (parentSortColNames != null) {
          listSortCols = getConvertedColNamesForSort(parentSortColNames, selOp);
        }
      }

      int numBuckets = -1;
      if (CollectionUtils.isNotEmpty(listBucketCols)
          && CollectionUtils.isNotEmpty(listBucketCols.get(0))
          && bucketFunctions.get(0) == null) {
        // if bucket columns are empty, then num buckets must be set to -1.
        // if a custom bucket function is available, the num buckets must be set to -1.
        numBuckets = parentOpTraits.getNumBuckets();
      }
      final int numReduceSinks = parentOpTraits.getNumReduceSinks();
      OpTraits opTraits = new OpTraits(listBucketCols, bucketFunctions, numBuckets, listSortCols,
          numReduceSinks);
      selOp.setOpTraits(opTraits);
      return null;
    }
  }

  public static class JoinRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      JoinOperator joinOp = (JoinOperator) nd;
      List<List<String>> bucketColsList = new ArrayList<List<String>>();
      List<CustomBucketFunction> bucketFunctions = new ArrayList<>();
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
        final Entry<List<String>, boolean[]> outputColNames =
            getOutputColNames(joinOp, parentOpTraits.getBucketColNames(), pos);
        if (outputColNames == null || outputColNames.getKey().isEmpty() || !parentOpTraits.hasCustomBucketFunction()) {
          bucketColsList.add(outputColNames == null ? null : outputColNames.getKey());
          bucketFunctions.add(null);
        } else {
          Preconditions.checkState(parentOpTraits.getBucketColNames().size() == 1);
          final Optional<CustomBucketFunction> joinBucketFunction =
              parentOpTraits.getCustomBucketFunctions().get(0).select(outputColNames.getValue());
          if (joinBucketFunction.isPresent()) {
            bucketColsList.add(outputColNames.getKey());
            bucketFunctions.add(joinBucketFunction.get());
          } else {
            bucketColsList.add(Collections.emptyList());
            bucketFunctions.add(null);
          }
        }

        final Entry<List<String>, boolean[]> outputSortColNames =
            getOutputColNames(joinOp, parentOpTraits.getSortCols(), pos);
        sortColsList.add(outputSortColNames == null ? null : outputSortColNames.getKey());
        if (parentOpTraits.getNumReduceSinks() > numReduceSinks) {
          numReduceSinks = parentOpTraits.getNumReduceSinks();
        }
        pos++;
      }

      // The bucketingVersion is not relevant here as it is never used.
      // For SMB, we look at the parent tables' bucketing versions and for
      // bucket map join the big table's bucketing version is considered.
      joinOp.setOpTraits(new OpTraits(bucketColsList, bucketFunctions, -1, bucketColsList, numReduceSinks));
      return null;
    }

    private Entry<List<String>, boolean[]> getOutputColNames(JoinOperator joinOp, List<List<String>> parentColNames,
        byte pos) {
      if (parentColNames == null) {
        // no col names in parent
        return null;
      }
      List<String> bucketColNames = new ArrayList<>();

      // guaranteed that there is only 1 list within this list because
      // a reduce sink always brings down the bucketing cols to a single list.
      // may not be true with correlation operators (mux-demux)
      List<String> colNames = parentColNames.size() > 0 ? parentColNames.get(0) : new ArrayList<>();
      final boolean[] retainedColumns = new boolean[colNames.size()];
      for (int i = 0; i < colNames.size(); i++) {
        final String colName = colNames.get(i);
        for (ExprNodeDesc exprNode : joinOp.getConf().getExprs().get(pos)) {
          if (exprNode instanceof ExprNodeColumnDesc) {
            if (((ExprNodeColumnDesc) (exprNode)).getColumn().equals(colName)) {
              for (Entry<String, ExprNodeDesc> entry : joinOp.getColumnExprMap().entrySet()) {
                if (entry.getValue().isSame(exprNode)) {
                  bucketColNames.add(entry.getKey());
                  retainedColumns[i] = true;
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

      return new SimpleImmutableEntry<>(bucketColNames, retainedColumns);
    }
  }

  /*
   * When we have operators that have multiple parents, it is not clear which
   * parent's traits we need to propagate forward.
   */
  public static class MultiParentRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> operator = (Operator<? extends OperatorDesc>) nd;

      int numReduceSinks = 0;
      for (Operator<?> parentOp : operator.getParentOperators()) {
        if (parentOp.getOpTraits() == null) {
          continue;
        }
        if (parentOp.getOpTraits().getNumReduceSinks() > numReduceSinks) {
          numReduceSinks = parentOp.getOpTraits().getNumReduceSinks();
        }
      }
      OpTraits opTraits = new OpTraits(null, null, -1,
          null, numReduceSinks);
      operator.setOpTraits(opTraits);
      return null;
    }
  }

  public static SemanticNodeProcessor getTableScanRule() {
    return new TableScanRule();
  }

  public static SemanticNodeProcessor getReduceSinkRule() {
    return new ReduceSinkRule();
  }

  public static SemanticNodeProcessor getSelectRule() {
    return new SelectRule();
  }

  public static SemanticNodeProcessor getDefaultRule() {
    return new DefaultRule();
  }

  public static SemanticNodeProcessor getMultiParentRule() {
    return new MultiParentRule();
  }

  public static SemanticNodeProcessor getGroupByRule() {
    return new GroupByRule();
  }

  public static SemanticNodeProcessor getPTFRule() {
    return new PTFRule();
  }

  public static SemanticNodeProcessor getJoinRule() {
    return new JoinRule();
  }
}
