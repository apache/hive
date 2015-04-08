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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.ReduceField;
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
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * When dynamic partitioning (with or without bucketing and sorting) is enabled, this optimization
 * sorts the records on partition, bucket and sort columns respectively before inserting records
 * into the destination table. This enables reducers to keep only one record writer all the time
 * thereby reducing the the memory pressure on the reducers. This optimization will force a reducer
 * even when hive.enforce.bucketing and hive.enforce.sorting is set to false.
 */
public class SortedDynPartitionOptimizer implements Transform {

  private static final String BUCKET_NUMBER_COL_NAME = "_bucket_number";
  @Override
  public ParseContext transform(ParseContext pCtx) throws SemanticException {

    // create a walker which walks the tree in a DFS manner while maintaining the
    // operator stack. The dispatcher generates the plan from the operator tree
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();

    String FS = FileSinkOperator.getOperatorName() + "%";

    opRules.put(new RuleRegExp("Sorted Dynamic Partition", FS), getSortDynPartProc(pCtx));

    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pCtx;
  }

  private NodeProcessor getSortDynPartProc(ParseContext pCtx) {
    return new SortedDynamicPartitionProc(pCtx);
  }

  class SortedDynamicPartitionProc implements NodeProcessor {

    private final Log LOG = LogFactory.getLog(SortedDynPartitionOptimizer.class);
    protected ParseContext parseCtx;

    public SortedDynamicPartitionProc(ParseContext pCtx) {
      this.parseCtx = pCtx;
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // introduce RS and EX before FS. If the operator tree already contains
      // RS then ReduceSinkDeDuplication optimization should merge them
      FileSinkOperator fsOp = (FileSinkOperator) nd;

      LOG.info("Sorted dynamic partitioning optimization kicked in..");

      // if not dynamic partitioning then bail out
      if (fsOp.getConf().getDynPartCtx() == null) {
        LOG.debug("Bailing out of sort dynamic partition optimization as dynamic partitioning context is null");
        return null;
      }

      // if list bucketing then bail out
      ListBucketingCtx lbCtx = fsOp.getConf().getLbCtx();
      if (lbCtx != null && !lbCtx.getSkewedColNames().isEmpty()
          && !lbCtx.getSkewedColValues().isEmpty()) {
        LOG.debug("Bailing out of sort dynamic partition optimization as list bucketing is enabled");
        return null;
      }

      Table destTable = fsOp.getConf().getTable();
      if (destTable == null) {
        LOG.debug("Bailing out of sort dynamic partition optimization as destination table is null");
        return null;
      }

      // if RS is inserted by enforce bucketing or sorting, we need to remove it
      // since ReduceSinkDeDuplication will not merge them to single RS.
      // RS inserted by enforce bucketing/sorting will have bucketing column in
      // reduce sink key whereas RS inserted by this optimization will have
      // partition columns followed by bucket number followed by sort columns in
      // the reduce sink key. Since both key columns are not prefix subset
      // ReduceSinkDeDuplication will not merge them together resulting in 2 MR jobs.
      // To avoid that we will remove the RS (and EX) inserted by enforce bucketing/sorting.
      if (!removeRSInsertedByEnforceBucketing(fsOp)) {
        LOG.debug("Bailing out of sort dynamic partition optimization as some partition columns " +
            "got constant folded.");
        return null;
      }

      // unlink connection between FS and its parent
      Operator<? extends OperatorDesc> fsParent = fsOp.getParentOperators().get(0);
      fsParent.getChildOperators().clear();

      DynamicPartitionCtx dpCtx = fsOp.getConf().getDynPartCtx();
      int numBuckets = destTable.getNumBuckets();

      // if enforce bucketing/sorting is disabled numBuckets will not be set.
      // set the number of buckets here to ensure creation of empty buckets
      dpCtx.setNumBuckets(numBuckets);

      // Get the positions for partition, bucket and sort columns
      List<Integer> bucketPositions = getBucketPositions(destTable.getBucketCols(),
          destTable.getCols());
      ObjectPair<List<Integer>, List<Integer>> sortOrderPositions = getSortPositionsOrder(
          destTable.getSortCols(), destTable.getCols());
      List<Integer> sortPositions = null;
      List<Integer> sortOrder = null;
      if (fsOp.getConf().getWriteType() == AcidUtils.Operation.UPDATE ||
          fsOp.getConf().getWriteType() == AcidUtils.Operation.DELETE) {
        // When doing updates and deletes we always want to sort on the rowid because the ACID
        // reader will expect this sort order when doing reads.  So
        // ignore whatever comes from the table and enforce this sort order instead.
        sortPositions = Arrays.asList(0);
        sortOrder = Arrays.asList(1); // 1 means asc, could really use enum here in the thrift if
      } else {
        sortPositions = sortOrderPositions.getFirst();
        sortOrder = sortOrderPositions.getSecond();
      }
      LOG.debug("Got sort order");
      for (int i : sortPositions) LOG.debug("sort position " + i);
      for (int i : sortOrder) LOG.debug("sort order " + i);
      List<Integer> partitionPositions = getPartitionPositions(dpCtx, fsParent.getSchema());
      List<ColumnInfo> colInfos = fsParent.getSchema().getSignature();
      ArrayList<ExprNodeDesc> bucketColumns = getPositionsToExprNodes(bucketPositions, colInfos);

      // update file sink descriptor
      fsOp.getConf().setMultiFileSpray(false);
      fsOp.getConf().setNumFiles(1);
      fsOp.getConf().setTotalFiles(1);

      // Create ReduceSinkDesc
      RowSchema outRS = new RowSchema(fsParent.getSchema());
      ArrayList<ColumnInfo> valColInfo = Lists.newArrayList(fsParent.getSchema().getSignature());
      ArrayList<ExprNodeDesc> newValueCols = Lists.newArrayList();
      Map<String, ExprNodeDesc> colExprMap = Maps.newHashMap();
      for (ColumnInfo ci : valColInfo) {
        newValueCols.add(new ExprNodeColumnDesc(ci));
        colExprMap.put(ci.getInternalName(), newValueCols.get(newValueCols.size() - 1));
      }
      ReduceSinkDesc rsConf = getReduceSinkDesc(partitionPositions, sortPositions, sortOrder,
          newValueCols, bucketColumns, numBuckets, fsParent, fsOp.getConf().getWriteType());

      if (!bucketColumns.isEmpty()) {
        String tableAlias = outRS.getSignature().get(0).getTabAlias();
        ColumnInfo ci = new ColumnInfo(BUCKET_NUMBER_COL_NAME, TypeInfoFactory.stringTypeInfo,
            tableAlias, true, true);
        outRS.getSignature().add(ci);
      }

      // Create ReduceSink operator
      ReduceSinkOperator rsOp = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(
              rsConf, new RowSchema(outRS.getSignature()), fsParent);
      rsOp.setColumnExprMap(colExprMap);

      List<ExprNodeDesc> valCols = rsConf.getValueCols();
      List<ExprNodeDesc> descs = new ArrayList<ExprNodeDesc>(valCols.size());
      List<String> colNames = new ArrayList<String>();
      String colName;
      for (ExprNodeDesc valCol : valCols) {
        colName = PlanUtils.stripQuotes(valCol.getExprString());
        colNames.add(colName);
        descs.add(new ExprNodeColumnDesc(valCol.getTypeInfo(), ReduceField.VALUE.toString()+"."+colName, null, false));
      }

      // Create SelectDesc
      SelectDesc selConf = new SelectDesc(descs, colNames);
      RowSchema selRS = new RowSchema(outRS);

      // Create Select Operator
      SelectOperator selOp = (SelectOperator) OperatorFactory.getAndMakeChild(
              selConf, selRS, rsOp);

      // link SEL to FS
      fsOp.getParentOperators().clear();
      fsOp.getParentOperators().add(selOp);
      selOp.getChildOperators().add(fsOp);

      // Set if partition sorted or partition bucket sorted
      fsOp.getConf().setDpSortState(FileSinkDesc.DPSortState.PARTITION_SORTED);
      if (bucketColumns.size() > 0) {
        fsOp.getConf().setDpSortState(FileSinkDesc.DPSortState.PARTITION_BUCKET_SORTED);
      }

      // update partition column info in FS descriptor
      ArrayList<ExprNodeDesc> partitionColumns = getPositionsToExprNodes(partitionPositions, rsOp
          .getSchema().getSignature());
      fsOp.getConf().setPartitionCols(partitionColumns);

      LOG.info("Inserted " + rsOp.getOperatorId() + " and " + selOp.getOperatorId()
          + " as parent of " + fsOp.getOperatorId() + " and child of " + fsParent.getOperatorId());
      return null;
    }

    // Remove RS and SEL introduced by enforce bucketing/sorting config
    // Convert PARENT -> RS -> SEL -> FS to PARENT -> FS
    private boolean removeRSInsertedByEnforceBucketing(FileSinkOperator fsOp) {
      HiveConf hconf = parseCtx.getConf();
      boolean enforceBucketing = HiveConf.getBoolVar(hconf, ConfVars.HIVEENFORCEBUCKETING);
      boolean enforceSorting = HiveConf.getBoolVar(hconf, ConfVars.HIVEENFORCESORTING);
      if (enforceBucketing || enforceSorting) {
        Set<ReduceSinkOperator> reduceSinks = OperatorUtils.findOperatorsUpstream(fsOp,
            ReduceSinkOperator.class);
        Operator<? extends OperatorDesc> rsToRemove = null;
        List<ReduceSinkOperator> rsOps = parseCtx
            .getReduceSinkOperatorsAddedByEnforceBucketingSorting();
        boolean found = false;

        // iterate through all RS and locate the one introduce by enforce bucketing
        for (ReduceSinkOperator reduceSink : reduceSinks) {
          for (ReduceSinkOperator rsOp : rsOps) {
            if (reduceSink.equals(rsOp)) {
              rsToRemove = reduceSink;
              found = true;
              break;
            }
          }

          if (found) {
            break;
          }
        }

        // iF RS is found remove it and its child (EX) and connect its parent
        // and grand child
        if (found) {
          Operator<? extends OperatorDesc> rsParent = rsToRemove.getParentOperators().get(0);
          Operator<? extends OperatorDesc> rsChild = rsToRemove.getChildOperators().get(0);
          Operator<? extends OperatorDesc> rsGrandChild = rsChild.getChildOperators().get(0);

          if (rsChild instanceof SelectOperator) {
            // if schema size cannot be matched, then it could be because of constant folding
            // converting partition column expression to constant expression. The constant
            // expression will then get pruned by column pruner since it will not reference to
            // any columns.
            if (rsParent.getSchema().getSignature().size() !=
                rsChild.getSchema().getSignature().size()) {
              return false;
            }
            rsParent.getChildOperators().clear();
            rsParent.getChildOperators().add(rsGrandChild);
            rsGrandChild.getParentOperators().clear();
            rsGrandChild.getParentOperators().add(rsParent);
            LOG.info("Removed " + rsToRemove.getOperatorId() + " and " + rsChild.getOperatorId()
                + " as it was introduced by enforce bucketing/sorting.");
          }
        }
      }

      return true;
    }

    private List<Integer> getPartitionPositions(DynamicPartitionCtx dpCtx, RowSchema schema) {
      int numPartCols = dpCtx.getNumDPCols();
      int numCols = schema.getSignature().size();
      List<Integer> partPos = Lists.newArrayList();

      // partition columns will always at the last
      for (int i = numCols - numPartCols; i < numCols; i++) {
        partPos.add(i);
      }
      return partPos;
    }

    // Get the bucket positions for the table
    private List<Integer> getBucketPositions(List<String> tabBucketCols, List<FieldSchema> tabCols) {
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

    public ReduceSinkDesc getReduceSinkDesc(List<Integer> partitionPositions,
        List<Integer> sortPositions, List<Integer> sortOrder, ArrayList<ExprNodeDesc> newValueCols,
        ArrayList<ExprNodeDesc> bucketColumns, int numBuckets,
        Operator<? extends OperatorDesc> parent, AcidUtils.Operation writeType) {

      // Order of KEY columns
      // 1) Partition columns
      // 2) Bucket number column
      // 3) Sort columns
      List<Integer> keyColsPosInVal = Lists.newArrayList();
      ArrayList<ExprNodeDesc> newKeyCols = Lists.newArrayList();
      List<Integer> newSortOrder = Lists.newArrayList();
      int numPartAndBuck = partitionPositions.size();

      keyColsPosInVal.addAll(partitionPositions);
      if (!bucketColumns.isEmpty()) {
        keyColsPosInVal.add(-1);
        numPartAndBuck += 1;
      }
      keyColsPosInVal.addAll(sortPositions);

      // by default partition and bucket columns are sorted in ascending order
      Integer order = 1;
      if (sortOrder != null && !sortOrder.isEmpty()) {
        if (sortOrder.get(0).intValue() == 0) {
          order = 0;
        }
      }
      for (int i = 0; i < numPartAndBuck; i++) {
        newSortOrder.add(order);
      }
      newSortOrder.addAll(sortOrder);

      String orderStr = "";
      for (Integer i : newSortOrder) {
        if(i.intValue() == 1) {
          orderStr += "+";
        } else {
          orderStr += "-";
        }
      }

      ArrayList<ExprNodeDesc> newPartCols = Lists.newArrayList();

      // we will clone here as RS will update bucket column key with its
      // corresponding with bucket number and hence their OIs
      for (Integer idx : keyColsPosInVal) {
        if (idx < 0) {
          // add bucket number column to both key and value
          ExprNodeConstantDesc encd = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo,
              BUCKET_NUMBER_COL_NAME);
          newKeyCols.add(encd);
          newValueCols.add(encd);
        } else {
          newKeyCols.add(newValueCols.get(idx).clone());
        }
      }

      for (Integer idx : partitionPositions) {
        newPartCols.add(newValueCols.get(idx).clone());
      }

      // in the absence of SORTED BY clause, the sorted dynamic partition insert
      // should honor the ordering of records provided by ORDER BY in SELECT statement
      ReduceSinkOperator parentRSOp = OperatorUtils.findSingleOperatorUpstream(parent,
          ReduceSinkOperator.class);
      if (parentRSOp != null && parseCtx.getQueryProperties().hasOuterOrderBy()) {
        String parentRSOpOrder = parentRSOp.getConf().getOrder();
        if (parentRSOpOrder != null && !parentRSOpOrder.isEmpty() && sortPositions.isEmpty()) {
          newKeyCols.addAll(parentRSOp.getConf().getKeyCols());
          orderStr += parentRSOpOrder;
        }
      }

      // Create Key/Value TableDesc. When the operator plan is split into MR tasks,
      // the reduce operator will initialize Extract operator with information
      // from Key and Value TableDesc
      List<FieldSchema> fields = PlanUtils.getFieldSchemasFromColumnList(newKeyCols,
          "reducesinkkey");
      TableDesc keyTable = PlanUtils.getReduceKeyTableDesc(fields, orderStr);
      ArrayList<String> outputKeyCols = Lists.newArrayList();
      for (int i = 0; i < newKeyCols.size(); i++) {
        outputKeyCols.add("reducesinkkey" + i);
      }

      List<String> outCols = Utilities.getInternalColumnNamesFromSignature(parent.getSchema()
          .getSignature());
      ArrayList<String> outValColNames = Lists.newArrayList(outCols);
      if (!bucketColumns.isEmpty()) {
        outValColNames.add(BUCKET_NUMBER_COL_NAME);
      }
      List<FieldSchema> valFields = PlanUtils.getFieldSchemasFromColumnList(newValueCols,
          outValColNames, 0, "");
      TableDesc valueTable = PlanUtils.getReduceValueTableDesc(valFields);
      List<List<Integer>> distinctColumnIndices = Lists.newArrayList();

      // Number of reducers is set to default (-1)
      ReduceSinkDesc rsConf = new ReduceSinkDesc(newKeyCols, newKeyCols.size(), newValueCols,
          outputKeyCols, distinctColumnIndices, outValColNames, -1, newPartCols, -1, keyTable,
          valueTable, writeType);
      rsConf.setBucketCols(bucketColumns);
      rsConf.setNumBuckets(numBuckets);

      return rsConf;
    }

    /**
     * Get the sort positions and sort order for the sort columns
     * @param tabSortCols
     * @param tabCols
     * @return
     */
    private ObjectPair<List<Integer>, List<Integer>> getSortPositionsOrder(List<Order> tabSortCols,
        List<FieldSchema> tabCols) {
      List<Integer> sortPositions = Lists.newArrayList();
      List<Integer> sortOrders = Lists.newArrayList();
      for (Order sortCol : tabSortCols) {
        int pos = 0;
        for (FieldSchema tabCol : tabCols) {
          if (sortCol.getCol().equals(tabCol.getName())) {
            sortPositions.add(pos);
            sortOrders.add(sortCol.getOrder());
            break;
          }
          pos++;
        }
      }
      return new ObjectPair<List<Integer>, List<Integer>>(sortPositions, sortOrders);
    }

    private ArrayList<ExprNodeDesc> getPositionsToExprNodes(List<Integer> pos,
        List<ColumnInfo> colInfos) {
      ArrayList<ExprNodeDesc> cols = Lists.newArrayList();

      for (Integer idx : pos) {
        ColumnInfo ci = colInfos.get(idx);
        ExprNodeColumnDesc encd = new ExprNodeColumnDesc(ci);
        cols.add(encd);
      }

      return cols;
    }

  }

}
