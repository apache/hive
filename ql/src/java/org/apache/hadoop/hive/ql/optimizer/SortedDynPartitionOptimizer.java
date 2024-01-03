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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.MemoryInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.Utilities.ReduceField;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.RecordIdentifier;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.DynamicPartitionCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.ListBucketingCtx;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.orc.OrcConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * When dynamic partitioning (with or without bucketing and sorting) is enabled, this optimization
 * sorts the records on partition, bucket and sort columns respectively before inserting records
 * into the destination table. This enables reducers to keep only one record writer all the time
 * thereby reducing the the memory pressure on the reducers.
 * Sorting is based on the Dynamic Partitioning context that is already created in the file sink operator.
 * If that contains instructions for custom expression sorting, then this optimizer will disregard any partitioning or
 * bucketing information of the Hive (table format) table, and will arrange the plan solely as per the custom exprs.
 */
public class SortedDynPartitionOptimizer extends Transform {

  private static final Function<List<ExprNodeDesc>, ExprNodeDesc> BUCKET_SORT_EXPRESSION = cols -> {
    try {
      return ExprNodeGenericFuncDesc.newInstance(
          FunctionRegistry.getFunctionInfo("bucket_number").getGenericUDF(), new ArrayList<>());
    } catch (SemanticException e) {
      throw new RuntimeException(e);
    }
  };

  @Override
  public ParseContext transform(ParseContext pCtx) throws SemanticException {

    // create a walker which walks the tree in a DFS manner while maintaining the
    // operator stack. The dispatcher generates the plan from the operator tree
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();

    String FS = FileSinkOperator.getOperatorName() + "%";

    opRules.put(new RuleRegExp("Sorted Dynamic Partition", FS), getSortDynPartProc(pCtx));

    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pCtx;
  }

  private SemanticNodeProcessor getSortDynPartProc(ParseContext pCtx) {
    return new SortedDynamicPartitionProc(pCtx);
  }

  class SortedDynamicPartitionProc implements SemanticNodeProcessor {

    private final Logger LOG = LoggerFactory.getLogger(SortedDynPartitionOptimizer.class);
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

      if (destTable.isMaterializedView() &&
          (destTable.getProperty(Constants.MATERIALIZED_VIEW_SORT_COLUMNS) != null ||
              destTable.getProperty(Constants.MATERIALIZED_VIEW_DISTRIBUTE_COLUMNS) != null)) {
        LOG.debug("Bailing out of sort dynamic partition optimization as destination is a materialized view"
            + "with CLUSTER/SORT/DISTRIBUTE spec");
        return null;
      }

      // unlink connection between FS and its parent
      Operator<? extends OperatorDesc> fsParent = fsOp.getParentOperators().get(0);
      DynamicPartitionCtx dpCtx = fsOp.getConf().getDynPartCtx();

      ArrayList<ColumnInfo> parentCols = Lists.newArrayList(fsParent.getSchema().getSignature());
      ArrayList<ExprNodeDesc> allRSCols = Lists.newArrayList();
      for (ColumnInfo ci : parentCols) {
        allRSCols.add(new ExprNodeColumnDesc(ci));
      }

      // if all dp columns / custom sort expressions got constant folded then disable this optimization
      if (allStaticPartitions(fsParent, allRSCols, dpCtx)) {
        LOG.debug("Bailing out of sorted dynamic partition optimizer as all dynamic partition" +
            " columns got constant folded (static partitioning)");
        return null;
      }

      List<Integer> partitionPositions = getPartitionPositions(dpCtx, fsParent.getSchema());
      LinkedList<Function<List<ExprNodeDesc>, ExprNodeDesc>> customSortExprs =
          new LinkedList<>(dpCtx.getCustomSortExpressions());
      LinkedList<Integer> customSortOrder = new LinkedList<>(dpCtx.getCustomSortOrder());
      LinkedList<Integer> customNullOrder = new LinkedList<>(dpCtx.getCustomSortNullOrder());

      // If custom sort expressions are present, there is an explicit requirement to do sorting
      if (customSortExprs.isEmpty() && !shouldDo(partitionPositions, fsParent)) {
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
      fsParent = fsOp.getParentOperators().get(0);
      // store the index of the file sink operator to later insert the modified operator with RS at the same position
      int fsOpIndex = fsParent.getChildOperators().indexOf(fsOp);
      fsParent.getChildOperators().remove(fsOp);

      // if enforce bucketing/sorting is disabled numBuckets will not be set.
      // set the number of buckets here to ensure creation of empty buckets
      int numBuckets = destTable.getNumBuckets();
      dpCtx.setNumBuckets(numBuckets);

      // Get the positions for partition, bucket and sort columns
      List<Integer> bucketPositions = getBucketPositions(destTable.getBucketCols(),
          destTable.getCols());
      List<Integer> sortPositions = null;
      List<Integer> sortOrder = null;
      ArrayList<ExprNodeDesc> bucketColumns = null;
      if (fsOp.getConf().getWriteType() == AcidUtils.Operation.UPDATE ||
          fsOp.getConf().getWriteType() == AcidUtils.Operation.DELETE) {
        // When doing updates and deletes we always want to sort on the rowid because the ACID
        // reader will expect this sort order when doing reads.  So
        // ignore whatever comes from the table and enforce this sort order instead.
        sortPositions = Collections.singletonList(0);
        sortOrder = Collections.singletonList(1); // 1 means asc, could really use enum here in the thrift if
        /**
         * ROW__ID is always the 1st column of Insert representing Update/Delete operation
         * (set up in {@link org.apache.hadoop.hive.ql.parse.UpdateDeleteSemanticAnalyzer})
         * and we wrap it in UDFToInteger
         * (in {@link org.apache.hadoop.hive.ql.parse.SemanticAnalyzer#getPartitionColsFromBucketColsForUpdateDelete(Operator, boolean)})
         * which extracts bucketId from it
         * see {@link org.apache.hadoop.hive.ql.udf.UDFToInteger#evaluate(RecordIdentifier)}*/
        ColumnInfo ci = fsParent.getSchema().getSignature().get(0);
        if (!VirtualColumn.ROWID.getTypeInfo().equals(ci.getType())) {
          throw new IllegalStateException("expected 1st column to be ROW__ID but got wrong type: " + ci.toString());
        }

        if (numBuckets > 0) {
          bucketColumns = new ArrayList<>();
          //add a cast(ROW__ID as int) to wrap in UDFToInteger()
          bucketColumns.add(ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
              .createConversionCast(new ExprNodeColumnDesc(ci), TypeInfoFactory.intTypeInfo));
        }
      } else {
        if (!destTable.getSortCols().isEmpty()) {
          // Sort columns specified by table
          sortPositions = getSortPositions(destTable.getSortCols(), destTable.getCols());
          sortOrder = getSortOrders(destTable.getSortCols(), destTable.getCols());
        } else if (HiveConf.getBoolVar(this.parseCtx.getConf(), HiveConf.ConfVars.HIVE_SORT_WHEN_BUCKETING) &&
            !bucketPositions.isEmpty()) {
          // We use clustered columns as sort columns
          sortPositions = new ArrayList<>(bucketPositions);
          sortOrder = sortPositions.stream().map(e -> 1).collect(Collectors.toList());
        } else {
          // Infer sort columns from operator tree
          sortPositions = Lists.newArrayList();
          sortOrder = Lists.newArrayList();
          inferSortPositions(fsParent, sortPositions, sortOrder);
        }
        List<ColumnInfo> colInfos = fsParent.getSchema().getSignature();
        bucketColumns = getPositionsToExprNodes(bucketPositions, colInfos);
      }
      List<Integer> sortNullOrder = new ArrayList<Integer>();
      for (int order : sortOrder) {
        sortNullOrder.add(order == 1 ? 0 : 1); // for asc, nulls first; for desc, nulls last
      }
      LOG.debug("Got sort order");
      for (int i : sortPositions) {
        LOG.debug("sort position " + i);
      }
      for (int i : sortOrder) {
        LOG.debug("sort order " + i);
      }
      for (int i : sortNullOrder) {
        LOG.debug("sort null order " + i);
      }

      // update file sink descriptor
      fsOp.getConf().setMultiFileSpray(false);
      fsOp.getConf().setNumFiles(1);
      fsOp.getConf().setTotalFiles(1);

      // Create ReduceSink operator
      ReduceSinkOperator rsOp = getReduceSinkOp(partitionPositions, sortPositions, sortOrder,
          sortNullOrder, customSortExprs, customSortOrder, customNullOrder, allRSCols, bucketColumns, numBuckets,
          fsParent, fsOp.getConf().getWriteType());
      // we have to make sure not to reorder the child operators as it might cause weird behavior in the tasks at
      // the same level. when there is auto stats gather at the same level as another operation then it might
      // cause unnecessary preemption. Maintaining the order here to avoid such preemption and possible errors
      // Ref TEZ-3296
      fsParent.getChildOperators().remove(rsOp);
      fsParent.getChildOperators().add(fsOpIndex, rsOp);
      rsOp.getConf().setBucketingVersion(fsOp.getConf().getBucketingVersion());

      List<ExprNodeDesc> descs = new ArrayList<ExprNodeDesc>(allRSCols.size());
      List<String> colNames = new ArrayList<String>();
      String colName;
      final List<ColumnInfo> fileSinkSchema = fsOp.getSchema().getSignature();
      for (int i = 0; i < allRSCols.size(); i++) {
        ExprNodeDesc col = allRSCols.get(i);
        ExprNodeDesc newColumnExpr = null;
        colName = col.getExprString();
        colNames.add(colName);
        if (partitionPositions.contains(i) || sortPositions.contains(i)) {
          newColumnExpr = (new ExprNodeColumnDesc(col.getTypeInfo(), ReduceField.KEY.toString()+"."+colName, null, false));
        } else {
          newColumnExpr = (new ExprNodeColumnDesc(col.getTypeInfo(), ReduceField.VALUE.toString()+"."+colName, null, false));
        }

        // make sure column type matches with expected types in FS op
        if(i < fileSinkSchema.size()) {
          final ColumnInfo fsColInfo = fileSinkSchema.get(i);
          if (!newColumnExpr.getTypeInfo().equals(fsColInfo.getType())) {
            newColumnExpr = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
                .createConversionCast(newColumnExpr, (PrimitiveTypeInfo) fsColInfo.getType());
          }
        }
        descs.add(newColumnExpr);
      }
      RowSchema selRS = new RowSchema(fsParent.getSchema());

      if (bucketColumns != null && !bucketColumns.isEmpty()) {
        customSortExprs.add(BUCKET_SORT_EXPRESSION);
      }

      for (Function<List<ExprNodeDesc>, ExprNodeDesc> customSortExpr : customSortExprs) {
        ExprNodeDesc colExpr = customSortExpr.apply(allRSCols);
        String customSortColName = colExpr.getExprString();
        TypeInfo customSortColTypeInfo = colExpr.getTypeInfo();

        descs.add(new ExprNodeColumnDesc(customSortColTypeInfo, ReduceField.KEY + "." + customSortColName,
            null, false));
        colNames.add(customSortColName);
        ColumnInfo ci = new ColumnInfo(
            customSortColName, customSortColTypeInfo, selRS.getSignature().get(0).getTabAlias(), true, true);
        selRS.getSignature().add(ci);
        rsOp.getSchema().getSignature().add(ci);
      }

      // Create SelectDesc
      SelectDesc selConf = new SelectDesc(descs, colNames);

      // Create Select Operator
      SelectOperator selOp = (SelectOperator) OperatorFactory.getAndMakeChild(
          selConf, selRS, rsOp);

      // link SEL to FS
      fsOp.getParentOperators().clear();
      fsOp.getParentOperators().add(selOp);
      selOp.getChildOperators().add(fsOp);

      // Set if partition sorted or partition bucket sorted
      fsOp.getConf().setDpSortState(FileSinkDesc.DPSortState.PARTITION_SORTED);
      if (bucketColumns!=null && !bucketColumns.isEmpty()) {
        fsOp.getConf().setDpSortState(FileSinkDesc.DPSortState.PARTITION_BUCKET_SORTED);
      }

      // update partition column info in FS descriptor
      fsOp.getConf().setPartitionCols(rsOp.getConf().getPartitionCols());

      rsOp.setStatistics(rsOp.getParentOperators().get(0).getStatistics());

      LOG.info("Inserted " + rsOp.getOperatorId() + " and " + selOp.getOperatorId()
          + " as parent of " + fsOp.getOperatorId() + " and child of " + fsParent.getOperatorId());

      parseCtx.setReduceSinkAddedBySortedDynPartition(true);
      return null;
    }

    private boolean allStaticPartitions(Operator<? extends OperatorDesc> op, List<ExprNodeDesc> allRSCols,
        final DynamicPartitionCtx dynPartCtx) {

      if (op.getColumnExprMap() == null) {
        // find first operator upstream with valid (non-null) column expression map
        for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
          if (parent.getColumnExprMap() != null) {
            op = parent;
            break;
          }
        }
      }
      // No mappings for any columns
      if (op.getColumnExprMap() == null) {
        return false;
      }

      List<String> referencedSortColumnNames = new LinkedList<>();
      List<Function<List<ExprNodeDesc>, ExprNodeDesc>> customSortExprs = dynPartCtx.getCustomSortExpressions();

      if (customSortExprs != null && !customSortExprs.isEmpty()) {
        Set<ExprNodeColumnDesc> columnDescs = new HashSet<>();

        // Find relevant column descs (e.g. _col0, _col2) for each sort expression
        for (Function<List<ExprNodeDesc>, ExprNodeDesc> customSortExpr : customSortExprs) {
          ExprNodeDesc sortExpressionForRSSchema = customSortExpr.apply(allRSCols);
          columnDescs.addAll(ExprNodeDescUtils.findAllColumnDescs(sortExpressionForRSSchema));
        }

        for (ExprNodeColumnDesc columnDesc : columnDescs) {
          referencedSortColumnNames.add(columnDesc.getColumn());
        }

      } else {
        int numDpCols = dynPartCtx.getNumDPCols();
        int numCols = op.getSchema().getColumnNames().size();
        referencedSortColumnNames.addAll(op.getSchema().getColumnNames().subList(numCols - numDpCols, numCols));
      }

      for(String dpCol : referencedSortColumnNames) {
        ExprNodeDesc end = ExprNodeDescUtils.findConstantExprOrigin(dpCol, op);
        if (!(end instanceof ExprNodeConstantDesc)) {
          // There is at least 1 column with no constant mapping -> we will need to do the sorting
          return false;
        }
      }

      // All columns had constant mappings
      return true;
    }

    // Remove RS and SEL introduced by enforce bucketing/sorting config
    // Convert PARENT -> RS -> SEL -> FS to PARENT -> FS
    private boolean removeRSInsertedByEnforceBucketing(FileSinkOperator fsOp) {

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

        // RS is expected to have exactly ONE child
        assert(rsToRemove.getChildOperators().size() == 1);

        Operator<? extends OperatorDesc> rsChildToRemove = rsToRemove.getChildOperators().get(0);

        if (!(rsChildToRemove instanceof SelectOperator) || rsParent.getSchema().getSignature().size()
            != rsChildToRemove.getSchema().getSignature().size()) {
          // if schema size cannot be matched, then it could be because of constant folding
          // converting partition column expression to constant expression. The constant
          // expression will then get pruned by column pruner since it will not reference to
          // any columns.
          return false;
        }

        // if child is select and contains expression which isn't column it shouldn't
        // be removed because otherwise we will end up with different types/schema later
        // while introducing select for RS
        for(ExprNodeDesc expr: rsChildToRemove.getColumnExprMap().values()){
          if(!(expr instanceof ExprNodeColumnDesc)){
            return false;
          }
        }

        List<Operator<? extends OperatorDesc>> rsGrandChildren = rsChildToRemove.getChildOperators();

        rsParent.getChildOperators().remove(rsToRemove);
        rsParent.getChildOperators().addAll(rsGrandChildren);


        // fix grandchildren
        for (Operator<? extends OperatorDesc> rsGrandChild: rsGrandChildren) {
          rsGrandChild.getParentOperators().clear();
          rsGrandChild.getParentOperators().add(rsParent);
        }
        LOG.info("Removed " + rsToRemove.getOperatorId() + " and " + rsChildToRemove.getOperatorId()
            + " as it was introduced by enforce bucketing/sorting.");
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

    // Try to infer possible sort columns in the query
    // i.e. the sequence must be pRS-SEL*-fsParent
    // Returns true if columns could be inferred, false otherwise
    private void inferSortPositions(Operator<? extends OperatorDesc> fsParent,
        List<Integer> sortPositions, List<Integer> sortOrder) throws SemanticException {
      // If it is not a SEL operator, we bail out
      if (!(fsParent instanceof SelectOperator)) {
        return;
      }
      SelectOperator pSel = (SelectOperator) fsParent;
      Operator<? extends OperatorDesc> parent = pSel;
      while (!(parent instanceof ReduceSinkOperator)) {
        if (parent.getNumParent() != 1 ||
            !(parent instanceof SelectOperator)) {
          return;
        }
        parent = parent.getParentOperators().get(0);
      }
      // Backtrack SEL columns to pRS
      List<ExprNodeDesc> selColsInPRS =
          ExprNodeDescUtils.backtrack(pSel.getConf().getColList(), pSel, parent);
      ReduceSinkOperator pRS = (ReduceSinkOperator) parent;
      for (int i = 0; i < pRS.getConf().getKeyCols().size(); i++) {
        ExprNodeDesc col = pRS.getConf().getKeyCols().get(i);
        int pos = selColsInPRS.indexOf(col);
        if (pos == -1) {
          sortPositions.clear();
          sortOrder.clear();
          return;
        }
        sortPositions.add(pos);
        sortOrder.add(pRS.getConf().getOrder().charAt(i) == '+' ? 1 : 0); // 1 asc, 0 desc
      }
    }

    public ReduceSinkOperator getReduceSinkOp(List<Integer> partitionPositions, List<Integer> sortPositions,
        List<Integer> sortOrder, List<Integer> sortNullOrder,
        List<Function<List<ExprNodeDesc>, ExprNodeDesc>> customSortExprs,
        List<Integer> customSortOrder, List<Integer> customSortNullOrder,
        ArrayList<ExprNodeDesc> allCols, ArrayList<ExprNodeDesc> bucketColumns,
        int numBuckets, Operator<? extends OperatorDesc> parent, AcidUtils.Operation writeType) {

      // Order of KEY columns, if custom sort is present partition and bucket columns are disregarded:
      // 0) Custom sort expressions
      //                              1) Partition columns
      //                              2) Bucket number column
      //                 3) Sort columns

      boolean customSortExprPresent = customSortExprs != null && !customSortExprs.isEmpty();

      Set<Integer> keyColsPosInVal = Sets.newLinkedHashSet();
      ArrayList<ExprNodeDesc> keyCols = Lists.newArrayList();
      List<Integer> newSortOrder = Lists.newArrayList();
      List<Integer> newSortNullOrder = Lists.newArrayList();

      if (customSortExprPresent) {
        partitionPositions = new ArrayList<>();
        bucketColumns = new ArrayList<>();
        numBuckets = -1;
      }

      keyColsPosInVal.addAll(partitionPositions);
      if (bucketColumns != null && !bucketColumns.isEmpty()) {
        keyColsPosInVal.add(-1);
      }
      keyColsPosInVal.addAll(sortPositions);

      Integer order = 1;
      // by default partition and bucket columns are sorted in ascending order
      if (sortOrder != null && !sortOrder.isEmpty()) {
        if (sortOrder.get(0) == 0) {
          order = 0;
        }
      }

      for (Integer ignored : keyColsPosInVal) {
        newSortOrder.add(order);
      }

      if (customSortExprPresent) {
        for (int i = 0; i < customSortExprs.size() - customSortOrder.size(); i++) {
          newSortOrder.add(order);
        }
        newSortOrder.addAll(customSortOrder);
      }

      String orderStr = "";
      for (Integer i : newSortOrder) {
        if (i == 1) {
          orderStr += "+";
        } else {
          orderStr += "-";
        }
      }

      // if partition and bucket columns are sorted in ascending order, by default
      // nulls come first; otherwise nulls come last
      Integer nullOrder = order == 1 ? 0 : 1;
      if (sortNullOrder != null && !sortNullOrder.isEmpty()) {
        if (sortNullOrder.get(0) == 0) {
          nullOrder = 0;
        } else {
          nullOrder = 1;
        }
      }

      for (Integer ignored : keyColsPosInVal) {
        newSortNullOrder.add(nullOrder);
      }

      if (customSortExprPresent) {
        for (int i = 0; i < customSortExprs.size() - customSortNullOrder.size(); i++) {
          newSortNullOrder.add(nullOrder);
        }
        newSortNullOrder.addAll(customSortNullOrder);
      }

      String nullOrderStr = "";
      for (Integer i : newSortNullOrder) {
        if (i == 0) {
          nullOrderStr += "a";
        } else {
          nullOrderStr += "z";
        }
      }

      Map<String, ExprNodeDesc> colExprMap = Maps.newHashMap();
      ArrayList<ExprNodeDesc> partCols = Lists.newArrayList();

      for (Function<List<ExprNodeDesc>, ExprNodeDesc> customSortExpr : customSortExprs) {
        ExprNodeDesc colExpr = customSortExpr.apply(allCols);
        // Custom sort expressions are marked as KEYs, which is required for sorting the rows that are going for
        // a particular reducer instance. They also need to be marked as 'partition' columns for MapReduce shuffle
        // phase, in order to gather the same keys to the same reducer instances.
        keyCols.add(colExpr);
        partCols.add(colExpr);
      }

      // we will clone here as RS will update bucket column key with its
      // corresponding with bucket number and hence their OIs
      for (Integer idx : keyColsPosInVal) {
        if (idx == -1) {
          keyCols.add(BUCKET_SORT_EXPRESSION.apply(allCols));
        } else {
          keyCols.add(allCols.get(idx).clone());
        }
      }

      ArrayList<ExprNodeDesc> valCols = Lists.newArrayList();
      for (int i = 0; i < allCols.size(); i++) {
        if (!keyColsPosInVal.contains(i)) {
          valCols.add(allCols.get(i).clone());
        }
      }

      for (Integer idx : partitionPositions) {
        partCols.add(allCols.get(idx).clone());
      }

      // in the absence of SORTED BY clause, the sorted dynamic partition insert
      // should honor the ordering of records provided by ORDER BY in SELECT statement
      ReduceSinkOperator parentRSOp = OperatorUtils.findSingleOperatorUpstream(parent,
          ReduceSinkOperator.class);
      if (parentRSOp != null && parseCtx.getQueryProperties().hasOuterOrderBy()) {
        String parentRSOpOrder = parentRSOp.getConf().getOrder();
        String parentRSOpNullOrder = parentRSOp.getConf().getNullOrder();
        if (parentRSOpOrder != null && !parentRSOpOrder.isEmpty() && sortPositions.isEmpty()) {
          keyCols.addAll(parentRSOp.getConf().getKeyCols());
          orderStr += parentRSOpOrder;
          nullOrderStr += parentRSOpNullOrder;
        }
      }

      // map _col0 to KEY._col0, etc
      Map<String, String> nameMapping = new HashMap<>();
      ArrayList<String> keyColNames = Lists.newArrayList();
      Set<String> computedFields = new HashSet<>();
      for (ExprNodeDesc keyCol : keyCols) {
        String keyColName = keyCol.getExprString();
        keyColNames.add(keyColName);
        colExprMap.put(Utilities.ReduceField.KEY + "." +keyColName, keyCol);
        nameMapping.put(keyColName, Utilities.ReduceField.KEY + "." + keyColName);
      }
      ArrayList<String> valColNames = Lists.newArrayList();
      for (ExprNodeDesc valCol : valCols) {
        String colName = valCol.getExprString();
        valColNames.add(colName);
        colExprMap.put(Utilities.ReduceField.VALUE + "." + colName, valCol);
        if (nameMapping.containsKey(colName)) {
          computedFields.add(nameMapping.get(colName));
        }
        nameMapping.put(colName, Utilities.ReduceField.VALUE + "." + colName);
      }

      // Create Key/Value TableDesc. When the operator plan is split into MR tasks,
      // the reduce operator will initialize Extract operator with information
      // from Key and Value TableDesc
      List<FieldSchema> fields = PlanUtils.getFieldSchemasFromColumnList(keyCols,
          keyColNames, 0, "");
      TableDesc keyTable = PlanUtils.getReduceKeyTableDesc(fields, orderStr, nullOrderStr);
      List<FieldSchema> valFields = PlanUtils.getFieldSchemasFromColumnList(valCols,
          valColNames, 0, "");
      TableDesc valueTable = PlanUtils.getReduceValueTableDesc(valFields);
      List<List<Integer>> distinctColumnIndices = Lists.newArrayList();

      // Number of reducers is set to default (-1)
      ReduceSinkDesc rsConf = new ReduceSinkDesc(keyCols, keyCols.size(), valCols,
          keyColNames, distinctColumnIndices, valColNames, -1, partCols, -1, keyTable,
          valueTable, writeType);
      rsConf.setBucketCols(bucketColumns);
      rsConf.setNumBuckets(numBuckets);
      rsConf.getComputedFields().addAll(computedFields);

      ArrayList<ColumnInfo> signature = new ArrayList<>();
      for (int index = 0; index < parent.getSchema().getSignature().size(); index++) {
        ColumnInfo colInfo = new ColumnInfo(parent.getSchema().getSignature().get(index));
        colInfo.setInternalName(nameMapping.get(colInfo.getInternalName()));
        signature.add(colInfo);
      }
      ReduceSinkOperator op = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(
          rsConf, new RowSchema(signature), parent);
      rsConf.addComputedField(Utilities.ReduceField.KEY + "." + BUCKET_SORT_EXPRESSION.apply(allCols).getExprString());
      for (Function<List<ExprNodeDesc>, ExprNodeDesc> customSortExpr : customSortExprs) {
        rsConf.addComputedField(Utilities.ReduceField.KEY + "." + customSortExpr.apply(allCols).getExprString());
      }
      op.setColumnExprMap(colExprMap);
      return op;
    }

    /**
     * Get the sort positions for the sort columns.
     *
     * @param tabSortCols
     * @param tabCols
     * @return
     */
    private List<Integer> getSortPositions(List<Order> tabSortCols,
        List<FieldSchema> tabCols) {
      List<Integer> sortPositions = Lists.newArrayList();
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

    /**
     * Get the sort order for the sort columns.
     *
     * @param tabSortCols
     * @param tabCols
     * @return
     */
    private List<Integer> getSortOrders(List<Order> tabSortCols,
        List<FieldSchema> tabCols) {
      List<Integer> sortOrders = Lists.newArrayList();
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

    // the idea is to estimate how many number of writers this insert can spun up.
    // Writers are proportional to number of partitions being inserted i.e cardinality of the partition columns
    //  if these writers are more than number of writers allowed within the memory pool (estimated) we go ahead with
    //  adding extra RS
    //  The way max number of writers allowed are computed based on
    //  (executor/container memory) * (percentage of memory taken by orc)
    //  and dividing that by max memory (stripe size) taken by a single writer.
    private boolean shouldDo(List<Integer> partitionPos, Operator<? extends OperatorDesc> fsParent) {

      int threshold = HiveConf.getIntVar(this.parseCtx.getConf(),
          HiveConf.ConfVars.HIVE_OPT_SORT_DYNAMIC_PARTITION_THRESHOLD);
      long MAX_WRITERS = -1;

      switch (threshold) {
      case -1:
        return false;
      case 0:
        break;
      case 1:
        return true;
      default:
        MAX_WRITERS = threshold;
        break;
      }

      Statistics tStats = fsParent.getStatistics();
      if (tStats == null) {
        return true;
      }

      List<ColStatistics> colStats = tStats.getColumnStats();
      if (colStats == null || colStats.isEmpty()) {
        return true;
      }
      long partCardinality = 1;

      // compute cardinality for partition columns
      for (Integer idx : partitionPos) {
        ColumnInfo ci = fsParent.getSchema().getSignature().get(idx);
        ColStatistics partStats = fsParent.getStatistics().getColumnStatisticsFromColName(ci.getInternalName());
        if (partStats == null) {
          // statistics for this partition are for some reason not available
          return true;
        }
        partCardinality = partCardinality * partStats.getCountDistint();
      }

      if (MAX_WRITERS < 0) {
        double orcMemPool = this.parseCtx.getConf().getDouble(OrcConf.MEMORY_POOL.getHiveConfName(),
            (Double) OrcConf.MEMORY_POOL.getDefaultValue());
        long orcStripSize = this.parseCtx.getConf().getLong(OrcConf.STRIPE_SIZE.getHiveConfName(),
            (Long) OrcConf.STRIPE_SIZE.getDefaultValue());
        MemoryInfo memoryInfo = new MemoryInfo(this.parseCtx.getConf());
        LOG.debug("Memory info during SDPO opt: {}", memoryInfo);
        long executorMem = memoryInfo.getMaxExecutorMemory();
        MAX_WRITERS = (long) (executorMem * orcMemPool) / orcStripSize;

      }
      if (partCardinality <= MAX_WRITERS) {
        return false;
      }
      return true;
    }
  }
}
