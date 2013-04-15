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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.QB;
import org.apache.hadoop.hive.ql.parse.QBJoinTree;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.TableAccessAnalyzer;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;
import org.apache.hadoop.util.ReflectionUtils;

//try to replace a bucket map join with a sorted merge map join
abstract public class AbstractSMBJoinProc extends AbstractBucketJoinProc implements NodeProcessor {

  private static final Log LOG = LogFactory
    .getLog(SortedMergeBucketMapJoinOptimizer.class.getName());

  public AbstractSMBJoinProc(ParseContext pctx) {
    super(pctx);
  }

  public AbstractSMBJoinProc() {
    super();
  }

  @Override
  abstract public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
         Object... nodeOutputs) throws SemanticException;

  /*
   * Return true or false based on whether a bucketed mapjoin can be converted successfully to
   * a sort-merge map join operator. The following checks are performed:
   * a. The mapjoin under consideration is a bucketed mapjoin.
   * b. All the tables are sorted in same order, such that join columns is equal to or a prefix
   *    of the sort columns.
   */
  protected boolean canConvertBucketMapJoinToSMBJoin(MapJoinOperator mapJoinOp,
    Stack<Node> stack,
    SortBucketJoinProcCtx smbJoinContext,
    Object... nodeOutputs) throws SemanticException {

    // Check whether the mapjoin is a bucketed mapjoin.
    // The above can be ascertained by checking the big table bucket -> small table buckets
    // mapping in the mapjoin descriptor.
    // First check if this map-join operator is already a BucketMapJoin or not. If not give up
    // we are only trying to convert a BucketMapJoin to sort-BucketMapJoin.
    if (mapJoinOp.getConf().getAliasBucketFileNameMapping() == null
      || mapJoinOp.getConf().getAliasBucketFileNameMapping().size() == 0) {
      return false;
    }

    boolean tableEligibleForBucketedSortMergeJoin = true;
    QBJoinTree joinCxt = this.pGraphContext.getMapJoinContext()
      .get(mapJoinOp);
    if (joinCxt == null) {
      return false;
    }
    String[] srcs = joinCxt.getBaseSrc();
    for (int srcPos = 0; srcPos < srcs.length; srcPos++) {
      srcs[srcPos] = QB.getAppendedAliasFromId(joinCxt.getId(), srcs[srcPos]);
    }

    // All the tables/partitions columns should be sorted in the same order
    // For example, if tables A and B are being joined on columns c1, c2 and c3
    // which are the sorted and bucketed columns. The join would work, as long
    // c1, c2 and c3 are sorted in the same order.
    List<Order> sortColumnsFirstTable = new ArrayList<Order>();

    for (int pos = 0; pos < srcs.length; pos++) {
      tableEligibleForBucketedSortMergeJoin = tableEligibleForBucketedSortMergeJoin
        && isEligibleForBucketSortMergeJoin(smbJoinContext,
             pGraphContext,
             mapJoinOp.getConf().getKeys().get((byte) pos),
             joinCxt,
             srcs,
             pos,
             sortColumnsFirstTable);
    }
    if (!tableEligibleForBucketedSortMergeJoin) {
      // this is a mapjoin but not suited for a sort merge bucket map join. check outer joins
      MapJoinProcessor.checkMapJoin(mapJoinOp.getConf().getPosBigTable(),
            mapJoinOp.getConf().getConds());
      return false;
    }

    smbJoinContext.setSrcs(srcs);
    return true;
  }


  // Convert the bucket map-join operator to a sort-merge map join operator
  protected SMBMapJoinOperator convertBucketMapJoinToSMBJoin(MapJoinOperator mapJoinOp,
    SortBucketJoinProcCtx smbJoinContext,
    ParseContext parseContext) {

    String[] srcs = smbJoinContext.getSrcs();
    SMBMapJoinOperator smbJop = new SMBMapJoinOperator(mapJoinOp);
    SMBJoinDesc smbJoinDesc = new SMBJoinDesc(mapJoinOp.getConf());
    smbJop.setConf(smbJoinDesc);
    HashMap<Byte, String> tagToAlias = new HashMap<Byte, String>();
    for (int i = 0; i < srcs.length; i++) {
      tagToAlias.put((byte) i, srcs[i]);
    }
    smbJoinDesc.setTagToAlias(tagToAlias);

    int indexInListMapJoinNoReducer =
      this.pGraphContext.getListMapJoinOpsNoReducer().indexOf(mapJoinOp);
    if (indexInListMapJoinNoReducer >= 0 ) {
      this.pGraphContext.getListMapJoinOpsNoReducer().remove(indexInListMapJoinNoReducer);
      this.pGraphContext.getListMapJoinOpsNoReducer().add(indexInListMapJoinNoReducer, smbJop);
    }

    Map<String, DummyStoreOperator> aliasToSink =
        new HashMap<String, DummyStoreOperator>();
    // For all parents (other than the big table), insert a dummy store operator
    /* Consider a query like:
     *
     * select * from
     *   (subq1 --> has a filter)
     *   join
     *   (subq2 --> has a filter)
     * on some key
     *
     * Let us assume that subq1 is the small table (either specified by the user or inferred
     * automatically). The following operator tree will be created:
     *
     * TableScan (subq1) --> Select --> Filter --> DummyStore
     *                                                         \
     *                                                          \     SMBJoin
     *                                                          /
     *                                                         /
     * TableScan (subq2) --> Select --> Filter
     */

    List<Operator<? extends OperatorDesc>> parentOperators = mapJoinOp.getParentOperators();
    for (int i = 0; i < parentOperators.size(); i++) {
      Operator<? extends OperatorDesc> par = parentOperators.get(i);
      int index = par.getChildOperators().indexOf(mapJoinOp);
      par.getChildOperators().remove(index);
      if (i == smbJoinDesc.getPosBigTable()) {
        par.getChildOperators().add(index, smbJop);
      }
      else {
        DummyStoreOperator dummyStoreOp = new DummyStoreOperator();
        par.getChildOperators().add(index, dummyStoreOp);

        List<Operator<? extends OperatorDesc>> childrenOps =
            new ArrayList<Operator<? extends OperatorDesc>>();
        childrenOps.add(smbJop);
        dummyStoreOp.setChildOperators(childrenOps);

        List<Operator<? extends OperatorDesc>> parentOps =
            new ArrayList<Operator<? extends OperatorDesc>>();
        parentOps.add(par);
        dummyStoreOp.setParentOperators(parentOps);

        aliasToSink.put(srcs[i], dummyStoreOp);
        smbJop.getParentOperators().remove(i);
        smbJop.getParentOperators().add(i, dummyStoreOp);
      }
    }
    smbJoinDesc.setAliasToSink(aliasToSink);

    List<Operator<? extends OperatorDesc>> childOps = mapJoinOp.getChildOperators();
    for (int i = 0; i < childOps.size(); i++) {
      Operator<? extends OperatorDesc> child = childOps.get(i);
      int index = child.getParentOperators().indexOf(mapJoinOp);
      child.getParentOperators().remove(index);
      child.getParentOperators().add(index, smbJop);
    }
    parseContext.getSmbMapJoinContext().put(smbJop,
        parseContext.getMapJoinContext().get(mapJoinOp));
    parseContext.getMapJoinContext().remove(mapJoinOp);
    parseContext.getOpParseCtx().put(smbJop, parseContext.getOpParseCtx().get(mapJoinOp));

    return smbJop;
  }

  /**
   * Whether this table is eligible for a sort-merge join.
   *
   * @param pctx                  parse context
   * @param op                    map join operator being considered
   * @param joinTree              join tree being considered
   * @param alias                 table alias in the join tree being checked
   * @param pos                   position of the table
   * @param sortColumnsFirstTable The names and order of the sorted columns for the first table.
   *                              It is not initialized when pos = 0.
   * @return
   * @throws SemanticException
   */
  private boolean isEligibleForBucketSortMergeJoin(
    SortBucketJoinProcCtx smbJoinContext,
    ParseContext pctx,
    List<ExprNodeDesc> keys,
    QBJoinTree joinTree,
    String[] aliases,
    int pos,
    List<Order> sortColumnsFirstTable) throws SemanticException {
    String alias = aliases[pos];
    Map<TableScanOperator, Table> topToTable = this.pGraphContext
      .getTopToTable();

    /*
     * Consider a query like:
     *
     * select -- mapjoin(subq1) --  * from
     * (select a.key, a.value from tbl1 a) subq1
     *   join
     * (select a.key, a.value from tbl2 a) subq2
     * on subq1.key = subq2.key;
     *
     * aliasToOpInfo contains the SelectOperator for subq1 and subq2.
     * We need to traverse the tree (using TableAccessAnalyzer) to get to the base
     * table. If the object being map-joined is a base table, then aliasToOpInfo
     * contains the TableScanOperator, and TableAccessAnalyzer is a no-op.
     */
    Operator<? extends OperatorDesc> topOp = joinTree.getAliasToOpInfo().get(alias);
    if (topOp == null) {
      return false;
    }

    // get all join columns from join keys
    List<String> joinCols = toColumns(keys);
    if (joinCols == null || joinCols.isEmpty()) {
      return false;
    }

    TableScanOperator tso = TableAccessAnalyzer.genRootTableScan(topOp, joinCols);
    if (tso == null) {
      return false;
    }

    // For nested sub-queries, the alias mapping is not maintained in QB currently.
    /*
     * Consider a query like:
     *
     * select count(*) from
     *   (
     *     select key, count(*) from
     *       (
     *         select --mapjoin(a)-- a.key as key, a.value as val1, b.value as val2
     *         from tbl1 a join tbl2 b on a.key = b.key
     *       ) subq1
     *     group by key
     *   ) subq2;
     *
     * The table alias should be subq2:subq1:a which needs to be fetched from topOps.
     */
    if (pGraphContext.getTopOps().containsValue(tso)) {
      for (Map.Entry<String, Operator<? extends OperatorDesc>> topOpEntry :
        this.pGraphContext.getTopOps().entrySet()) {
        if (topOpEntry.getValue() == tso) {
          alias = topOpEntry.getKey();
          aliases[pos] = alias;
          break;
        }
      }
    }
    else {
      // Ideally, this should never happen, and this should be an assert.
      return false;
    }

    Table tbl = topToTable.get(tso);
    if (tbl.isPartitioned()) {
      PrunedPartitionList prunedParts = null;
      try {
        prunedParts = pGraphContext.getOpToPartList().get(tso);
        if (prunedParts == null) {
          prunedParts = PartitionPruner.prune(tbl, pGraphContext
            .getOpToPartPruner().get(tso), pGraphContext.getConf(), alias,
          pGraphContext.getPrunedPartitions());
          pGraphContext.getOpToPartList().put(tso, prunedParts);
        }
      } catch (HiveException e) {
        LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
        throw new SemanticException(e.getMessage(), e);
      }
      List<Partition> partitions = prunedParts.getNotDeniedPartns();
      // Populate the names and order of columns for the first partition of the
      // first table
      if ((pos == 0) && (partitions != null) && (!partitions.isEmpty())) {
        Partition firstPartition = partitions.get(0);
        sortColumnsFirstTable.addAll(firstPartition.getSortCols());
      }

      for (Partition partition : prunedParts.getNotDeniedPartns()) {
        if (!checkSortColsAndJoinCols(partition.getSortCols(),
          joinCols,
          sortColumnsFirstTable)) {
          return false;
        }
      }
      return true;
    }

    // Populate the names and order of columns for the first table
    if (pos == 0) {
      sortColumnsFirstTable.addAll(tbl.getSortCols());
    }

    return checkSortColsAndJoinCols(tbl.getSortCols(),
      joinCols,
      sortColumnsFirstTable);
  }

  private boolean checkSortColsAndJoinCols(List<Order> sortCols,
      List<String> joinCols,
      List<Order> sortColumnsFirstPartition) {

    if (sortCols == null || sortCols.size() < joinCols.size()) {
      return false;
    }

    // A join is eligible for a sort-merge join, only if it is eligible for
    // a bucketized map join. So, we dont need to check for bucketized map
    // join here. We are guaranteed that the join keys contain all the
    // bucketized keys (note that the order need not be the same).
    List<String> sortColNames = new ArrayList<String>();

    // The join columns should contain all the sort columns
    // The sort columns of all the tables should be in the same order
    // compare the column names and the order with the first table/partition.
    for (int pos = 0; pos < sortCols.size(); pos++) {
      Order o = sortCols.get(pos);

      if (o.getOrder() != sortColumnsFirstPartition.get(pos).getOrder()) {
        return false;
      }
      sortColNames.add(o.getCol());
    }

    // The column names and order (ascending/descending) matched
    // The first 'n' sorted columns should be the same as the joinCols, where
    // 'n' is the size of join columns.
    // For eg: if the table is sorted by (a,b,c), it is OK to convert if the join is
    // on (a), (a,b), or any combination of (a,b,c):
    //   (a,b,c), (a,c,b), (c,a,b), (c,b,a), (b,c,a), (b,a,c)
    // but it is not OK to convert if the join is on (a,c)
    return sortColNames.subList(0, joinCols.size()).containsAll(joinCols);
  }

  // Can the join operator be converted to a sort-merge join operator ?
  // It is already verified that the join can be converted to a bucket map join
  protected boolean checkConvertJoinToSMBJoin(
    JoinOperator joinOperator,
    SortBucketJoinProcCtx smbJoinContext,
    ParseContext pGraphContext) throws SemanticException {

    boolean tableEligibleForBucketedSortMergeJoin = true;
    QBJoinTree joinCtx = pGraphContext.getJoinContext().get(joinOperator);

    if (joinCtx == null) {
      return false;
    }
    String[] srcs = joinCtx.getBaseSrc();

    // All the tables/partitions columns should be sorted in the same order
    // For example, if tables A and B are being joined on columns c1, c2 and c3
    // which are the sorted and bucketed columns. The join would work, as long
    // c1, c2 and c3 are sorted in the same order.
    List<Order> sortColumnsFirstTable = new ArrayList<Order>();

    for (int pos = 0; pos < srcs.length; pos++) {
      tableEligibleForBucketedSortMergeJoin = tableEligibleForBucketedSortMergeJoin &&
        isEligibleForBucketSortMergeJoin(smbJoinContext,
                      pGraphContext,
                      smbJoinContext.getKeyExprMap().get((byte)pos),
                      joinCtx,
                      srcs,
                      pos,
                      sortColumnsFirstTable);
    }

    smbJoinContext.setSrcs(srcs);
    return true;
  }

  // Can the join operator be converted to a sort-merge join operator ?
  protected boolean canConvertJoinToSMBJoin(
    JoinOperator joinOperator,
    SortBucketJoinProcCtx smbJoinContext,
    ParseContext pGraphContext) throws SemanticException {
    boolean canConvert =
      canConvertJoinToBucketMapJoin(
        joinOperator,
        pGraphContext,
        smbJoinContext
      );

    if (!canConvert) {
      return false;
    }

    return checkConvertJoinToSMBJoin(joinOperator, smbJoinContext, pGraphContext);
  }

  // Can the join operator be converted to a bucket map-merge join operator ?
  protected boolean canConvertJoinToBucketMapJoin(
    JoinOperator joinOp,
    ParseContext pGraphContext,
    SortBucketJoinProcCtx context) throws SemanticException {

    // This has already been inspected and rejected
    if (context.getRejectedJoinOps().contains(joinOp)) {
      return false;
    }

    QBJoinTree joinCtx = pGraphContext.getJoinContext().get(joinOp);
    if (joinCtx == null) {
      return false;
    }

    Class<? extends BigTableSelectorForAutoSMJ> bigTableMatcherClass = null;
    try {
      bigTableMatcherClass =
        (Class<? extends BigTableSelectorForAutoSMJ>)
          (Class.forName(HiveConf.getVar(pGraphContext.getConf(),
            HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN_BIGTABLE_SELECTOR)));
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e.getMessage());
    }

    BigTableSelectorForAutoSMJ bigTableMatcher =
      (BigTableSelectorForAutoSMJ) ReflectionUtils.newInstance(bigTableMatcherClass, null);
    int bigTablePosition =
      bigTableMatcher.getBigTablePosition(pGraphContext, joinOp);
    context.setBigTablePosition(bigTablePosition);
    String joinAlias =
      bigTablePosition == 0 ?
        joinCtx.getLeftAlias() : joinCtx.getRightAliases()[bigTablePosition - 1];
    joinAlias = QB.getAppendedAliasFromId(joinCtx.getId(), joinAlias);

    Map<Byte, List<ExprNodeDesc>> keyExprMap  = new HashMap<Byte, List<ExprNodeDesc>>();
    List<Operator<? extends OperatorDesc>> parentOps = joinOp.getParentOperators();
    // get the join keys from parent ReduceSink operators
    for (Operator<? extends OperatorDesc> parentOp : parentOps) {
      ReduceSinkDesc rsconf = ((ReduceSinkOperator)parentOp).getConf();
      Byte tag = (byte) rsconf.getTag();
      List<ExprNodeDesc> keys = rsconf.getKeyCols();
      keyExprMap.put(tag, keys);
    }

    context.setKeyExprMap(keyExprMap);
    String[] srcs = joinCtx.getBaseSrc();
    for (int srcPos = 0; srcPos < srcs.length; srcPos++) {
      srcs[srcPos] = QB.getAppendedAliasFromId(joinCtx.getId(), srcs[srcPos]);
    }

    // Given a candidate map-join, can this join be converted.
    // The candidate map-join was derived from the pluggable sort merge join big
    // table matcher.
    return checkConvertBucketMapJoin(
      pGraphContext,
      context,
      joinCtx,
      keyExprMap,
      joinAlias,
      Arrays.asList(srcs));
  }

  // Convert the join operator to a bucket map-join join operator
  protected MapJoinOperator convertJoinToBucketMapJoin(
    JoinOperator joinOp,
    SortBucketJoinProcCtx joinContext,
    ParseContext parseContext) throws SemanticException {
    MapJoinOperator mapJoinOp = MapJoinProcessor.convertMapJoin(
      parseContext.getOpParseCtx(),
      joinOp,
      pGraphContext.getJoinContext().get(joinOp),
      joinContext.getBigTablePosition(),
      false,
      false);
    // Remove the join operator from the query join context
    parseContext.getMapJoinContext().put(mapJoinOp, parseContext.getJoinContext().get(joinOp));
    parseContext.getJoinContext().remove(joinOp);
    convertMapJoinToBucketMapJoin(mapJoinOp, joinContext);
    return mapJoinOp;
  }

  // Convert the join operator to a sort-merge join operator
  protected void convertJoinToSMBJoin(
    JoinOperator joinOp,
    SortBucketJoinProcCtx smbJoinContext,
    ParseContext parseContext) throws SemanticException {
    MapJoinOperator mapJoinOp = convertJoinToBucketMapJoin(joinOp, smbJoinContext, parseContext);
    SMBMapJoinOperator smbMapJoinOp =
        convertBucketMapJoinToSMBJoin(mapJoinOp, smbJoinContext, parseContext);
    smbMapJoinOp.setConvertedAutomaticallySMBJoin(true);
  }
}
