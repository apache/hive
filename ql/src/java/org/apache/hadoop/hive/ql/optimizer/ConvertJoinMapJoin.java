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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MuxOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TezDummyStoreOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.GenTezUtils;
import org.apache.hadoop.hive.ql.parse.OptimizeTezProcContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.CommonMergeJoinDesc;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OpTraits;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConvertJoinMapJoin is an optimization that replaces a common join
 * (aka shuffle join) with a map join (aka broadcast or fragment replicate
 * join when possible. Map joins have restrictions on which joins can be
 * converted (e.g.: full outer joins cannot be handled as map joins) as well
 * as memory restrictions (one side of the join has to fit into memory).
 */
public class ConvertJoinMapJoin implements NodeProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(ConvertJoinMapJoin.class.getName());


  @Override
  /*
   * (non-Javadoc) we should ideally not modify the tree we traverse. However,
   * since we need to walk the tree at any time when we modify the operator, we
   * might as well do it here.
   */
  public Object
      process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
          throws SemanticException {

    OptimizeTezProcContext context = (OptimizeTezProcContext) procCtx;

    JoinOperator joinOp = (JoinOperator) nd;
    long maxSize = context.conf.getLongVar(HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);

    TezBucketJoinProcCtx tezBucketJoinProcCtx = new TezBucketJoinProcCtx(context.conf);
    if (!context.conf.getBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN)) {
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      Object retval = checkAndConvertSMBJoin(context, joinOp, tezBucketJoinProcCtx);
      if (retval == null) {
        return retval;
      } else {
        fallbackToReduceSideJoin(joinOp, context);
        return null;
      }
    }

    // if we have traits, and table info is present in the traits, we know the
    // exact number of buckets. Else choose the largest number of estimated
    // reducers from the parent operators.
    int numBuckets = -1;
    if (context.conf.getBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_BUCKET_MAPJOIN_TEZ)) {
      numBuckets = estimateNumBuckets(joinOp, true);
    } else {
      numBuckets = 1;
    }
    LOG.info("Estimated number of buckets " + numBuckets);
    int mapJoinConversionPos = getMapJoinConversionPos(joinOp, context, numBuckets, false, maxSize, true);
    if (mapJoinConversionPos < 0) {
      Object retval = checkAndConvertSMBJoin(context, joinOp, tezBucketJoinProcCtx);
      if (retval == null) {
        return retval;
      } else {
        // only case is full outer join with SMB enabled which is not possible. Convert to regular
        // join.
        fallbackToReduceSideJoin(joinOp, context);
        return null;
      }
    }

    if (numBuckets > 1) {
      if (context.conf.getBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_BUCKET_MAPJOIN_TEZ)) {
        if (convertJoinBucketMapJoin(joinOp, context, mapJoinConversionPos, tezBucketJoinProcCtx)) {
          return null;
        }
      }
    }

    // check if we can convert to map join no bucket scaling.
    LOG.info("Convert to non-bucketed map join");
    if (numBuckets != 1) {
      mapJoinConversionPos = getMapJoinConversionPos(joinOp, context, 1, false, maxSize, true);
    }
    if (mapJoinConversionPos < 0) {
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      fallbackToReduceSideJoin(joinOp, context);
      return null;
    }

    MapJoinOperator mapJoinOp = convertJoinMapJoin(joinOp, context, mapJoinConversionPos, true);
    // map join operator by default has no bucket cols and num of reduce sinks
    // reduced by 1
    mapJoinOp.setOpTraits(new OpTraits(null, -1, null, joinOp.getOpTraits().getNumReduceSinks()));
    mapJoinOp.setStatistics(joinOp.getStatistics());
    // propagate this change till the next RS
    for (Operator<? extends OperatorDesc> childOp : mapJoinOp.getChildOperators()) {
      setAllChildrenTraits(childOp, mapJoinOp.getOpTraits());
    }

    return null;
  }

  @SuppressWarnings("unchecked")
  private Object checkAndConvertSMBJoin(OptimizeTezProcContext context, JoinOperator joinOp,
      TezBucketJoinProcCtx tezBucketJoinProcCtx) throws SemanticException {
    // we cannot convert to bucket map join, we cannot convert to
    // map join either based on the size. Check if we can convert to SMB join.
    if ((HiveConf.getBoolVar(context.conf, ConfVars.HIVE_AUTO_SORTMERGE_JOIN) == false)
      || ((!HiveConf.getBoolVar(context.conf, ConfVars.HIVE_AUTO_SORTMERGE_JOIN_REDUCE))
          && joinOp.getOpTraits().getNumReduceSinks() >= 2)) {
      fallbackToReduceSideJoin(joinOp, context);
      return null;
    }
    Class<? extends BigTableSelectorForAutoSMJ> bigTableMatcherClass = null;
    try {
      String selector = HiveConf.getVar(context.parseContext.getConf(),
          HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN_BIGTABLE_SELECTOR);
      bigTableMatcherClass =
          JavaUtils.loadClass(selector);
    } catch (ClassNotFoundException e) {
      throw new SemanticException(e.getMessage());
    }

    BigTableSelectorForAutoSMJ bigTableMatcher =
        ReflectionUtils.newInstance(bigTableMatcherClass, null);
    JoinDesc joinDesc = joinOp.getConf();
    JoinCondDesc[] joinCondns = joinDesc.getConds();
    Set<Integer> joinCandidates = MapJoinProcessor.getBigTableCandidates(joinCondns);
    if (joinCandidates.isEmpty()) {
      // This is a full outer join. This can never be a map-join
      // of any type. So return false.
      return false;
    }
    int mapJoinConversionPos =
        bigTableMatcher.getBigTablePosition(context.parseContext, joinOp, joinCandidates);
    if (mapJoinConversionPos < 0) {
      // contains aliases from sub-query
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      fallbackToReduceSideJoin(joinOp, context);
      return null;
    }

    if (checkConvertJoinSMBJoin(joinOp, context, mapJoinConversionPos, tezBucketJoinProcCtx)) {
      convertJoinSMBJoin(joinOp, context, mapJoinConversionPos,
          tezBucketJoinProcCtx.getNumBuckets(), true);
    } else {
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      fallbackToReduceSideJoin(joinOp, context);
    }
    return null;
  }

  // replaces the join operator with a new CommonJoinOperator, removes the
  // parent reduce sinks
  private void convertJoinSMBJoin(JoinOperator joinOp, OptimizeTezProcContext context,
      int mapJoinConversionPos, int numBuckets, boolean adjustParentsChildren)
      throws SemanticException {
    MapJoinDesc mapJoinDesc = null;
    if (adjustParentsChildren) {
      mapJoinDesc = MapJoinProcessor.getMapJoinDesc(context.conf,
            joinOp, joinOp.getConf().isLeftInputJoin(), joinOp.getConf().getBaseSrc(),
            joinOp.getConf().getMapAliases(), mapJoinConversionPos, true);
    } else {
      JoinDesc joinDesc = joinOp.getConf();
      // retain the original join desc in the map join.
      mapJoinDesc =
          new MapJoinDesc(
                  MapJoinProcessor.getKeys(joinOp.getConf().isLeftInputJoin(),
                  joinOp.getConf().getBaseSrc(), joinOp).getSecond(),
                  null, joinDesc.getExprs(), null, null,
                  joinDesc.getOutputColumnNames(), mapJoinConversionPos, joinDesc.getConds(),
                  joinDesc.getFilters(), joinDesc.getNoOuterJoin(), null);
      mapJoinDesc.setNullSafes(joinDesc.getNullSafes());
      mapJoinDesc.setFilterMap(joinDesc.getFilterMap());
      mapJoinDesc.setResidualFilterExprs(joinDesc.getResidualFilterExprs());
      mapJoinDesc.resetOrder();
    }

    CommonMergeJoinOperator mergeJoinOp =
        (CommonMergeJoinOperator) OperatorFactory.get(joinOp.getCompilationOpContext(),
            new CommonMergeJoinDesc(numBuckets, mapJoinConversionPos, mapJoinDesc),
            joinOp.getSchema());
    int numReduceSinks = joinOp.getOpTraits().getNumReduceSinks();
    OpTraits opTraits = new OpTraits(joinOp.getOpTraits().getBucketColNames(), numBuckets,
      joinOp.getOpTraits().getSortCols(), numReduceSinks);
    mergeJoinOp.setOpTraits(opTraits);
    mergeJoinOp.setStatistics(joinOp.getStatistics());

    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
      int pos = parentOp.getChildOperators().indexOf(joinOp);
      parentOp.getChildOperators().remove(pos);
      parentOp.getChildOperators().add(pos, mergeJoinOp);
    }

    for (Operator<? extends OperatorDesc> childOp : joinOp.getChildOperators()) {
      int pos = childOp.getParentOperators().indexOf(joinOp);
      childOp.getParentOperators().remove(pos);
      childOp.getParentOperators().add(pos, mergeJoinOp);
    }

    List<Operator<? extends OperatorDesc>> childOperators = mergeJoinOp.getChildOperators();
    List<Operator<? extends OperatorDesc>> parentOperators = mergeJoinOp.getParentOperators();

    childOperators.clear();
    parentOperators.clear();
    childOperators.addAll(joinOp.getChildOperators());
    parentOperators.addAll(joinOp.getParentOperators());
    mergeJoinOp.getConf().setGenJoinKeys(false);

    if (adjustParentsChildren) {
      mergeJoinOp.getConf().setGenJoinKeys(true);
      List<Operator<? extends OperatorDesc>> newParentOpList = new ArrayList<Operator<? extends OperatorDesc>>();
      for (Operator<? extends OperatorDesc> parentOp : mergeJoinOp.getParentOperators()) {
        for (Operator<? extends OperatorDesc> grandParentOp : parentOp.getParentOperators()) {
          grandParentOp.getChildOperators().remove(parentOp);
          grandParentOp.getChildOperators().add(mergeJoinOp);
          newParentOpList.add(grandParentOp);
        }
      }
      mergeJoinOp.getParentOperators().clear();
      mergeJoinOp.getParentOperators().addAll(newParentOpList);
      List<Operator<? extends OperatorDesc>> parentOps =
          new ArrayList<Operator<? extends OperatorDesc>>(mergeJoinOp.getParentOperators());
      for (Operator<? extends OperatorDesc> parentOp : parentOps) {
        int parentIndex = mergeJoinOp.getParentOperators().indexOf(parentOp);
        if (parentIndex == mapJoinConversionPos) {
          continue;
        }

        // insert the dummy store operator here
        DummyStoreOperator dummyStoreOp = new TezDummyStoreOperator(
            mergeJoinOp.getCompilationOpContext());
        dummyStoreOp.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>());
        dummyStoreOp.setChildOperators(new ArrayList<Operator<? extends OperatorDesc>>());
        dummyStoreOp.getChildOperators().add(mergeJoinOp);
        int index = parentOp.getChildOperators().indexOf(mergeJoinOp);
        parentOp.getChildOperators().remove(index);
        parentOp.getChildOperators().add(index, dummyStoreOp);
        dummyStoreOp.getParentOperators().add(parentOp);
        mergeJoinOp.getParentOperators().remove(parentIndex);
        mergeJoinOp.getParentOperators().add(parentIndex, dummyStoreOp);
      }
    }
    mergeJoinOp.cloneOriginalParentsList(mergeJoinOp.getParentOperators());
  }

  private void setAllChildrenTraits(Operator<? extends OperatorDesc> currentOp, OpTraits opTraits) {
    if (currentOp instanceof ReduceSinkOperator) {
      return;
    }
    currentOp.setOpTraits(new OpTraits(opTraits.getBucketColNames(),
      opTraits.getNumBuckets(), opTraits.getSortCols(), opTraits.getNumReduceSinks()));
    for (Operator<? extends OperatorDesc> childOp : currentOp.getChildOperators()) {
      if ((childOp instanceof ReduceSinkOperator) || (childOp instanceof GroupByOperator)) {
        break;
      }
      setAllChildrenTraits(childOp, opTraits);
    }
  }

  private boolean convertJoinBucketMapJoin(JoinOperator joinOp, OptimizeTezProcContext context,
      int bigTablePosition, TezBucketJoinProcCtx tezBucketJoinProcCtx) throws SemanticException {

    if (!checkConvertJoinBucketMapJoin(joinOp, context, bigTablePosition, tezBucketJoinProcCtx)) {
      LOG.info("Check conversion to bucket map join failed.");
      return false;
    }

    MapJoinOperator mapJoinOp = convertJoinMapJoin(joinOp, context, bigTablePosition, true);
    if (mapJoinOp == null) {
      LOG.debug("Conversion to bucket map join failed.");
      return false;
    }
    MapJoinDesc joinDesc = mapJoinOp.getConf();
    joinDesc.setBucketMapJoin(true);

    // we can set the traits for this join operator
    OpTraits opTraits = new OpTraits(joinOp.getOpTraits().getBucketColNames(),
        tezBucketJoinProcCtx.getNumBuckets(), null, joinOp.getOpTraits().getNumReduceSinks());
    mapJoinOp.setOpTraits(opTraits);
    mapJoinOp.setStatistics(joinOp.getStatistics());
    setNumberOfBucketsOnChildren(mapJoinOp);

    // Once the conversion is done, we can set the partitioner to bucket cols on the small table
    Map<String, Integer> bigTableBucketNumMapping = new HashMap<String, Integer>();
    bigTableBucketNumMapping.put(joinDesc.getBigTableAlias(), tezBucketJoinProcCtx.getNumBuckets());
    joinDesc.setBigTableBucketNumMapping(bigTableBucketNumMapping);

    return true;
  }

  /*
   * This method tries to convert a join to an SMB. This is done based on
   * traits. If the sorted by columns are the same as the join columns then, we
   * can convert the join to an SMB. Otherwise retain the bucket map join as it
   * is still more efficient than a regular join.
   */
  private boolean checkConvertJoinSMBJoin(JoinOperator joinOp, OptimizeTezProcContext context,
      int bigTablePosition, TezBucketJoinProcCtx tezBucketJoinProcCtx) throws SemanticException {

    ReduceSinkOperator bigTableRS =
        (ReduceSinkOperator) joinOp.getParentOperators().get(bigTablePosition);
    int numBuckets = bigTableRS.getParentOperators().get(0).getOpTraits().getNumBuckets();

    int size = -1;
    for (Operator<?> parentOp : joinOp.getParentOperators()) {
      // each side better have 0 or more RS. if either side is unbalanced, cannot convert.
      // This is a workaround for now. Right fix would be to refactor code in the
      // MapRecordProcessor and ReduceRecordProcessor with respect to the sources.
      @SuppressWarnings({"rawtypes","unchecked"})
      Set<ReduceSinkOperator> set =
          OperatorUtils.findOperatorsUpstream(parentOp.getParentOperators(),
              ReduceSinkOperator.class);
      if (size < 0) {
        size = set.size();
        continue;
      }

      if (((size > 0) && (set.size() > 0)) || ((size == 0) && (set.size() == 0))) {
        continue;
      } else {
        return false;
      }
    }

    // the sort and bucket cols have to match on both sides for this
    // transformation of the join operation
    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
      if (!(parentOp instanceof ReduceSinkOperator)) {
        // could be mux/demux operators. Currently not supported
        LOG.info("Found correlation optimizer operators. Cannot convert to SMB at this time.");
        return false;
      }
      ReduceSinkOperator rsOp = (ReduceSinkOperator) parentOp;
      if (checkColEquality(rsOp.getParentOperators().get(0).getOpTraits().getSortCols(), rsOp
          .getOpTraits().getSortCols(), rsOp.getColumnExprMap(), tezBucketJoinProcCtx, false) == false) {
        LOG.info("We cannot convert to SMB because the sort column names do not match.");
        return false;
      }

      if (checkColEquality(rsOp.getParentOperators().get(0).getOpTraits().getBucketColNames(), rsOp
          .getOpTraits().getBucketColNames(), rsOp.getColumnExprMap(), tezBucketJoinProcCtx, true)
          == false) {
        LOG.info("We cannot convert to SMB because bucket column names do not match.");
        return false;
      }
    }

    if (numBuckets < 0) {
      numBuckets = bigTableRS.getConf().getNumReducers();
    }
    tezBucketJoinProcCtx.setNumBuckets(numBuckets);
    LOG.info("We can convert the join to an SMB join.");
    return true;
  }

  private void setNumberOfBucketsOnChildren(Operator<? extends OperatorDesc> currentOp) {
    int numBuckets = currentOp.getOpTraits().getNumBuckets();
    for (Operator<? extends OperatorDesc>op : currentOp.getChildOperators()) {
      if (!(op instanceof ReduceSinkOperator) && !(op instanceof GroupByOperator)) {
        op.getOpTraits().setNumBuckets(numBuckets);
        setNumberOfBucketsOnChildren(op);
      }
    }
  }

  /*
   * If the parent reduce sink of the big table side has the same emit key cols as its parent, we
   * can create a bucket map join eliminating the reduce sink.
   */
  private boolean checkConvertJoinBucketMapJoin(JoinOperator joinOp,
      OptimizeTezProcContext context, int bigTablePosition,
      TezBucketJoinProcCtx tezBucketJoinProcCtx) throws SemanticException {
    // bail on mux-operator because mux operator masks the emit keys of the
    // constituent reduce sinks
    if (!(joinOp.getParentOperators().get(0) instanceof ReduceSinkOperator)) {
      LOG.info("Operator is " + joinOp.getParentOperators().get(0).getName() +
          ". Cannot convert to bucket map join");
      return false;
    }

    ReduceSinkOperator rs = (ReduceSinkOperator) joinOp.getParentOperators().get(bigTablePosition);
    List<List<String>> parentColNames = rs.getOpTraits().getBucketColNames();
    Operator<? extends OperatorDesc> parentOfParent = rs.getParentOperators().get(0);
    List<List<String>> grandParentColNames = parentOfParent.getOpTraits().getBucketColNames();
    int numBuckets = parentOfParent.getOpTraits().getNumBuckets();
    // all keys matched.
    if (checkColEquality(grandParentColNames, parentColNames, rs.getColumnExprMap(),
        tezBucketJoinProcCtx, true) == false) {
      LOG.info("No info available to check for bucket map join. Cannot convert");
      return false;
    }

    /*
     * this is the case when the big table is a sub-query and is probably already bucketed by the
     * join column in say a group by operation
     */
    if (numBuckets < 0) {
      numBuckets = rs.getConf().getNumReducers();
    }
    tezBucketJoinProcCtx.setNumBuckets(numBuckets);
    return true;
  }

  private boolean checkColEquality(List<List<String>> grandParentColNames,
      List<List<String>> parentColNames, Map<String, ExprNodeDesc> colExprMap,
      TezBucketJoinProcCtx tezBucketJoinProcCtx, boolean strict) {

    if ((grandParentColNames == null) || (parentColNames == null)) {
      return false;
    }

    if ((parentColNames != null) && (parentColNames.isEmpty() == false)) {
      for (List<String> listBucketCols : grandParentColNames) {
        // can happen if this operator does not carry forward the previous bucketing columns
        // for e.g. another join operator which does not carry one of the sides' key columns
        if (listBucketCols.isEmpty()) {
          continue;
        }
        int colCount = 0;
        // parent op is guaranteed to have a single list because it is a reduce sink
        for (String colName : parentColNames.get(0)) {
          if (listBucketCols.size() <= colCount) {
            // can happen with virtual columns. RS would add the column to its output columns
            // but it would not exist in the grandparent output columns or exprMap.
            return false;
          }
          // all columns need to be at least a subset of the parentOfParent's bucket cols
          ExprNodeDesc exprNodeDesc = colExprMap.get(colName);
          if (exprNodeDesc instanceof ExprNodeColumnDesc) {
            if (((ExprNodeColumnDesc) exprNodeDesc).getColumn()
                .equals(listBucketCols.get(colCount))) {
              colCount++;
            } else {
              break;
            }
          }

          if (colCount == parentColNames.get(0).size()) {
            if (strict) {
              if (colCount == listBucketCols.size()) {
                return true;
              } else {
                return false;
              }
            } else {
              return true;
            }
          }
        }
      }
      return false;
    }
    return false;
  }

  /**
   * Obtain big table position for join.
   *
   * @param joinOp join operator
   * @param context optimization context
   * @param buckets bucket count for Bucket Map Join conversion consideration or reduce count
   * for Dynamic Hash Join conversion consideration
   * @param skipJoinTypeChecks whether to skip join type checking
   * @param maxSize size threshold for Map Join conversion
   * @param checkHashTableEntries whether to check threshold for distinct keys in hash table for Map Join
   * @return returns big table position or -1 if it cannot be determined
   * @throws SemanticException
   */
  public int getMapJoinConversionPos(JoinOperator joinOp, OptimizeTezProcContext context,
      int buckets, boolean skipJoinTypeChecks, long maxSize, boolean checkHashTableEntries)
              throws SemanticException {
    if (!skipJoinTypeChecks) {
      /*
       * HIVE-9038: Join tests fail in tez when we have more than 1 join on the same key and there is
       * an outer join down the join tree that requires filterTag. We disable this conversion to map
       * join here now. We need to emulate the behavior of HashTableSinkOperator as in MR or create a
       * new operation to be able to support this. This seems like a corner case enough to special
       * case this for now.
       */
      if (joinOp.getConf().getConds().length > 1) {
        boolean hasOuter = false;
        for (JoinCondDesc joinCondDesc : joinOp.getConf().getConds()) {
          switch (joinCondDesc.getType()) {
          case JoinDesc.INNER_JOIN:
          case JoinDesc.LEFT_SEMI_JOIN:
          case JoinDesc.UNIQUE_JOIN:
            hasOuter = false;
            break;

          case JoinDesc.FULL_OUTER_JOIN:
          case JoinDesc.LEFT_OUTER_JOIN:
          case JoinDesc.RIGHT_OUTER_JOIN:
            hasOuter = true;
            break;

          default:
            throw new SemanticException("Unknown join type " + joinCondDesc.getType());
          }
        }
        if (hasOuter) {
          return -1;
        }
      }
    }
    Set<Integer> bigTableCandidateSet =
        MapJoinProcessor.getBigTableCandidates(joinOp.getConf().getConds());
    int bigTablePosition = -1;
    // big input cumulative row count
    long bigInputCumulativeCardinality = -1L;
    // stats of the big input
    Statistics bigInputStat = null;

    // bigTableFound means we've encountered a table that's bigger than the
    // max. This table is either the the big table or we cannot convert.
    boolean foundInputNotFittingInMemory = false;

    // total size of the inputs
    long totalSize = 0;

    for (int pos = 0; pos < joinOp.getParentOperators().size(); pos++) {
      Operator<? extends OperatorDesc> parentOp = joinOp.getParentOperators().get(pos);

      Statistics currInputStat = parentOp.getStatistics();
      if (currInputStat == null) {
        LOG.warn("Couldn't get statistics from: " + parentOp);
        return -1;
      }

      long inputSize = currInputStat.getDataSize();

      boolean currentInputNotFittingInMemory = false;
      if ((bigInputStat == null)
              || ((bigInputStat != null) && (inputSize > bigInputStat.getDataSize()))) {

        if (foundInputNotFittingInMemory) {
          // cannot convert to map join; we've already chosen a big table
          // on size and there's another one that's bigger.
          return -1;
        }

        if (inputSize/buckets > maxSize) {
          if (!bigTableCandidateSet.contains(pos)) {
            // can't use the current table as the big table, but it's too
            // big for the map side.
            return -1;
          }

          currentInputNotFittingInMemory = true;
          foundInputNotFittingInMemory = true;
        }
      }

      long currentInputCumulativeCardinality;
      if (foundInputNotFittingInMemory) {
        currentInputCumulativeCardinality = -1L;
      } else {
        Long cardinality = computeCumulativeCardinality(parentOp);
        if (cardinality == null) {
          // We could not get stats, we cannot convert
          return -1;
        }
        currentInputCumulativeCardinality = cardinality;
      }

      // This input is the big table if it is contained in the big candidates set, and either:
      // 1) we have not chosen a big table yet, or
      // 2) it has been chosen as the big table above, or
      // 3) the cumulative cardinality for this input is higher, or
      // 4) the cumulative cardinality is equal, but the size is bigger,
      boolean selectedBigTable = bigTableCandidateSet.contains(pos) &&
              (bigInputStat == null || currentInputNotFittingInMemory ||
                      (!foundInputNotFittingInMemory && (currentInputCumulativeCardinality > bigInputCumulativeCardinality ||
                              (currentInputCumulativeCardinality == bigInputCumulativeCardinality && inputSize > bigInputStat.getDataSize()))));

      if (bigInputStat != null && selectedBigTable) {
        // We are replacing the current big table with a new one, thus
        // we need to count the current one as a map table then.
        totalSize += bigInputStat.getDataSize();
        // Check if number of distinct keys is larger than given max
        // number of entries for HashMap. If it is, we do not convert.
        if (checkHashTableEntries && !checkNumberOfEntriesForHashTable(joinOp, bigTablePosition, context)) {
          return -1;
        }
      } else if (!selectedBigTable) {
        // This is not the first table and we are not using it as big table,
        // in fact, we're adding this table as a map table
        totalSize += inputSize;
        // Check if number of distinct keys is larger than given max
        // number of entries for HashMap. If it is, we do not convert.
        if (checkHashTableEntries && !checkNumberOfEntriesForHashTable(joinOp, pos, context)) {
          return -1;
        }
      }

      if (totalSize/buckets > maxSize) {
        // sum of small tables size in this join exceeds configured limit
        // hence cannot convert.
        return -1;
      }

      if (selectedBigTable) {
        bigTablePosition = pos;
        bigInputCumulativeCardinality = currentInputCumulativeCardinality;
        bigInputStat = currInputStat;
      }

    }

    return bigTablePosition;
  }

  // This is akin to CBO cumulative cardinality model
  private static Long computeCumulativeCardinality(Operator<? extends OperatorDesc> op) {
    long cumulativeCardinality = 0L;
    if (op instanceof CommonJoinOperator) {
      // Choose max
      for (Operator<? extends OperatorDesc> inputOp : op.getParentOperators()) {
        Long inputCardinality = computeCumulativeCardinality(inputOp);
        if (inputCardinality == null) {
          return null;
        }
        if (inputCardinality > cumulativeCardinality) {
          cumulativeCardinality = inputCardinality;
        }
      }
    } else {
      // Choose cumulative
      for (Operator<? extends OperatorDesc> inputOp : op.getParentOperators()) {
        Long inputCardinality = computeCumulativeCardinality(inputOp);
        if (inputCardinality == null) {
          return null;
        }
        cumulativeCardinality += inputCardinality;
      }
    }
    Statistics currInputStat = op.getStatistics();
    if (currInputStat == null) {
      LOG.warn("Couldn't get statistics from: " + op);
      return null;
    }
    cumulativeCardinality += currInputStat.getNumRows();
    return cumulativeCardinality;
  }

  /*
   * Once we have decided on the map join, the tree would transform from
   *
   *        |                   |
   *       Join               MapJoin
   *       / \                /   \
   *     RS   RS   --->     RS    TS (big table)
   *    /      \           /
   *   TS       TS        TS (small table)
   *
   * for tez.
   */
  public MapJoinOperator convertJoinMapJoin(JoinOperator joinOp, OptimizeTezProcContext context,
      int bigTablePosition, boolean removeReduceSink) throws SemanticException {
    // bail on mux operator because currently the mux operator masks the emit keys
    // of the constituent reduce sinks.
    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
      if (parentOp instanceof MuxOperator) {
        return null;
      }
    }

    // can safely convert the join to a map join.
    MapJoinOperator mapJoinOp =
        MapJoinProcessor.convertJoinOpMapJoinOp(context.conf, joinOp,
            joinOp.getConf().isLeftInputJoin(), joinOp.getConf().getBaseSrc(),
            joinOp.getConf().getMapAliases(), bigTablePosition, true, removeReduceSink);
    mapJoinOp.getConf().setHybridHashJoin(HiveConf.getBoolVar(context.conf,
        HiveConf.ConfVars.HIVEUSEHYBRIDGRACEHASHJOIN));
    List<ExprNodeDesc> joinExprs = mapJoinOp.getConf().getKeys().values().iterator().next();
    if (joinExprs.size() == 0) {  // In case of cross join, we disable hybrid grace hash join
      mapJoinOp.getConf().setHybridHashJoin(false);
    }

    Operator<? extends OperatorDesc> parentBigTableOp =
        mapJoinOp.getParentOperators().get(bigTablePosition);
    if (parentBigTableOp instanceof ReduceSinkOperator) {
      Operator<?> parentSelectOpOfBigTableOp = parentBigTableOp.getParentOperators().get(0);
      if (removeReduceSink) {
        for (Operator<?> p : parentBigTableOp.getParentOperators()) {
          // we might have generated a dynamic partition operator chain. Since
          // we're removing the reduce sink we need do remove that too.
          Set<Operator<?>> dynamicPartitionOperators = new HashSet<Operator<?>>();
          Map<Operator<?>, AppMasterEventOperator> opEventPairs = new HashMap<>();
          for (Operator<?> c : p.getChildOperators()) {
            AppMasterEventOperator event = findDynamicPartitionBroadcast(c);
            if (event != null) {
              dynamicPartitionOperators.add(c);
              opEventPairs.put(c, event);
            }
          }
          for (Operator<?> c : dynamicPartitionOperators) {
            if (context.pruningOpsRemovedByPriorOpt.isEmpty() ||
                !context.pruningOpsRemovedByPriorOpt.contains(opEventPairs.get(c))) {
              p.removeChild(c);
              // at this point we've found the fork in the op pipeline that has the pruning as a child plan.
              LOG.info("Disabling dynamic pruning for: "
                  + ((DynamicPruningEventDesc) opEventPairs.get(c).getConf()).getTableScan().getName()
                  + ". Need to be removed together with reduce sink");
            }
          }
          for (Operator<?> op : dynamicPartitionOperators) {
            context.pruningOpsRemovedByPriorOpt.add(opEventPairs.get(op));
          }
        }

        mapJoinOp.getParentOperators().remove(bigTablePosition);
        if (!(mapJoinOp.getParentOperators().contains(parentBigTableOp.getParentOperators().get(0)))) {
          mapJoinOp.getParentOperators().add(bigTablePosition,
              parentBigTableOp.getParentOperators().get(0));
        }
        parentBigTableOp.getParentOperators().get(0).removeChild(parentBigTableOp);
      }

      for (Operator<? extends OperatorDesc>op : mapJoinOp.getParentOperators()) {
        if (!(op.getChildOperators().contains(mapJoinOp))) {
          op.getChildOperators().add(mapJoinOp);
        }
        op.getChildOperators().remove(joinOp);
      }

      // Remove semijoin Op if there is any.
      // The semijoin branch can potentially create a task level cycle
      // with the hashjoin except when it is dynamically partitioned hash
      // join which takes place in a separate task.
      if (context.parseContext.getRsOpToTsOpMap().size() > 0
              && removeReduceSink) {
        removeCycleCreatingSemiJoinOps(mapJoinOp, parentSelectOpOfBigTableOp,
                context.parseContext);
      }
    }

    return mapJoinOp;
  }

  // Remove any semijoin branch associated with hashjoin's parent's operator
  // pipeline which can cause a cycle after hashjoin optimization.
  private void removeCycleCreatingSemiJoinOps(MapJoinOperator mapjoinOp,
                                              Operator<?> parentSelectOpOfBigTable,
                                              ParseContext parseContext) throws SemanticException {
    Map<ReduceSinkOperator, TableScanOperator> semiJoinMap =
            new HashMap<ReduceSinkOperator, TableScanOperator>();
    for (Operator<?> op : parentSelectOpOfBigTable.getChildOperators()) {
      if (!(op instanceof SelectOperator)) {
        continue;
      }

      while (op.getChildOperators().size() > 0) {
        op = op.getChildOperators().get(0);
      }

      // If not ReduceSink Op, skip
      if (!(op instanceof ReduceSinkOperator)) {
        continue;
      }

      ReduceSinkOperator rs = (ReduceSinkOperator) op;
      TableScanOperator ts = parseContext.getRsOpToTsOpMap().get(rs);
      if (ts == null) {
        // skip, no semijoin branch
        continue;
      }

      // Found a semijoin branch.
      for (Operator<?> parent : mapjoinOp.getParentOperators()) {
        if (!(parent instanceof ReduceSinkOperator)) {
          continue;
        }

        Set<TableScanOperator> tsOps = OperatorUtils.findOperatorsUpstream(parent,
                TableScanOperator.class);
        for (TableScanOperator parentTS : tsOps) {
          // If the parent is same as the ts, then we have a cycle.
          if (ts == parentTS) {
            semiJoinMap.put(rs, ts);
            break;
          }
        }
      }
    }
    if (semiJoinMap.size() > 0) {
      for (ReduceSinkOperator rs : semiJoinMap.keySet()) {
        GenTezUtils.removeBranch(rs);
        GenTezUtils.removeSemiJoinOperator(parseContext, rs,
                semiJoinMap.get(rs));
      }
    }
  }

  private AppMasterEventOperator findDynamicPartitionBroadcast(Operator<?> parent) {

    for (Operator<?> op : parent.getChildOperators()) {
      while (op != null) {
        if (op instanceof AppMasterEventOperator && op.getConf() instanceof DynamicPruningEventDesc) {
          // found dynamic partition pruning operator
          return (AppMasterEventOperator)op;
        }
        if (op instanceof ReduceSinkOperator || op instanceof FileSinkOperator) {
          // crossing reduce sink or file sink means the pruning isn't for this parent.
          break;
        }

        if (op.getChildOperators().size() != 1) {
          // dynamic partition pruning pipeline doesn't have multiple children
          break;
        }

        op = op.getChildOperators().get(0);
      }
    }

    return null;
  }

  /**
   * Estimate the number of buckets in the join, using the parent operators' OpTraits and/or
   * parent operators' number of reducers
   * @param joinOp
   * @param useOpTraits  Whether OpTraits should be used for the estimate.
   * @return
   */
  private static int estimateNumBuckets(JoinOperator joinOp, boolean useOpTraits) {
    int numBuckets = -1;
    int estimatedBuckets = -1;

    for (Operator<? extends OperatorDesc>parentOp : joinOp.getParentOperators()) {
      if (parentOp.getOpTraits().getNumBuckets() > 0) {
        numBuckets = (numBuckets < parentOp.getOpTraits().getNumBuckets()) ?
            parentOp.getOpTraits().getNumBuckets() : numBuckets;
      }

      if (parentOp instanceof ReduceSinkOperator) {
        ReduceSinkOperator rs = (ReduceSinkOperator) parentOp;
        estimatedBuckets = (estimatedBuckets < rs.getConf().getNumReducers()) ?
            rs.getConf().getNumReducers() : estimatedBuckets;
      }
    }

    if (!useOpTraits) {
      // Ignore the value we got from OpTraits.
      // The logic below will fall back to the estimate from numReducers
      numBuckets = -1;
    }

    if (numBuckets <= 0) {
      numBuckets = estimatedBuckets;
      if (numBuckets <= 0) {
        numBuckets = 1;
      }
    }

    return numBuckets;
  }

  private boolean convertJoinDynamicPartitionedHashJoin(JoinOperator joinOp, OptimizeTezProcContext context)
    throws SemanticException {
    // Attempt dynamic partitioned hash join
    // Since we don't have big table index yet, must start with estimate of numReducers
    int numReducers = estimateNumBuckets(joinOp, false);
    LOG.info("Try dynamic partitioned hash join with estimated " + numReducers + " reducers");
    int bigTablePos = getMapJoinConversionPos(joinOp, context, numReducers, false,
            context.conf.getLongVar(HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD),
            false);
    if (bigTablePos >= 0) {
      // Now that we have the big table index, get real numReducers value based on big table RS
      ReduceSinkOperator bigTableParentRS =
          (ReduceSinkOperator) (joinOp.getParentOperators().get(bigTablePos));
      numReducers = bigTableParentRS.getConf().getNumReducers();
      LOG.debug("Real big table reducers = " + numReducers);

      MapJoinOperator mapJoinOp = convertJoinMapJoin(joinOp, context, bigTablePos, false);
      if (mapJoinOp != null) {
        LOG.info("Selected dynamic partitioned hash join");
        mapJoinOp.getConf().setDynamicPartitionHashJoin(true);
        // Set OpTraits for dynamically partitioned hash join:
        // bucketColNames: Re-use previous joinOp's bucketColNames. Parent operators should be
        //   reduce sink, which should have bucket columns based on the join keys.
        // numBuckets: set to number of reducers
        // sortCols: This is an unsorted join - no sort cols
        OpTraits opTraits = new OpTraits(
            joinOp.getOpTraits().getBucketColNames(),
            numReducers,
            null,
            joinOp.getOpTraits().getNumReduceSinks());
        mapJoinOp.setOpTraits(opTraits);
        mapJoinOp.setStatistics(joinOp.getStatistics());
        // propagate this change till the next RS
        for (Operator<? extends OperatorDesc> childOp : mapJoinOp.getChildOperators()) {
          setAllChildrenTraits(childOp, mapJoinOp.getOpTraits());
        }
        return true;
      }
    }

    return false;
  }

  private void fallbackToReduceSideJoin(JoinOperator joinOp, OptimizeTezProcContext context)
      throws SemanticException {
    if (context.conf.getBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN) &&
        context.conf.getBoolVar(HiveConf.ConfVars.HIVEDYNAMICPARTITIONHASHJOIN)) {
      if (convertJoinDynamicPartitionedHashJoin(joinOp, context)) {
        return;
      }
    }

    int pos = getMapJoinConversionPos(joinOp, context, estimateNumBuckets(joinOp, false),
                  true, Long.MAX_VALUE, false);
    if (pos < 0) {
      LOG.info("Could not get a valid join position. Defaulting to position 0");
      pos = 0;
    }
    // we are just converting to a common merge join operator. The shuffle
    // join in map-reduce case.
    LOG.info("Fallback to common merge join operator");
    convertJoinSMBJoin(joinOp, context, pos, 0, false);
  }

  /* Returns true if it passes the test, false otherwise. */
  private boolean checkNumberOfEntriesForHashTable(JoinOperator joinOp, int position,
          OptimizeTezProcContext context) {
    long max = HiveConf.getLongVar(context.parseContext.getConf(),
            HiveConf.ConfVars.HIVECONVERTJOINMAXENTRIESHASHTABLE);
    if (max < 1) {
      // Max is disabled, we can safely return true
      return true;
    }
    // Calculate number of different entries and evaluate
    ReduceSinkOperator rsOp = (ReduceSinkOperator) joinOp.getParentOperators().get(position);
    List<String> keys = StatsUtils.getQualifedReducerKeyNames(rsOp.getConf().getOutputKeyColumnNames());
    Statistics inputStats = rsOp.getStatistics();
    List<ColStatistics> columnStats = new ArrayList<>();
    for (String key : keys) {
      ColStatistics cs = inputStats.getColumnStatisticsFromColName(key);
      if (cs == null) {
        LOG.debug("Couldn't get statistics for: {}", key);
        return true;
      }
      columnStats.add(cs);
    }
    long numRows = inputStats.getNumRows();
    long estimation = estimateNDV(numRows, columnStats);
    LOG.debug("Estimated NDV for input {}: {}; Max NDV for MapJoin conversion: {}",
            position, estimation, max);
    if (estimation > max) {
      // Estimation larger than max
      LOG.debug("Number of different entries for HashTable is greater than the max; "
          + "we do not converting to MapJoin");
      return false;
    }
    // We can proceed with the conversion
    return true;
  }

  private static long estimateNDV(long numRows, List<ColStatistics> columnStats) {
    // If there is a single column, return the number of distinct values
    if (columnStats.size() == 1) {
      return columnStats.get(0).getCountDistint();
    }

    // The expected number of distinct values when choosing p values
    // with replacement from n integers is n . (1 - ((n - 1) / n) ^ p).
    //
    // If we have several uniformly distributed attributes A1 ... Am
    // with N1 ... Nm distinct values, they behave as one uniformly
    // distributed attribute with N1 * ... * Nm distinct values.
    long n = 1L;
    for (ColStatistics cs : columnStats) {
      final long ndv = cs.getCountDistint();
      if (ndv > 1) {
        n = StatsUtils.safeMult(n, ndv);
      }
    }
    final double nn = (double) n;
    final double a = (nn - 1d) / nn;
    if (a == 1d) {
      // A under-flows if nn is large.
      return numRows;
    }
    final double v = nn * (1d - Math.pow(a, numRows));
    // Cap at fact-row-count, because numerical artifacts can cause it
    // to go a few % over.
    return Math.min(Math.round(v), numRows);
  }
}
