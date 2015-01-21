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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MuxOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TezDummyStoreOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OptimizeTezProcContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
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
import org.apache.hadoop.util.ReflectionUtils;

/**
 * ConvertJoinMapJoin is an optimization that replaces a common join
 * (aka shuffle join) with a map join (aka broadcast or fragment replicate
 * join when possible. Map joins have restrictions on which joins can be
 * converted (e.g.: full outer joins cannot be handled as map joins) as well
 * as memory restrictions (one side of the join has to fit into memory).
 */
public class ConvertJoinMapJoin implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(ConvertJoinMapJoin.class.getName());

  @SuppressWarnings("unchecked")
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

    TezBucketJoinProcCtx tezBucketJoinProcCtx = new TezBucketJoinProcCtx(context.conf);
    if (!context.conf.getBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN)) {
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      Object retval = checkAndConvertSMBJoin(context, joinOp, tezBucketJoinProcCtx);
      if (retval == null) {
        return retval;
      } else {
        int pos = 0; // it doesn't matter which position we use in this case.
        convertJoinSMBJoin(joinOp, context, pos, 0, false, false);
        return null;
      }
    }

    // if we have traits, and table info is present in the traits, we know the
    // exact number of buckets. Else choose the largest number of estimated
    // reducers from the parent operators.
    int numBuckets = -1;
    int estimatedBuckets = -1;
    if (context.conf.getBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_BUCKET_MAPJOIN_TEZ)) {
      for (Operator<? extends OperatorDesc>parentOp : joinOp.getParentOperators()) {
        if (parentOp.getOpTraits().getNumBuckets() > 0) {
          numBuckets = (numBuckets < parentOp.getOpTraits().getNumBuckets()) ?
              parentOp.getOpTraits().getNumBuckets() : numBuckets;
        }

        if (parentOp instanceof ReduceSinkOperator) {
          ReduceSinkOperator rs = (ReduceSinkOperator)parentOp;
          estimatedBuckets = (estimatedBuckets < rs.getConf().getNumReducers()) ?
              rs.getConf().getNumReducers() : estimatedBuckets;
        }
      }

      if (numBuckets <= 0) {
        numBuckets = estimatedBuckets;
        if (numBuckets <= 0) {
          numBuckets = 1;
        }
      }
    } else {
      numBuckets = 1;
    }
    LOG.info("Estimated number of buckets " + numBuckets);
    int mapJoinConversionPos = getMapJoinConversionPos(joinOp, context, numBuckets);
    if (mapJoinConversionPos < 0) {
      Object retval = checkAndConvertSMBJoin(context, joinOp, tezBucketJoinProcCtx);
      if (retval == null) {
        return retval;
      } else {
          // only case is full outer join with SMB enabled which is not possible. Convert to regular
          // join.
          convertJoinSMBJoin(joinOp, context, 0, 0, false, false);
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

    LOG.info("Convert to non-bucketed map join");
    // check if we can convert to map join no bucket scaling.
    mapJoinConversionPos = getMapJoinConversionPos(joinOp, context, 1);
    if (mapJoinConversionPos < 0) {
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      int pos = 0; // it doesn't matter which position we use in this case.
      convertJoinSMBJoin(joinOp, context, pos, 0, false, false);
      return null;
    }

    MapJoinOperator mapJoinOp = convertJoinMapJoin(joinOp, context, mapJoinConversionPos);
    // map join operator by default has no bucket cols
    mapJoinOp.setOpTraits(new OpTraits(null, -1, null));
    mapJoinOp.setStatistics(joinOp.getStatistics());
    // propagate this change till the next RS
    for (Operator<? extends OperatorDesc> childOp : mapJoinOp.getChildOperators()) {
      setAllChildrenTraitsToNull(childOp);
    }

    return null;
  }

  private Object checkAndConvertSMBJoin(OptimizeTezProcContext context, JoinOperator joinOp,
      TezBucketJoinProcCtx tezBucketJoinProcCtx) throws SemanticException {
    // we cannot convert to bucket map join, we cannot convert to
    // map join either based on the size. Check if we can convert to SMB join.
    if (context.conf.getBoolVar(HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN) == false) {
      convertJoinSMBJoin(joinOp, context, 0, 0, false, false);
      return null;
    }
    Class<? extends BigTableSelectorForAutoSMJ> bigTableMatcherClass = null;
    try {
      bigTableMatcherClass =
          (Class<? extends BigTableSelectorForAutoSMJ>) (Class.forName(HiveConf.getVar(
              context.parseContext.getConf(),
              HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN_BIGTABLE_SELECTOR)));
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
      int pos = 0; // it doesn't matter which position we use in this case.
      convertJoinSMBJoin(joinOp, context, pos, 0, false, false);
      return null;
    }

    if (checkConvertJoinSMBJoin(joinOp, context, mapJoinConversionPos, tezBucketJoinProcCtx)) {
      convertJoinSMBJoin(joinOp, context, mapJoinConversionPos,
          tezBucketJoinProcCtx.getNumBuckets(), tezBucketJoinProcCtx.isSubQuery(), true);
    } else {
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      int pos = 0; // it doesn't matter which position we use in this case.
      convertJoinSMBJoin(joinOp, context, pos, 0, false, false);
    }
    return null;
}

  // replaces the join operator with a new CommonJoinOperator, removes the
  // parent reduce sinks
  private void convertJoinSMBJoin(JoinOperator joinOp, OptimizeTezProcContext context,
      int mapJoinConversionPos, int numBuckets, boolean isSubQuery, boolean adjustParentsChildren)
      throws SemanticException {
    ParseContext parseContext = context.parseContext;
    MapJoinDesc mapJoinDesc = null;
    if (adjustParentsChildren) {
      mapJoinDesc = MapJoinProcessor.getMapJoinDesc(context.conf, parseContext.getOpParseCtx(),
            joinOp, joinOp.getConf().isLeftInputJoin(), joinOp.getConf().getBaseSrc(), joinOp.getConf().getMapAliases(),
            mapJoinConversionPos, true);
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
      mapJoinDesc.resetOrder();
    }

    @SuppressWarnings("unchecked")
    CommonMergeJoinOperator mergeJoinOp =
        (CommonMergeJoinOperator) OperatorFactory.get(new CommonMergeJoinDesc(numBuckets,
            isSubQuery, mapJoinConversionPos, mapJoinDesc), joinOp.getSchema());
    OpTraits opTraits =
        new OpTraits(joinOp.getOpTraits().getBucketColNames(), numBuckets, joinOp.getOpTraits()
            .getSortCols());
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
    if (childOperators == null) {
      childOperators = new ArrayList<Operator<? extends OperatorDesc>>();
      mergeJoinOp.setChildOperators(childOperators);
    }

    List<Operator<? extends OperatorDesc>> parentOperators = mergeJoinOp.getParentOperators();
    if (parentOperators == null) {
      parentOperators = new ArrayList<Operator<? extends OperatorDesc>>();
      mergeJoinOp.setParentOperators(parentOperators);
    }

    childOperators.clear();
    parentOperators.clear();
    childOperators.addAll(joinOp.getChildOperators());
    parentOperators.addAll(joinOp.getParentOperators());
    mergeJoinOp.getConf().setGenJoinKeys(false);

    if (adjustParentsChildren) {
      mergeJoinOp.getConf().setGenJoinKeys(true);
      List<Operator<? extends OperatorDesc>> newParentOpList =
          new ArrayList<Operator<? extends OperatorDesc>>();
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
        DummyStoreOperator dummyStoreOp = new TezDummyStoreOperator();
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

  private void setAllChildrenTraitsToNull(Operator<? extends OperatorDesc> currentOp) {
    if (currentOp instanceof ReduceSinkOperator) {
      return;
    }
    currentOp.setOpTraits(new OpTraits(null, -1, null));
    for (Operator<? extends OperatorDesc> childOp : currentOp.getChildOperators()) {
      if ((childOp instanceof ReduceSinkOperator) || (childOp instanceof GroupByOperator)) {
        break;
      }
      setAllChildrenTraitsToNull(childOp);
    }
  }

  private boolean convertJoinBucketMapJoin(JoinOperator joinOp, OptimizeTezProcContext context,
      int bigTablePosition, TezBucketJoinProcCtx tezBucketJoinProcCtx) throws SemanticException {

    if (!checkConvertJoinBucketMapJoin(joinOp, context, bigTablePosition, tezBucketJoinProcCtx)) {
      LOG.info("Check conversion to bucket map join failed.");
      return false;
    }

    MapJoinOperator mapJoinOp = convertJoinMapJoin(joinOp, context, bigTablePosition);
    MapJoinDesc joinDesc = mapJoinOp.getConf();
    joinDesc.setBucketMapJoin(true);

    // we can set the traits for this join operator
    OpTraits opTraits = new OpTraits(joinOp.getOpTraits().getBucketColNames(),
        tezBucketJoinProcCtx.getNumBuckets(), null);
    mapJoinOp.setOpTraits(opTraits);
    mapJoinOp.setStatistics(joinOp.getStatistics());
    setNumberOfBucketsOnChildren(mapJoinOp);

    // Once the conversion is done, we can set the partitioner to bucket cols on the small table
    Map<String, Integer> bigTableBucketNumMapping = new HashMap<String, Integer>();
    bigTableBucketNumMapping.put(joinDesc.getBigTableAlias(), tezBucketJoinProcCtx.getNumBuckets());
    joinDesc.setBigTableBucketNumMapping(bigTableBucketNumMapping);
    LOG.info("Setting legacy map join to " + (!tezBucketJoinProcCtx.isSubQuery()));
    joinDesc.setCustomBucketMapJoin(!tezBucketJoinProcCtx.isSubQuery());

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
    int numBuckets = bigTableRS.getParentOperators().get(0).getOpTraits()
            .getNumBuckets();

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
          .getOpTraits().getSortCols(), rsOp.getColumnExprMap(), tezBucketJoinProcCtx) == false) {
        LOG.info("We cannot convert to SMB because the sort column names do not match.");
        return false;
      }

      if (checkColEquality(rsOp.getParentOperators().get(0).getOpTraits().getBucketColNames(), rsOp
          .getOpTraits().getBucketColNames(), rsOp.getColumnExprMap(), tezBucketJoinProcCtx)
          == false) {
        LOG.info("We cannot convert to SMB because bucket column names do not match.");
        return false;
      }
    }

    boolean isSubQuery = false;
    if (numBuckets < 0) {
      isSubQuery = true;
      numBuckets = bigTableRS.getConf().getNumReducers();
    }
    tezBucketJoinProcCtx.setNumBuckets(numBuckets);
    tezBucketJoinProcCtx.setIsSubQuery(isSubQuery);
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
   * If the parent reduce sink of the big table side has the same emit key cols
   * as its parent, we can create a bucket map join eliminating the reduce sink.
   */
  private boolean checkConvertJoinBucketMapJoin(JoinOperator joinOp,
      OptimizeTezProcContext context, int bigTablePosition,
      TezBucketJoinProcCtx tezBucketJoinProcCtx)
  throws SemanticException {
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
        tezBucketJoinProcCtx) == false) {
      LOG.info("No info available to check for bucket map join. Cannot convert");
      return false;
    }

    /*
     * this is the case when the big table is a sub-query and is probably
     * already bucketed by the join column in say a group by operation
     */
    boolean isSubQuery = false;
    if (numBuckets < 0) {
      isSubQuery = true;
      numBuckets = rs.getConf().getNumReducers();
    }
    tezBucketJoinProcCtx.setNumBuckets(numBuckets);
    tezBucketJoinProcCtx.setIsSubQuery(isSubQuery);
    return true;
  }

  private boolean checkColEquality(List<List<String>> grandParentColNames,
      List<List<String>> parentColNames, Map<String, ExprNodeDesc> colExprMap,
      TezBucketJoinProcCtx tezBucketJoinProcCtx) {

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
          // all columns need to be at least a subset of the parentOfParent's bucket cols
          ExprNodeDesc exprNodeDesc = colExprMap.get(colName);
          if (exprNodeDesc instanceof ExprNodeColumnDesc) {
            if (((ExprNodeColumnDesc)exprNodeDesc).getColumn().equals(listBucketCols.get(colCount))) {
              colCount++;
            } else {
              break;
            }
          }

          if (colCount == parentColNames.get(0).size()) {
            return true;
          }
        }
      }
      return false;
    }
    return false;
  }

  public int getMapJoinConversionPos(JoinOperator joinOp, OptimizeTezProcContext context,
      int buckets) throws SemanticException {
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
    Set<Integer> bigTableCandidateSet =
        MapJoinProcessor.getBigTableCandidates(joinOp.getConf().getConds());

    long maxSize = context.conf.getLongVar(
        HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASKTHRESHOLD);

    int bigTablePosition = -1;

    Statistics bigInputStat = null;
    long totalSize = 0;
    int pos = 0;

    // bigTableFound means we've encountered a table that's bigger than the
    // max. This table is either the the big table or we cannot convert.
    boolean bigTableFound = false;

    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {

      Statistics currInputStat = parentOp.getStatistics();
      if (currInputStat == null) {
        LOG.warn("Couldn't get statistics from: "+parentOp);
        return -1;
      }

      long inputSize = currInputStat.getDataSize();
      if ((bigInputStat == null) ||
          ((bigInputStat != null) &&
          (inputSize > bigInputStat.getDataSize()))) {

        if (bigTableFound) {
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

          bigTableFound = true;
        }

        if (bigInputStat != null) {
          // we're replacing the current big table with a new one. Need
          // to count the current one as a map table then.
          totalSize += bigInputStat.getDataSize();
        }

        if (totalSize/buckets > maxSize) {
          // sum of small tables size in this join exceeds configured limit
          // hence cannot convert.
          return -1;
        }

        if (bigTableCandidateSet.contains(pos)) {
          bigTablePosition = pos;
          bigInputStat = currInputStat;
        }
      } else {
        totalSize += currInputStat.getDataSize();
        if (totalSize/buckets > maxSize) {
          // cannot hold all map tables in memory. Cannot convert.
          return -1;
        }
      }
      pos++;
    }

    return bigTablePosition;
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
      int bigTablePosition) throws SemanticException {
    // bail on mux operator because currently the mux operator masks the emit keys
    // of the constituent reduce sinks.
    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
      if (parentOp instanceof MuxOperator) {
        return null;
      }
    }

    //can safely convert the join to a map join.
    ParseContext parseContext = context.parseContext;
    MapJoinOperator mapJoinOp =
        MapJoinProcessor.convertJoinOpMapJoinOp(context.conf, parseContext.getOpParseCtx(), joinOp,
                joinOp.getConf().isLeftInputJoin(), joinOp.getConf().getBaseSrc(), joinOp.getConf().getMapAliases(),
                bigTablePosition, true);

    Operator<? extends OperatorDesc> parentBigTableOp =
        mapJoinOp.getParentOperators().get(bigTablePosition);
    if (parentBigTableOp instanceof ReduceSinkOperator) {
      for (Operator<?> p : parentBigTableOp.getParentOperators()) {
        // we might have generated a dynamic partition operator chain. Since
        // we're removing the reduce sink we need do remove that too.
        Set<Operator<?>> dynamicPartitionOperators = new HashSet<Operator<?>>();
        for (Operator<?> c : p.getChildOperators()) {
          if (hasDynamicPartitionBroadcast(c)) {
            dynamicPartitionOperators.add(c);
          }
        }
        for (Operator<?> c : dynamicPartitionOperators) {
          p.removeChild(c);
        }
      }
      mapJoinOp.getParentOperators().remove(bigTablePosition);
      if (!(mapJoinOp.getParentOperators().contains(parentBigTableOp.getParentOperators().get(0)))) {
        mapJoinOp.getParentOperators().add(bigTablePosition,
            parentBigTableOp.getParentOperators().get(0));
      }
      parentBigTableOp.getParentOperators().get(0).removeChild(parentBigTableOp);
      for (Operator<? extends OperatorDesc> op : mapJoinOp.getParentOperators()) {
        if (!(op.getChildOperators().contains(mapJoinOp))) {
          op.getChildOperators().add(mapJoinOp);
        }
        op.getChildOperators().remove(joinOp);
      }
    }

    return mapJoinOp;
  }

  private boolean hasDynamicPartitionBroadcast(Operator<?> parent) {
    boolean hasDynamicPartitionPruning = false;

    for (Operator<?> op: parent.getChildOperators()) {
      while (op != null) {
        if (op instanceof AppMasterEventOperator && op.getConf() instanceof DynamicPruningEventDesc) {
          // found dynamic partition pruning operator
          hasDynamicPartitionPruning = true;
          break;
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

    return hasDynamicPartitionPruning;
  }
}
