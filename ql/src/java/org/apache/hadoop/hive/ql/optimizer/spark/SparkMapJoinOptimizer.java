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

package org.apache.hadoop.hive.ql.optimizer.spark;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.MuxOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.MapJoinProcessor;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.OptimizeSparkProcContext;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.OpTraits;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
/**
 * SparkMapJoinOptimizer cloned from ConvertJoinMapJoin is an optimization that replaces a common join
 * (aka shuffle join) with a map join (aka broadcast or fragment replicate
 * join when possible. Map joins have restrictions on which joins can be
 * converted (e.g.: full outer joins cannot be handled as map joins) as well
 * as memory restrictions (one side of the join has to fit into memory).
 */
public class SparkMapJoinOptimizer implements NodeProcessor {

  private static final Log LOG = LogFactory.getLog(SparkMapJoinOptimizer.class.getName());

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

    OptimizeSparkProcContext context = (OptimizeSparkProcContext) procCtx;
    HiveConf conf = context.getConf();
    ParseContext parseContext = context.getParseContext();
    JoinOperator joinOp = (JoinOperator) nd;

    /*
    if (!conf.getBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN)
        && !(conf.getBoolVar(HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN))) {
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      int pos = 0; // it doesn't matter which position we use in this case.
      convertJoinSMBJoin(joinOp, context, pos, 0, false, false);
      return null;
    }*/

    // if we have traits, and table info is present in the traits, we know the
    // exact number of buckets. Else choose the largest number of estimated
    // reducers from the parent operators.
    //TODO  enable later. disabling this check for now
    int numBuckets = 1;

    LOG.info("Estimated number of buckets " + numBuckets);
    int mapJoinConversionPos = getMapJoinConversionPos(joinOp, context, numBuckets);
    /* TODO: handle this later
    if (mapJoinConversionPos < 0) {
      // we cannot convert to bucket map join, we cannot convert to
      // map join either based on the size. Check if we can convert to SMB join.
      if (conf.getBoolVar(HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN) == false) {
        convertJoinSMBJoin(joinOp, context, 0, 0, false, false);
        return null;
      }
      Class<? extends BigTableSelectorForAutoSMJ> bigTableMatcherClass = null;
      try {
        bigTableMatcherClass =
            (Class<? extends BigTableSelectorForAutoSMJ>) (Class.forName(HiveConf.getVar(
                parseContext.getConf(),
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
      mapJoinConversionPos =
          bigTableMatcher.getBigTablePosition(parseContext, joinOp, joinCandidates);
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

    if (numBuckets > 1) {
      if (conf.getBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN_BUCKET_MAPJOIN_TEZ)) {
        if (convertJoinBucketMapJoin(joinOp, context, mapJoinConversionPos, tezBucketJoinProcCtx)) {
          return null;
        }
      }
    }*/

    LOG.info("Convert to non-bucketed map join");
    // check if we can convert to map join no bucket scaling.
    mapJoinConversionPos = getMapJoinConversionPos(joinOp, context, 1);


    /*
    if (mapJoinConversionPos < 0) {
      // we are just converting to a common merge join operator. The shuffle
      // join in map-reduce case.
      int pos = 0; // it doesn't matter which position we use in this case.
      convertJoinSMBJoin(joinOp, context, pos, 0, false, false);
      return null;
    }*/

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

  // replaces the join operator with a new CommonJoinOperator, removes the
  // parent reduce sinks
  /*
  private void convertJoinSMBJoin(JoinOperator joinOp, OptimizeSparkProcContext context,
      int mapJoinConversionPos, int numBuckets, boolean isSubQuery, boolean adjustParentsChildren)
      throws SemanticException {
    ParseContext parseContext = context.parseContext;
    MapJoinDesc mapJoinDesc = null;
    if (adjustParentsChildren) {
        mapJoinDesc = MapJoinProcessor.getMapJoinDesc(context.conf, parseContext.getOpParseCtx(),
            joinOp, parseContext.getJoinContext().get(joinOp), mapJoinConversionPos, true);
    } else {
      JoinDesc joinDesc = joinOp.getConf();
      // retain the original join desc in the map join.
      mapJoinDesc =
          new MapJoinDesc(null, null, joinDesc.getExprs(), null, null,
              joinDesc.getOutputColumnNames(), mapJoinConversionPos, joinDesc.getConds(),
              joinDesc.getFilters(), joinDesc.getNoOuterJoin(), null);
    }

    @SuppressWarnings("unchecked")
    CommonMergeJoinOperator mergeJoinOp =
        (CommonMergeJoinOperator) OperatorFactory.get(new CommonMergeJoinDesc(numBuckets,
            isSubQuery, mapJoinConversionPos, mapJoinDesc));
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
  */
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


  private void setNumberOfBucketsOnChildren(Operator<? extends OperatorDesc> currentOp) {
    int numBuckets = currentOp.getOpTraits().getNumBuckets();
    for (Operator<? extends OperatorDesc>op : currentOp.getChildOperators()) {
      if (!(op instanceof ReduceSinkOperator) && !(op instanceof GroupByOperator)) {
        op.getOpTraits().setNumBuckets(numBuckets);
        setNumberOfBucketsOnChildren(op);
      }
    }
  }

  /**
   *   This method returns the big table position in a map-join. If the given join
   *   cannot be converted to a map-join (This could happen for several reasons - one
   *   of them being presence of 2 or more big tables that cannot fit in-memory), it returns -1.
   *
   *   Otherwise, it returns an int value that is the index of the big table in the set
   *   MapJoinProcessor.bigTableCandidateSet
   *
   * @param joinOp
   * @param context
   * @param buckets
   * @return
   */
  private int getMapJoinConversionPos(JoinOperator joinOp, OptimizeSparkProcContext context,
      int buckets) {
    Set<Integer> bigTableCandidateSet =
        MapJoinProcessor.getBigTableCandidates(joinOp.getConf().getConds());

    long maxSize = context.getConf().getLongVar(
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
   * for spark.
   */

  public MapJoinOperator convertJoinMapJoin(JoinOperator joinOp, OptimizeSparkProcContext context,
      int bigTablePosition) throws SemanticException {
    // bail on mux operator because currently the mux operator masks the emit keys
    // of the constituent reduce sinks.
    for (Operator<? extends OperatorDesc> parentOp : joinOp.getParentOperators()) {
      if (parentOp instanceof MuxOperator) {
        return null;
      }
    }

    //can safely convert the join to a map join.
    ParseContext parseContext = context.getParseContext();
    MapJoinOperator mapJoinOp =
        MapJoinProcessor.convertJoinOpMapJoinOp(context.getConf(), parseContext.getOpParseCtx(), joinOp,
            parseContext.getJoinContext().get(joinOp), bigTablePosition, true);

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
