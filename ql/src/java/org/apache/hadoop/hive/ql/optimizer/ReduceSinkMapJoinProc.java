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

import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.FIXED;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.GenTezProcContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.HashTableDummyDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OpTraits;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TezWork.VertexType;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class ReduceSinkMapJoinProc implements SemanticNodeProcessor {

  private final static Logger LOG = LoggerFactory.getLogger(ReduceSinkMapJoinProc.class.getName());

  /* (non-Javadoc)
   * This processor addresses the RS-MJ case that occurs in tez on the small/hash
   * table side of things. The work that RS will be a part of must be connected
   * to the MJ work via be a broadcast edge.
   * We should not walk down the tree when we encounter this pattern because:
   * the type of work (map work or reduce work) needs to be determined
   * on the basis of the big table side because it may be a mapwork (no need for shuffle)
   * or reduce work.
   */
  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procContext, Object... nodeOutputs)
      throws SemanticException {
    GenTezProcContext context = (GenTezProcContext) procContext;
    MapJoinOperator mapJoinOp = (MapJoinOperator)nd;

    // remember the original parent list before we start modifying it.
    if (!context.mapJoinParentMap.containsKey(mapJoinOp)) {
      List<Operator<?>> parents = new ArrayList<Operator<?>>(mapJoinOp.getParentOperators());
      context.mapJoinParentMap.put(mapJoinOp, parents);
    }

    boolean isBigTable = stack.size() < 2
        || !(stack.get(stack.size() - 2) instanceof ReduceSinkOperator);

    ReduceSinkOperator parentRS = null;
    if (!isBigTable) {
      parentRS = (ReduceSinkOperator)stack.get(stack.size() - 2);

      // For dynamic partitioned hash join, the big table will also be coming from a ReduceSinkOperator
      // Check for this condition.
      // TODO: use indexOf(), or parentRS.getTag()?
      isBigTable =
          (mapJoinOp.getParentOperators().indexOf(parentRS) == mapJoinOp.getConf().getPosBigTable());
    }

    if (mapJoinOp.getConf().isDynamicPartitionHashJoin() &&
        !context.mapJoinToUnprocessedSmallTableReduceSinks.containsKey(mapJoinOp)) {
      // Initialize set of unprocessed small tables
      Set<ReduceSinkOperator> rsSet = Sets.newIdentityHashSet();
      for (int pos = 0; pos < mapJoinOp.getParentOperators().size(); ++pos) {
        if (pos == mapJoinOp.getConf().getPosBigTable()) {
          continue;
        }
        rsSet.add((ReduceSinkOperator) mapJoinOp.getParentOperators().get(pos));
      }
      context.mapJoinToUnprocessedSmallTableReduceSinks.put(mapJoinOp, rsSet);
    }

    if (isBigTable) {
      context.currentMapJoinOperators.add(mapJoinOp);
      return null;
    }

    context.preceedingWork = null;
    context.currentRootOperator = null;

    return processReduceSinkToHashJoin(parentRS, mapJoinOp, context);
  }

  public static BaseWork getMapJoinParentWork(GenTezProcContext context, Operator<?> parentRS) {
    BaseWork parentWork;
    if (context.unionWorkMap.containsKey(parentRS)) {
      parentWork = context.unionWorkMap.get(parentRS);
    } else {
      assert context.childToWorkMap.get(parentRS).size() == 1;
      parentWork = context.childToWorkMap.get(parentRS).get(0);
    }
    return parentWork;
  }

  public static Object processReduceSinkToHashJoin(ReduceSinkOperator parentRS, MapJoinOperator mapJoinOp,
      GenTezProcContext context) throws SemanticException {
    // remove the tag for in-memory side of mapjoin
    parentRS.getConf().setSkipTag(true);
    parentRS.setSkipTag(true);

    // Mark this small table as being processed
    if (mapJoinOp.getConf().isDynamicPartitionHashJoin()) {
      context.mapJoinToUnprocessedSmallTableReduceSinks.get(mapJoinOp).remove(parentRS);
    }

    List<BaseWork> mapJoinWork = null;

    /*
     *  if there was a pre-existing work generated for the big-table mapjoin side,
     *  we need to hook the work generated for the RS (associated with the RS-MJ pattern)
     *  with the pre-existing work.
     *
     *  Otherwise, we need to associate that the mapjoin op
     *  to be linked to the RS work (associated with the RS-MJ pattern).
     *
     */
    mapJoinWork = context.mapJoinWorkMap.get(mapJoinOp);
    BaseWork parentWork = getMapJoinParentWork(context, parentRS);

    // set the link between mapjoin and parent vertex
    int pos = context.mapJoinParentMap.get(mapJoinOp).indexOf(parentRS);
    if (pos == -1) {
      throw new SemanticException("Cannot find position of parent in mapjoin");
    }
    MapJoinDesc joinConf = mapJoinOp.getConf();
    long keyCount = Long.MAX_VALUE, rowCount = Long.MAX_VALUE, bucketCount = 1;
    long tableSize = Long.MAX_VALUE;
    Statistics stats = parentRS.getStatistics();
    if (stats != null) {
      keyCount = rowCount = stats.getNumRows();
      if (keyCount <= 0) {
        keyCount = rowCount = Long.MAX_VALUE;
      }
      tableSize = stats.getDataSize();
      List<String> keyCols = parentRS.getConf().getOutputKeyColumnNames();
      if (keyCols != null && !keyCols.isEmpty()) {
        // See if we can arrive at a smaller number using distinct stats from key columns.
        long maxKeyCount = 1;
        String prefix = Utilities.ReduceField.KEY.toString();
        for (String keyCol : keyCols) {
          ExprNodeDesc realCol = parentRS.getColumnExprMap().get(prefix + "." + keyCol);
          ColStatistics cs =
              StatsUtils.getColStatisticsFromExpression(context.conf, stats, realCol);
          if (cs == null || cs.getCountDistint() <= 0) {
            maxKeyCount = Long.MAX_VALUE;
            break;
          }
          maxKeyCount *= cs.getCountDistint();
          if (maxKeyCount >= keyCount) {
            break;
          }
        }
        keyCount = Math.min(maxKeyCount, keyCount);
      }
      if (joinConf.isBucketMapJoin()) {
        OpTraits opTraits = mapJoinOp.getOpTraits();
        bucketCount = (opTraits == null) ? -1 : opTraits.getNumBuckets();
        if (bucketCount > 0) {
          // We cannot obtain a better estimate without CustomPartitionVertex providing it
          // to us somehow; in which case using statistics would be completely unnecessary.
          keyCount /= bucketCount;
          tableSize /= bucketCount;
        }
      } else if (joinConf.isDynamicPartitionHashJoin()) {
        // For dynamic partitioned hash join, assuming table is split evenly among the reduce tasks.
        bucketCount = parentRS.getConf().getNumReducers();
        keyCount /= bucketCount;
        tableSize /= bucketCount;
      }
    }
    if (keyCount == 0) {
      keyCount = 1;
    }
    if (tableSize == 0) {
      tableSize = 1;
    }
    LOG.info("Mapjoin " + mapJoinOp + "(bucket map join = " + joinConf.isBucketMapJoin()
    + "), pos: " + pos + " --> " + parentWork.getName() + " (" + keyCount
    + " keys estimated from " + rowCount + " rows, " + bucketCount + " buckets)");
    joinConf.getParentToInput().put(pos, parentWork.getName());
    if (keyCount != Long.MAX_VALUE) {
      joinConf.getParentKeyCounts().put(pos, keyCount);
    }
    joinConf.getParentDataSizes().put(pos, tableSize);

    int numBuckets = -1;
    EdgeType edgeType = EdgeType.BROADCAST_EDGE;
    if (joinConf.isBucketMapJoin()) {
      numBuckets = (Integer) joinConf.getBigTableBucketNumMapping().values().toArray()[0];
      /*
       * Here, we can be in one of 4 states.
       *
       * 1. If map join work is null implies that we have not yet traversed the big table side. We
       * just need to see if we can find a reduce sink operator in the big table side. This would
       * imply a reduce side operation.
       *
       * 2. If we don't find a reducesink in 1 it has to be the case that it is a map side operation.
       *
       * 3. If we have already created a work item for the big table side, we need to see if we can
       * find a table scan operator in the big table side. This would imply a map side operation.
       *
       * 4. If we don't find a table scan operator, it has to be a reduce side operation.
       */
      if (mapJoinWork == null) {
        Operator<?> rootOp = OperatorUtils.findSingleOperatorUpstreamJoinAccounted(
            mapJoinOp.getParentOperators().get(joinConf.getPosBigTable()),
            ReduceSinkOperator.class);
        if (rootOp == null) {
          // likely we found a table scan operator
          edgeType = EdgeType.CUSTOM_EDGE;
        } else {
          // we have found a reduce sink
          edgeType = EdgeType.CUSTOM_SIMPLE_EDGE;
        }
      } else {
        Operator<?> rootOp = OperatorUtils.findSingleOperatorUpstreamJoinAccounted(
            mapJoinOp.getParentOperators().get(joinConf.getPosBigTable()),
            TableScanOperator.class);
        if (rootOp != null) {
          // likely we found a table scan operator
          edgeType = EdgeType.CUSTOM_EDGE;
        } else {
          // we have found a reduce sink
          edgeType = EdgeType.CUSTOM_SIMPLE_EDGE;
        }
      }
    } else if (mapJoinOp.getConf().isDynamicPartitionHashJoin()) {
      if (parentRS.getConf().isForwarding()) {
        edgeType = EdgeType.ONE_TO_ONE_EDGE;
      } else {
        edgeType = EdgeType.CUSTOM_SIMPLE_EDGE;
      }
    }
    if (edgeType == EdgeType.CUSTOM_EDGE) {
      // disable auto parallelism for bucket map joins
      parentRS.getConf().setReducerTraits(EnumSet.of(FIXED));
    }
    TezEdgeProperty edgeProp = new TezEdgeProperty(null, edgeType, numBuckets);

    if (mapJoinWork != null) {
      for (BaseWork myWork: mapJoinWork) {
        // link the work with the work associated with the reduce sink that triggered this rule
        TezWork tezWork = context.currentTask.getWork();
        LOG.debug("connecting "+parentWork.getName()+" with "+myWork.getName());
        tezWork.connect(parentWork, myWork, edgeProp);
        if (edgeType == EdgeType.CUSTOM_EDGE) {
          tezWork.setVertexType(myWork, VertexType.INITIALIZED_EDGES);
        }

        ReduceSinkOperator r = null;
        if (context.connectedReduceSinks.contains(parentRS)) {
          LOG.debug("Cloning reduce sink " + parentRS + " for multi-child broadcast edge");
          // we've already set this one up. Need to clone for the next work.
          r = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(
              parentRS.getCompilationOpContext(),
              (ReduceSinkDesc) parentRS.getConf().clone(),
              new RowSchema(parentRS.getSchema()),
              parentRS.getParentOperators());
          context.clonedReduceSinks.add(r);
        } else {
          r = parentRS;
        }
        // remember the output name of the reduce sink
        r.getConf().setOutputName(myWork.getName());
        context.connectedReduceSinks.add(r);
      }
    }

    // remember in case we need to connect additional work later
    Map<BaseWork, TezEdgeProperty> linkWorkMap = null;
    if (context.linkOpWithWorkMap.containsKey(mapJoinOp)) {
      linkWorkMap = context.linkOpWithWorkMap.get(mapJoinOp);
    } else {
      linkWorkMap = new HashMap<BaseWork, TezEdgeProperty>();
    }
    linkWorkMap.put(parentWork, edgeProp);
    context.linkOpWithWorkMap.put(mapJoinOp, linkWorkMap);

    List<ReduceSinkOperator> reduceSinks
    = context.linkWorkWithReduceSinkMap.get(parentWork);
    if (reduceSinks == null) {
      reduceSinks = new ArrayList<ReduceSinkOperator>();
    }
    reduceSinks.add(parentRS);
    context.linkWorkWithReduceSinkMap.put(parentWork, reduceSinks);

    // create the dummy operators
    List<Operator<?>> dummyOperators = new ArrayList<Operator<?>>();

    // create an new operator: HashTableDummyOperator, which share the table desc
    HashTableDummyDesc desc = new HashTableDummyDesc();
    HashTableDummyOperator dummyOp = (HashTableDummyOperator) OperatorFactory.get(
        parentRS.getCompilationOpContext(), desc);
    TableDesc tbl;

    // need to create the correct table descriptor for key/value
    RowSchema rowSchema = parentRS.getParentOperators().get(0).getSchema();
    tbl = PlanUtils.getReduceValueTableDesc(PlanUtils.getFieldSchemasFromRowSchema(rowSchema, ""));
    dummyOp.getConf().setTbl(tbl);

    Map<Byte, List<ExprNodeDesc>> keyExprMap = mapJoinOp.getConf().getKeys();
    List<ExprNodeDesc> keyCols = keyExprMap.get(Byte.valueOf((byte) 0));
    StringBuilder keyOrder = new StringBuilder();
    StringBuilder keyNullOrder = new StringBuilder();
    for (ExprNodeDesc k: keyCols) {
      keyOrder.append("+");
      keyNullOrder.append(NullOrdering.defaultNullOrder(context.conf).getSign());
    }
    TableDesc keyTableDesc = PlanUtils.getReduceKeyTableDesc(PlanUtils
        .getFieldSchemasFromColumnList(keyCols, "mapjoinkey"), keyOrder.toString(),
        keyNullOrder.toString());
    mapJoinOp.getConf().setKeyTableDesc(keyTableDesc);

    // let the dummy op be the parent of mapjoin op
    mapJoinOp.replaceParent(parentRS, dummyOp);
    List<Operator<? extends OperatorDesc>> dummyChildren =
        new ArrayList<Operator<? extends OperatorDesc>>();
    dummyChildren.add(mapJoinOp);
    dummyOp.setChildOperators(dummyChildren);
    dummyOperators.add(dummyOp);

    // cut the operator tree so as to not retain connections from the parent RS downstream
    List<Operator<? extends OperatorDesc>> childOperators = parentRS.getChildOperators();
    int childIndex = childOperators.indexOf(mapJoinOp);
    childOperators.remove(childIndex);

    // the "work" needs to know about the dummy operators. They have to be separately initialized
    // at task startup
    if (mapJoinWork != null) {
      for (BaseWork myWork: mapJoinWork) {
        LOG.debug("adding dummy op to work " + myWork.getName() + " from MJ work: " + dummyOp);
        myWork.addDummyOp(dummyOp);
      }
    }
    if (context.linkChildOpWithDummyOp.containsKey(mapJoinOp)) {
      for (Operator<?> op: context.linkChildOpWithDummyOp.get(mapJoinOp)) {
        dummyOperators.add(op);
      }
    }
    context.linkChildOpWithDummyOp.put(mapJoinOp, dummyOperators);

    return true;
  }
}
