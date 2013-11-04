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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TezWork.EdgeType;

/**
 * GenTezWork separates the operator tree into tez tasks.
 * It is called once per leaf operator (operator that forces
 * a new execution unit.) and break the operators into work
 * and tasks along the way.
 */
public class GenTezWork implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(GenTezWork.class.getName());

  private int sequenceNumber = 0;

  @SuppressWarnings("unchecked")
  @Override
  public Object process(Node nd, Stack<Node> stack,
      NodeProcessorCtx procContext, Object... nodeOutputs)
      throws SemanticException {

    GenTezProcContext context = (GenTezProcContext) procContext;

    // Operator is a file sink or reduce sink. Something that forces
    // a new vertex.
    Operator<?> operator = (Operator<?>) nd;

    TezWork tezWork = context.currentTask.getWork();
    if (!context.rootTasks.contains(context.currentTask)) {
      context.rootTasks.add(context.currentTask);
    }

    // root is the start of the operator pipeline we're currently
    // packing into a vertex, typically a table scan, union or join
    Operator<?> root = context.currentRootOperator;
    if (root == null) {
      // if there are no more rootOperators we're dealing with multiple
      // file sinks off of the same table scan. Bail.
      if (context.rootOperators.isEmpty()) {
        return null;
      }

      // null means that we're starting with a new table scan
      // the graph walker walks the rootOperators in the same
      // order so we can just take the next
      context.preceedingWork = null;
      root = context.rootOperators.pop();
    }

    LOG.debug("Root operator: " + root);
    LOG.debug("Leaf operator: " + operator);

    // Right now the work graph is pretty simple. If there is no
    // Preceding work we have a root and will generate a map
    // vertex. If there is a preceding work we will generate
    // a reduce vertex
    BaseWork work;
    if (context.preceedingWork == null) {
      assert root.getParentOperators().isEmpty();
      MapWork mapWork = new MapWork("Map "+ (++sequenceNumber));
      LOG.debug("Adding map work (" + mapWork.getName() + ") for " + root);

      // map work starts with table scan operators
      assert root instanceof TableScanOperator;
      String alias = ((TableScanOperator)root).getConf().getAlias();

      GenMapRedUtils.setMapWork(mapWork, context.parseContext,
          context.inputs, null, root, alias, context.conf, false);
      tezWork.add(mapWork);
      work = mapWork;
    } else {
      assert !root.getParentOperators().isEmpty();
      ReduceWork reduceWork = new ReduceWork("Reducer "+ (++sequenceNumber));
      LOG.debug("Adding reduce work (" + reduceWork.getName() + ") for " + root);
      reduceWork.setReducer(root);
      reduceWork.setNeedsTagging(GenMapRedUtils.needsTagging(reduceWork));

      // All parents should be reduce sinks. We pick the one we just walked
      // to choose the number of reducers. In the join/union case they will
      // all be -1. In sort/order case where it matters there will be only
      // one parent.
      assert context.parentOfRoot instanceof ReduceSinkOperator;
      ReduceSinkOperator reduceSink = (ReduceSinkOperator) context.parentOfRoot;

      reduceWork.setNumReduceTasks(reduceSink.getConf().getNumReducers());

      // need to fill in information about the key and value in the reducer
      GenMapRedUtils.setKeyAndValueDesc(reduceWork, reduceSink);

      // remember which parent belongs to which tag
      reduceWork.getTagToInput().put(reduceSink.getConf().getTag(),
           context.preceedingWork.getName());

      tezWork.add(reduceWork);
      tezWork.connect(
          context.preceedingWork,
          reduceWork, EdgeType.SIMPLE_EDGE);

      work = reduceWork;
    }

    // We're scanning the operator from table scan to final file sink.
    // We're scanning a tree from roots to leaf (this is not technically
    // correct, demux and mux operators might form a diamond shape, but
    // we will only scan one path and ignore the others, because the
    // diamond shape is always contained in a single vertex). The scan
    // is depth first and because we remove parents when we pack a pipeline
    // into a vertex we will never visit any node twice. But because of that
    // we might have a situation where we need to connect 'work' that comes after
    // the 'work' we're currently looking at.
    //
    // Also note: the concept of leaf and root is reversed in hive for historical
    // reasons. Roots are data sources, leaves are data sinks. I know.
    if (context.leafOperatorToFollowingWork.containsKey(operator)) {

      BaseWork followingWork = context.leafOperatorToFollowingWork.get(operator);

      // need to add this branch to the key + value info
      assert operator instanceof ReduceSinkOperator
        && followingWork instanceof ReduceWork;
      ReduceSinkOperator rs = (ReduceSinkOperator) operator;
      ReduceWork rWork = (ReduceWork) followingWork;
      GenMapRedUtils.setKeyAndValueDesc(rWork, rs);

      // remember which parent belongs to which tag
      rWork.getTagToInput().put(rs.getConf().getTag(), work.getName());

      // add dependency between the two work items
      tezWork.connect(work, context.leafOperatorToFollowingWork.get(operator),
         EdgeType.SIMPLE_EDGE);
    }

    // This is where we cut the tree as described above. We also remember that
    // we might have to connect parent work with this work later.
    for (Operator<?> parent: new ArrayList<Operator<?>>(root.getParentOperators())) {
      context.leafOperatorToFollowingWork.put(parent, work);
      LOG.debug("Removing " + parent + " as parent from " + root);
      root.removeParent(parent);
    }

    // No children means we're at the bottom. If there are more operators to scan
    // the next item will be a new root.
    if (!operator.getChildOperators().isEmpty()) {
      assert operator.getChildOperators().size() == 1;
      context.parentOfRoot = operator;
      context.currentRootOperator = operator.getChildOperators().get(0);
      context.preceedingWork = work;
    } else {
      context.parentOfRoot = null;
      context.currentRootOperator = null;
      context.preceedingWork = null;
    }

    /*
     * this happens in case of map join operations.
     * The tree looks like this:
     *
     *        RS <--- we are here perhaps
     *        |
     *      MapJoin
     *    /     \
     *  RS       TS
     *  /
     * TS
     *
     * If we are at the RS pointed above, and we may have already visited the
     * RS following the TS, we have already generated work for the TS-RS.
     * We need to hook the current work to this generated work.
     */
    context.operatorWorkMap.put(operator, work);
    List<BaseWork> linkWorkList = context.linkOpWithWorkMap.get(operator);
    if (linkWorkList != null) {
      if (context.linkChildOpWithDummyOp.containsKey(operator)) {
        for (Operator<?> dummy: context.linkChildOpWithDummyOp.get(operator)) {
          work.addDummyOp((HashTableDummyOperator) dummy);
        }
      }
      for (BaseWork parentWork : linkWorkList) {
        tezWork.connect(parentWork, work, EdgeType.BROADCAST_EDGE);
      }
    }

    return null;
  }

}
