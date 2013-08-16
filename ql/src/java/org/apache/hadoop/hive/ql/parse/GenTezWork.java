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
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

/**
 * GenTezWork separates the operator tree into tez tasks.
 * It is called once per leaf operator (operator that forces
 * a new execution unit.) and break the operators into work
 * and tasks along the way.
 */
public class GenTezWork implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(GenTezWork.class.getName());

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
      LOG.debug("Adding map work for " + root);
      MapWork mapWork = new MapWork();

      // map work starts with table scan operators
      assert root instanceof TableScanOperator;
      String alias = ((TableScanOperator)root).getConf().getAlias();

      GenMapRedUtils.setMapWork(mapWork, context.parseContext,
          context.inputs, null, root, alias, context.conf, false);
      tezWork.add(mapWork);
      work = mapWork;
    } else {
      assert !root.getParentOperators().isEmpty();
      LOG.debug("Adding reduce work for " + root);
      ReduceWork reduceWork = new ReduceWork();
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

      // needs to be fixed in HIVE-5052. This should be driven off of stats
      if (reduceWork.getNumReduceTasks() <= 0) {
        reduceWork.setNumReduceTasks(1);
      }

      tezWork.add(reduceWork);
      tezWork.connect(
          context.preceedingWork,
          reduceWork);
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

      // add dependency between the two work items
      tezWork.connect(work, context.leafOperatorToFollowingWork.get(operator));
    }

    // This is where we cut the tree as described above. We also remember that
    // we might have to connect parent work with this work later.
    for (Operator<?> parent: new ArrayList<Operator<?>>(root.getParentOperators())) {
      assert !context.leafOperatorToFollowingWork.containsKey(parent);
      assert !(work instanceof MapWork);
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

    return null;
  }

}
