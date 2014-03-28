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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;

/**
 * GenTezWork separates the operator tree into tez tasks.
 * It is called once per leaf operator (operator that forces
 * a new execution unit.) and break the operators into work
 * and tasks along the way.
 */
public class GenTezWork implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(GenTezWork.class.getName());

  // instance of shared utils
  private GenTezUtils utils = null;

  /**
   * Constructor takes utils as parameter to facilitate testing
   */
  public GenTezWork(GenTezUtils utils) {
    this.utils = utils;
  }

  @Override
  public Object process(Node nd, Stack<Node> stack,
      NodeProcessorCtx procContext, Object... nodeOutputs)
      throws SemanticException {

    GenTezProcContext context = (GenTezProcContext) procContext;

    assert context != null && context.currentTask != null
        && context.currentRootOperator != null;

    // Operator is a file sink or reduce sink. Something that forces
    // a new vertex.
    Operator<?> operator = (Operator<?>) nd;

    // root is the start of the operator pipeline we're currently
    // packing into a vertex, typically a table scan, union or join
    Operator<?> root = context.currentRootOperator;

    LOG.debug("Root operator: " + root);
    LOG.debug("Leaf operator: " + operator);

    if (context.clonedReduceSinks.contains(operator)) {
      // if we're visiting a terminal we've created ourselves,
      // just skip and keep going
      return null;
    }

    TezWork tezWork = context.currentTask.getWork();

    // Right now the work graph is pretty simple. If there is no
    // Preceding work we have a root and will generate a map
    // vertex. If there is a preceding work we will generate
    // a reduce vertex
    BaseWork work;
    if (context.rootToWorkMap.containsKey(root)) {
      // having seen the root operator before means there was a branch in the
      // operator graph. There's typically two reasons for that: a) mux/demux
      // b) multi insert. Mux/Demux will hit the same leaf again, multi insert
      // will result into a vertex with multiple FS or RS operators.

      // At this point we don't have to do anything special in this case. Just
      // run through the regular paces w/o creating a new task.
      work = context.rootToWorkMap.get(root);
    } else {
      // create a new vertex
      if (context.preceedingWork == null) {
        work = utils.createMapWork(context, root, tezWork, null);
      } else {
        work = utils.createReduceWork(context, root, tezWork);
      }
      context.rootToWorkMap.put(root, work);
    }

    if (!context.childToWorkMap.containsKey(operator)) {
      List<BaseWork> workItems = new LinkedList<BaseWork>();
      workItems.add(work);
      context.childToWorkMap.put(operator, workItems);
    } else {
      context.childToWorkMap.get(operator).add(work);
    }

    // remember which mapjoin operator links with which work
    if (!context.currentMapJoinOperators.isEmpty()) {
      for (MapJoinOperator mj: context.currentMapJoinOperators) {
        LOG.debug("Processing map join: " + mj);
        // remember the mapping in case we scan another branch of the 
        // mapjoin later
        if (!context.mapJoinWorkMap.containsKey(mj)) {
          List<BaseWork> workItems = new LinkedList<BaseWork>();
          workItems.add(work);
          context.mapJoinWorkMap.put(mj, workItems);
        } else {
          context.mapJoinWorkMap.get(mj).add(work);
        }

        /*
         * this happens in case of map join operations.
         * The tree looks like this:
         *
         *        RS <--- we are here perhaps
         *        |
         *     MapJoin
         *     /     \
         *   RS       TS
         *  /
         * TS
         *
         * If we are at the RS pointed above, and we may have already visited the
         * RS following the TS, we have already generated work for the TS-RS.
         * We need to hook the current work to this generated work.
         */
        if (context.linkOpWithWorkMap.containsKey(mj)) {
          Map<BaseWork,TezEdgeProperty> linkWorkMap = context.linkOpWithWorkMap.get(mj);
          if (linkWorkMap != null) {
            if (context.linkChildOpWithDummyOp.containsKey(mj)) {
              for (Operator<?> dummy: context.linkChildOpWithDummyOp.get(mj)) {
                work.addDummyOp((HashTableDummyOperator) dummy);
              }
            }
            for (Entry<BaseWork,TezEdgeProperty> parentWorkMap : linkWorkMap.entrySet()) {
              BaseWork parentWork = parentWorkMap.getKey();
              LOG.debug("connecting "+parentWork.getName()+" with "+work.getName());
              TezEdgeProperty edgeProp = parentWorkMap.getValue();
              tezWork.connect(parentWork, work, edgeProp);
              
              // need to set up output name for reduce sink now that we know the name
              // of the downstream work
              for (ReduceSinkOperator r:
                     context.linkWorkWithReduceSinkMap.get(parentWork)) {
                if (r.getConf().getOutputName() != null) {
                  LOG.debug("Cloning reduce sink for multi-child broadcast edge");
                  // we've already set this one up. Need to clone for the next work.
                  r = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(
                      (ReduceSinkDesc)r.getConf().clone(), r.getParentOperators());
                  context.clonedReduceSinks.add(r);
                }
                r.getConf().setOutputName(work.getName());
                context.connectedReduceSinks.add(r);
              }
            }
          }
        }
      }
      // clear out the set. we don't need it anymore.
      context.currentMapJoinOperators.clear();
    }

    // This is where we cut the tree as described above. We also remember that
    // we might have to connect parent work with this work later.
    for (Operator<?> parent: new ArrayList<Operator<?>>(root.getParentOperators())) {
      context.leafOperatorToFollowingWork.put(parent, work);
      LOG.debug("Removing " + parent + " as parent from " + root);
      root.removeParent(parent);
    }

    if (!context.currentUnionOperators.isEmpty()) {      
      // if there are union all operators we need to add the work to the set
      // of union operators.

      UnionWork unionWork;
      if (context.unionWorkMap.containsKey(operator)) {
        // we've seen this terminal before and have created a union work object.
        // just need to add this work to it. There will be no children of this one
        // since we've passed this operator before.
        assert operator.getChildOperators().isEmpty();
        unionWork = (UnionWork) context.unionWorkMap.get(operator);

      } else {
        // first time through. we need to create a union work object and add this
        // work to it. Subsequent work should reference the union and not the actual
        // work.
        unionWork = utils.createUnionWork(context, operator, tezWork);
      }

      // finally hook everything up
      LOG.debug("Connecting union work ("+unionWork+") with work ("+work+")");
      TezEdgeProperty edgeProp = new TezEdgeProperty(EdgeType.CONTAINS);
      tezWork.connect(unionWork, work, edgeProp);
      unionWork.addUnionOperators(context.currentUnionOperators);
      context.currentUnionOperators.clear();
      context.workWithUnionOperators.add(work);
      work = unionWork;
    }

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

      LOG.debug("Second pass. Leaf operator: "+operator
        +" has common downstream work:"+followingWork);

      // need to add this branch to the key + value info
      assert operator instanceof ReduceSinkOperator
        && followingWork instanceof ReduceWork;
      ReduceSinkOperator rs = (ReduceSinkOperator) operator;
      ReduceWork rWork = (ReduceWork) followingWork;
      GenMapRedUtils.setKeyAndValueDesc(rWork, rs);

      // remember which parent belongs to which tag
      rWork.getTagToInput().put(rs.getConf().getTag(), work.getName());

      // remember the output name of the reduce sink
      rs.getConf().setOutputName(rWork.getName());

      if (!context.connectedReduceSinks.contains(rs)) {
        // add dependency between the two work items
        TezEdgeProperty edgeProp = new TezEdgeProperty(EdgeType.SIMPLE_EDGE);
        tezWork.connect(work, rWork, edgeProp);
        context.connectedReduceSinks.add(rs);
      }
    } else {
      LOG.debug("First pass. Leaf operator: "+operator);
    }

    // No children means we're at the bottom. If there are more operators to scan
    // the next item will be a new root.
    if (!operator.getChildOperators().isEmpty()) {
      assert operator.getChildOperators().size() == 1;
      context.parentOfRoot = operator;
      context.currentRootOperator = operator.getChildOperators().get(0);
      context.preceedingWork = work;
    }

    return null;
  }
}
