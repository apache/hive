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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.TezWork.VertexType;
import org.apache.hadoop.hive.ql.plan.UnionWork;

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
      if (context.childToWorkMap.containsKey(operator)) {
        // if we've seen both root and child, we can bail.

        // clear out the mapjoin set. we don't need it anymore.
        context.currentMapJoinOperators.clear();

        // clear out the union set. we don't need it anymore.
        context.currentUnionOperators.clear();

        return null;
      } else {
        // At this point we don't have to do anything special. Just
        // run through the regular paces w/o creating a new task.
        work = context.rootToWorkMap.get(root);
      }
    } else {
      // create a new vertex
      if (context.preceedingWork == null) {
        work = utils.createMapWork(context, root, tezWork, null);
      } else {
        work = utils.createReduceWork(context, root, tezWork);
      }
      context.rootToWorkMap.put(root, work);
    }

    // this is where we set the sort columns that we will be using for KeyValueInputMerge
    if (operator instanceof DummyStoreOperator) {
      work.addSortCols(root.getOpTraits().getSortCols().get(0));
    }

    if (!context.childToWorkMap.containsKey(operator)) {
      List<BaseWork> workItems = new LinkedList<BaseWork>();
      workItems.add(work);
      context.childToWorkMap.put(operator, workItems);
    } else {
      context.childToWorkMap.get(operator).add(work);
    }

    // this transformation needs to be first because it changes the work item itself.
    // which can affect the working of all downstream transformations.
    if (context.currentMergeJoinOperator != null) {
      // we are currently walking the big table side of the merge join. we need to create or hook up
      // merge join work.
      MergeJoinWork mergeJoinWork = null;
      if (context.opMergeJoinWorkMap.containsKey(context.currentMergeJoinOperator)) {
        // we have found a merge work corresponding to this closing operator. Hook up this work.
        mergeJoinWork = context.opMergeJoinWorkMap.get(context.currentMergeJoinOperator);
      } else {
        // we need to create the merge join work
        mergeJoinWork = new MergeJoinWork();
        mergeJoinWork.setMergeJoinOperator(context.currentMergeJoinOperator);
        tezWork.add(mergeJoinWork);
        context.opMergeJoinWorkMap.put(context.currentMergeJoinOperator, mergeJoinWork);
      }
      // connect the work correctly.
      work.addSortCols(root.getOpTraits().getSortCols().get(0));
      mergeJoinWork.addMergedWork(work, null, context.leafOperatorToFollowingWork);
      Operator<? extends OperatorDesc> parentOp =
          getParentFromStack(context.currentMergeJoinOperator, stack);
      int pos = context.currentMergeJoinOperator.getTagForOperator(parentOp);
      work.setTag(pos);
      tezWork.setVertexType(work, VertexType.MULTI_INPUT_UNINITIALIZED_EDGES);
      for (BaseWork parentWork : tezWork.getParents(work)) {
        TezEdgeProperty edgeProp = tezWork.getEdgeProperty(parentWork, work);
        tezWork.disconnect(parentWork, work);
        tezWork.connect(parentWork, mergeJoinWork, edgeProp);
      }

      for (BaseWork childWork : tezWork.getChildren(work)) {
        TezEdgeProperty edgeProp = tezWork.getEdgeProperty(work, childWork);
        tezWork.disconnect(work, childWork);
        tezWork.connect(mergeJoinWork, childWork, edgeProp);
      }
      tezWork.remove(work);
      context.rootToWorkMap.put(root, mergeJoinWork);
      context.childToWorkMap.get(operator).remove(work);
      context.childToWorkMap.get(operator).add(mergeJoinWork);
      work = mergeJoinWork;
      context.currentMergeJoinOperator = null;
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
              if (edgeProp.getEdgeType() == EdgeType.CUSTOM_EDGE) {
                tezWork.setVertexType(work, VertexType.INITIALIZED_EDGES);
              }

              // need to set up output name for reduce sink now that we know the name
              // of the downstream work
              for (ReduceSinkOperator r:
                     context.linkWorkWithReduceSinkMap.get(parentWork)) {
                if (r.getConf().getOutputName() != null) {
                  LOG.debug("Cloning reduce sink for multi-child broadcast edge");
                  // we've already set this one up. Need to clone for the next work.
                  r = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(
                      (ReduceSinkDesc)r.getConf().clone(),
                      new RowSchema(r.getSchema()),
                      r.getParentOperators());
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
    for (Operator<?> parent : new ArrayList<Operator<?>>(root.getParentOperators())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removing " + parent + " as parent from " + root);
      }
      context.leafOperatorToFollowingWork.remove(parent);
      context.leafOperatorToFollowingWork.put(parent, work);
      root.removeParent(parent);
    }

    if (!context.currentUnionOperators.isEmpty()) {
      // if there are union all operators, it means that the walking context contains union all operators.
      // please see more details of context.currentUnionOperator in GenTezWorkWalker

      UnionWork unionWork;
      if (context.unionWorkMap.containsKey(operator)) {
        // we've seen this terminal before and have created a union work object.
        // just need to add this work to it. There will be no children of this one
        // since we've passed this operator before.
        assert operator.getChildOperators().isEmpty();
        unionWork = (UnionWork) context.unionWorkMap.get(operator);
        // finally connect the union work with work
        connectUnionWorkWithWork(unionWork, work, tezWork, context);
      } else {
        // we've not seen this terminal before. we need to check
        // rootUnionWorkMap which contains the information of mapping the root
        // operator of a union work to a union work
        unionWork = context.rootUnionWorkMap.get(root);
        if (unionWork == null) {
          // if unionWork is null, it means it is the first time. we need to
          // create a union work object and add this work to it. Subsequent 
          // work should reference the union and not the actual work.
          unionWork = utils.createUnionWork(context, root, operator, tezWork);
          // finally connect the union work with work
          connectUnionWorkWithWork(unionWork, work, tezWork, context);
        }
      }
      context.currentUnionOperators.clear();
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
      long bytesPerReducer = context.conf.getLongVar(HiveConf.ConfVars.BYTESPERREDUCER);

      LOG.debug("Second pass. Leaf operator: "+operator
        +" has common downstream work:"+followingWork);

      if (operator instanceof DummyStoreOperator) {
        // this is the small table side.
        assert (followingWork instanceof MergeJoinWork);
        MergeJoinWork mergeJoinWork = (MergeJoinWork) followingWork;
        CommonMergeJoinOperator mergeJoinOp = mergeJoinWork.getMergeJoinOperator();
        work.setTag(mergeJoinOp.getTagForOperator(operator));
        mergeJoinWork.addMergedWork(null, work, context.leafOperatorToFollowingWork);
        tezWork.setVertexType(mergeJoinWork, VertexType.MULTI_INPUT_UNINITIALIZED_EDGES);
        for (BaseWork parentWork : tezWork.getParents(work)) {
          TezEdgeProperty edgeProp = tezWork.getEdgeProperty(parentWork, work);
          tezWork.disconnect(parentWork, work);
          tezWork.connect(parentWork, mergeJoinWork, edgeProp);
        }
        work = mergeJoinWork;
      } else {
        // need to add this branch to the key + value info
        assert operator instanceof ReduceSinkOperator
            && ((followingWork instanceof ReduceWork) || (followingWork instanceof MergeJoinWork)
                || followingWork instanceof UnionWork);
        ReduceSinkOperator rs = (ReduceSinkOperator) operator;
        ReduceWork rWork = null;
        if (followingWork instanceof MergeJoinWork) {
          MergeJoinWork mergeJoinWork = (MergeJoinWork) followingWork;
          rWork = (ReduceWork) mergeJoinWork.getMainWork();
        } else if (followingWork instanceof UnionWork) {
          // this can only be possible if there is merge work followed by the union
          UnionWork unionWork = (UnionWork) followingWork;
          int index = getFollowingWorkIndex(tezWork, unionWork, rs);
          BaseWork baseWork = tezWork.getChildren(unionWork).get(index);
          if (baseWork instanceof MergeJoinWork) {
            MergeJoinWork mergeJoinWork = (MergeJoinWork) baseWork;
            // disconnect the connection to union work and connect to merge work
            followingWork = mergeJoinWork;
            rWork = (ReduceWork) mergeJoinWork.getMainWork();
          } else {
            rWork = (ReduceWork) baseWork;
          }
        } else {
          rWork = (ReduceWork) followingWork;
        }
        GenMapRedUtils.setKeyAndValueDesc(rWork, rs);

        // remember which parent belongs to which tag
        int tag = rs.getConf().getTag();
        rWork.getTagToInput().put(tag == -1 ? 0 : tag, work.getName());

        // remember the output name of the reduce sink
        rs.getConf().setOutputName(rWork.getName());

        if (!context.connectedReduceSinks.contains(rs)) {
          // add dependency between the two work items
          TezEdgeProperty edgeProp;
          if (rWork.isAutoReduceParallelism()) {
            edgeProp =
                new TezEdgeProperty(context.conf, EdgeType.SIMPLE_EDGE, true,
                    rWork.getMinReduceTasks(), rWork.getMaxReduceTasks(), bytesPerReducer);
          } else {
            edgeProp = new TezEdgeProperty(EdgeType.SIMPLE_EDGE);
          }
          tezWork.connect(work, followingWork, edgeProp);
          context.connectedReduceSinks.add(rs);
        }
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

  private int getFollowingWorkIndex(TezWork tezWork, UnionWork unionWork, ReduceSinkOperator rs)
      throws SemanticException {
    int index = 0;
    for (BaseWork baseWork : tezWork.getChildren(unionWork)) {
      TezEdgeProperty edgeProperty = tezWork.getEdgeProperty(unionWork, baseWork);
      if (edgeProperty.getEdgeType() != TezEdgeProperty.EdgeType.CONTAINS) {
        return index;
      }
      index++;
    }
    throw new SemanticException("Following work not found for the reduce sink: " + rs.getName());
  }

  @SuppressWarnings("unchecked")
  private Operator<? extends OperatorDesc> getParentFromStack(Node currentMergeJoinOperator,
      Stack<Node> stack) {
    int pos = stack.indexOf(currentMergeJoinOperator);
    return (Operator<? extends OperatorDesc>) stack.get(pos - 1);
  }
  
  private void connectUnionWorkWithWork(UnionWork unionWork, BaseWork work, TezWork tezWork,
      GenTezProcContext context) {
    LOG.debug("Connecting union work (" + unionWork + ") with work (" + work + ")");
    TezEdgeProperty edgeProp = new TezEdgeProperty(EdgeType.CONTAINS);
    tezWork.connect(unionWork, work, edgeProp);
    unionWork.addUnionOperators(context.currentUnionOperators);
    context.workWithUnionOperators.add(work);
  }
}
