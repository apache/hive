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

package org.apache.hadoop.hive.ql.parse.spark;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;
import org.apache.hadoop.hive.ql.plan.SparkWork;

import com.google.common.base.Preconditions;

/**
 * GenSparkWork separates the operator tree into spark tasks.
 * It is called once per leaf operator (operator that forces a new execution unit.)
 * and break the operators into work and tasks along the way.
 *
 * Cloned from GenTezWork.
 */
public class GenSparkWork implements SemanticNodeProcessor {
  static final private Logger LOG = LoggerFactory.getLogger(GenSparkWork.class.getName());

  // instance of shared utils
  private GenSparkUtils utils = null;

  /**
   * Constructor takes utils as parameter to facilitate testing
   */
  public GenSparkWork(GenSparkUtils utils) {
    this.utils = utils;
  }

  @Override
  public Object process(Node nd, Stack<Node> stack,
                        NodeProcessorCtx procContext, Object... nodeOutputs) throws SemanticException {
    GenSparkProcContext context = (GenSparkProcContext) procContext;

    Preconditions.checkArgument(context != null,
        "AssertionError: expected context to be not null");
    Preconditions.checkArgument(context.currentTask != null,
        "AssertionError: expected context.currentTask to be not null");
    Preconditions.checkArgument(context.currentRootOperator != null,
        "AssertionError: expected context.currentRootOperator to be not null");

    // Operator is a file sink or reduce sink. Something that forces a new vertex.
    @SuppressWarnings("unchecked")
    Operator<? extends OperatorDesc> operator = (Operator<? extends OperatorDesc>) nd;

    // root is the start of the operator pipeline we're currently
    // packing into a vertex, typically a table scan, union or join
    Operator<?> root = context.currentRootOperator;

    LOG.debug("Root operator: " + root);
    LOG.debug("Leaf operator: " + operator);

    SparkWork sparkWork = context.currentTask.getWork();
    SMBMapJoinOperator smbOp = GenSparkUtils.getChildOperator(root, SMBMapJoinOperator.class);

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
        if (smbOp == null) {
          work = utils.createMapWork(context, root, sparkWork, null);
        } else {
          //save work to be initialized later with SMB information.
          work = utils.createMapWork(context, root, sparkWork, null, true);
          context.smbMapJoinCtxMap.get(smbOp).mapWork = (MapWork) work;
        }
      } else {
        work = utils.createReduceWork(context, root, sparkWork);
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
        // remember the mapping in case we scan another branch of the mapjoin later
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
          Map<BaseWork, SparkEdgeProperty> linkWorkMap = context.linkOpWithWorkMap.get(mj);
          if (linkWorkMap != null) {
            if (context.linkChildOpWithDummyOp.containsKey(mj)) {
              for (Operator<?> dummy: context.linkChildOpWithDummyOp.get(mj)) {
                work.addDummyOp((HashTableDummyOperator) dummy);
              }
            }
            for (Entry<BaseWork, SparkEdgeProperty> parentWorkMap : linkWorkMap.entrySet()) {
              BaseWork parentWork = parentWorkMap.getKey();
              LOG.debug("connecting " + parentWork.getName() + " with " + work.getName());
              SparkEdgeProperty edgeProp = parentWorkMap.getValue();
              sparkWork.connect(parentWork, work, edgeProp);

              // need to set up output name for reduce sink now that we know the name
              // of the downstream work
              for (ReduceSinkOperator r : context.linkWorkWithReduceSinkMap.get(parentWork)) {
                if (r.getConf().getOutputName() != null) {
                  LOG.debug("Cloning reduce sink for multi-child broadcast edge");
                  // we've already set this one up. Need to clone for the next work.
                  r = (ReduceSinkOperator) OperatorFactory.getAndMakeChild(
                      r.getCompilationOpContext(), (ReduceSinkDesc)r.getConf().clone(),
                      r.getParentOperators());
                }
                r.getConf().setOutputName(work.getName());
              }
            }
          }
        }
      }
      // clear out the set. we don't need it anymore.
      context.currentMapJoinOperators.clear();
    }

    // Here we are disconnecting root with its parents. However, we need to save
    // a few information, since in future we may reach the parent operators via a
    // different path, and we may need to connect parent works with the work associated
    // with this root operator.
    if (root.getNumParent() > 0) {
      Preconditions.checkArgument(work instanceof ReduceWork,
          "AssertionError: expected work to be a ReduceWork, but was " + work.getClass().getName());
      ReduceWork reduceWork = (ReduceWork) work;
      for (Operator<?> parent : new ArrayList<Operator<?>>(root.getParentOperators())) {
        Preconditions.checkArgument(parent instanceof ReduceSinkOperator,
          "AssertionError: expected operator to be a ReduceSinkOperator, but was "
          + parent.getClass().getName());
        ReduceSinkOperator rsOp = (ReduceSinkOperator) parent;
        SparkEdgeProperty edgeProp = GenSparkUtils.getEdgeProperty(context.conf, rsOp, reduceWork);

        rsOp.getConf().setOutputName(reduceWork.getName());
        GenMapRedUtils.setKeyAndValueDesc(reduceWork, rsOp);

        context.leafOpToFollowingWorkInfo.put(rsOp, Pair.of(edgeProp, reduceWork));
        LOG.debug("Removing " + parent + " as parent from " + root);
        root.removeParent(parent);
      }
    }

    // If `currentUnionOperators` is not empty, it means we are creating BaseWork whose operator tree
    // contains union operators. In this case, we need to save these BaseWorks, and remove
    // the union operators from the operator tree later.
    if (!context.currentUnionOperators.isEmpty()) {
      context.currentUnionOperators.clear();
      context.workWithUnionOperators.add(work);
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
    if (context.leafOpToFollowingWorkInfo.containsKey(operator)) {
      Pair<SparkEdgeProperty, ReduceWork> childWorkInfo = context.leafOpToFollowingWorkInfo.get(operator);
      SparkEdgeProperty edgeProp = childWorkInfo.getLeft();
      ReduceWork childWork = childWorkInfo.getRight();

      LOG.debug("Second pass. Leaf operator: " + operator + " has common downstream work:" + childWork);

      // We may have already connected `work` with `childWork`, in case, for example, lateral view:
      //    TS
      //     |
      //    ...
      //     |
      //    LVF
      //     | \
      //    SEL SEL
      //     |    |
      //    LVJ-UDTF
      //     |
      //    SEL
      //     |
      //    RS
      // Here, RS can be reached from TS via two different paths. If there is any child work after RS,
      // we don't want to connect them with the work associated with TS more than once.
      if (sparkWork.getEdgeProperty(work, childWork) == null) {
        sparkWork.connect(work, childWork, edgeProp);
      } else {
        LOG.debug("work " + work.getName() + " is already connected to " + childWork.getName() + " before");
      }
    } else {
      LOG.debug("First pass. Leaf operator: " + operator);
    }

    // No children means we're at the bottom. If there are more operators to scan
    // the next item will be a new root.
    if (!operator.getChildOperators().isEmpty()) {
      Preconditions.checkArgument(operator.getChildOperators().size() == 1,
        "AssertionError: expected operator.getChildOperators().size() to be 1, but was "
        + operator.getChildOperators().size());
      context.parentOfRoot = operator;
      context.currentRootOperator = operator.getChildOperators().get(0);
      context.preceedingWork = work;
    }

    return null;
  }
}
