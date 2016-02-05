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

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.DependencyCollectionTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.UnionWork;

/**
 * GenTezProcContext. GenTezProcContext maintains information
 * about the tasks and operators as we walk the operator tree
 * to break them into TezTasks.
 *
 */
public class GenTezProcContext implements NodeProcessorCtx{

  public final ParseContext parseContext;
  public final HiveConf conf;
  public final List<Task<MoveWork>> moveTask;

  // rootTasks is the entry point for all generated tasks
  public final List<Task<? extends Serializable>> rootTasks;

  public final Set<ReadEntity> inputs;
  public final Set<WriteEntity> outputs;

  // holds the root of the operator tree we're currently processing
  // this could be a table scan, but also a join, ptf, etc (i.e.:
  // first operator of a reduce task.
  public Operator<? extends OperatorDesc> currentRootOperator;

  // this is the original parent of the currentRootOperator as we scan
  // through the graph. A root operator might have multiple parents and
  // we just use this one to remember where we came from in the current
  // walk.
  public Operator<? extends OperatorDesc> parentOfRoot;

  // sequence number is used to name vertices (e.g.: Map 1, Reduce 14, ...)
  private AtomicInteger sequenceNumber;

  // tez task we're currently processing
  public TezTask currentTask;

  // last work we've processed (in order to hook it up to the current
  // one.
  public BaseWork preceedingWork;

  // map that keeps track of the last operator of a task to the work
  // that follows it. This is used for connecting them later.
  public final Map<Operator<?>, BaseWork> leafOperatorToFollowingWork;

  // a map that keeps track of work that need to be linked while
  // traversing an operator tree
  public final Map<Operator<?>, Map<BaseWork,TezEdgeProperty>> linkOpWithWorkMap;

  // a map to keep track of what reduce sinks have to be hooked up to
  // map join work
  public final Map<BaseWork, List<ReduceSinkOperator>> linkWorkWithReduceSinkMap;

  // map that says which mapjoin belongs to which work item
  public final Map<MapJoinOperator, List<BaseWork>> mapJoinWorkMap;

  // Mapping of reducesink to mapjoin operators
  // Only used for dynamic partitioned hash joins (mapjoin operator in the reducer)
  public final Map<Operator<?>, MapJoinOperator> smallTableParentToMapJoinMap;

  // a map to keep track of which root generated which work
  public final Map<Operator<?>, BaseWork> rootToWorkMap;

  // a map to keep track of which child generated with work
  public final Map<Operator<?>, List<BaseWork>> childToWorkMap;

  // we need to keep the original list of operators in the map join to know
  // what position in the mapjoin the different parent work items will have.
  public final Map<MapJoinOperator, List<Operator<?>>> mapJoinParentMap;

  // remember the dummy ops we created
  public final Map<Operator<?>, List<Operator<?>>> linkChildOpWithDummyOp;

  // used to group dependent tasks for multi table inserts
  public final DependencyCollectionTask dependencyTask;

  // remember map joins as we encounter them.
  public final Set<MapJoinOperator> currentMapJoinOperators;

  // used to hook up unions
  public final Map<Operator<?>, BaseWork> unionWorkMap;
  public final Map<Operator<?>, UnionWork> rootUnionWorkMap;
  public List<UnionOperator> currentUnionOperators;
  public final Set<BaseWork> workWithUnionOperators;
  public final Set<ReduceSinkOperator> clonedReduceSinks;

  // we link filesink that will write to the same final location
  public final Map<Path, List<FileSinkDesc>> linkedFileSinks;
  public final Set<FileSinkOperator> fileSinkSet;

  // remember which reducesinks we've already connected
  public final Set<ReduceSinkOperator> connectedReduceSinks;
  public final Map<Operator<?>, MergeJoinWork> opMergeJoinWorkMap;
  public CommonMergeJoinOperator currentMergeJoinOperator;

  // remember the event operators we've seen
  public final Set<AppMasterEventOperator> eventOperatorSet;

  // remember the event operators we've abandoned.
  public final Set<AppMasterEventOperator> abandonedEventOperatorSet;

  // remember the connections between ts and event
  public final Map<TableScanOperator, List<AppMasterEventOperator>> tsToEventMap;

  // When processing dynamic partitioned hash joins, some of the small tables may not get processed
  // before the mapjoin's parents are removed during GenTezWork.process(). This is to keep
  // track of which small tables haven't been processed yet.
  public Map<MapJoinOperator, Set<ReduceSinkOperator>> mapJoinToUnprocessedSmallTableReduceSinks;

  @SuppressWarnings("unchecked")
  public GenTezProcContext(HiveConf conf, ParseContext parseContext,
      List<Task<MoveWork>> moveTask, List<Task<? extends Serializable>> rootTasks,
      Set<ReadEntity> inputs, Set<WriteEntity> outputs) {

    this.conf = conf;
    this.parseContext = parseContext;
    this.moveTask = moveTask;
    this.rootTasks = rootTasks;
    this.inputs = inputs;
    this.outputs = outputs;
    this.currentTask = (TezTask) TaskFactory.get(
         new TezWork(conf.getVar(HiveConf.ConfVars.HIVEQUERYID), conf), conf);
    this.leafOperatorToFollowingWork = new LinkedHashMap<Operator<?>, BaseWork>();
    this.linkOpWithWorkMap = new LinkedHashMap<Operator<?>, Map<BaseWork, TezEdgeProperty>>();
    this.linkWorkWithReduceSinkMap = new LinkedHashMap<BaseWork, List<ReduceSinkOperator>>();
    this.smallTableParentToMapJoinMap = new LinkedHashMap<Operator<?>, MapJoinOperator>();
    this.mapJoinWorkMap = new LinkedHashMap<MapJoinOperator, List<BaseWork>>();
    this.rootToWorkMap = new LinkedHashMap<Operator<?>, BaseWork>();
    this.childToWorkMap = new LinkedHashMap<Operator<?>, List<BaseWork>>();
    this.mapJoinParentMap = new LinkedHashMap<MapJoinOperator, List<Operator<?>>>();
    this.currentMapJoinOperators = new LinkedHashSet<MapJoinOperator>();
    this.linkChildOpWithDummyOp = new LinkedHashMap<Operator<?>, List<Operator<?>>>();
    this.dependencyTask = (DependencyCollectionTask)
        TaskFactory.get(new DependencyCollectionWork(), conf);
    this.unionWorkMap = new LinkedHashMap<Operator<?>, BaseWork>();
    this.rootUnionWorkMap = new LinkedHashMap<Operator<?>, UnionWork>();
    this.currentUnionOperators = new LinkedList<UnionOperator>();
    this.workWithUnionOperators = new LinkedHashSet<BaseWork>();
    this.clonedReduceSinks = new LinkedHashSet<ReduceSinkOperator>();
    this.linkedFileSinks = new LinkedHashMap<Path, List<FileSinkDesc>>();
    this.fileSinkSet = new LinkedHashSet<FileSinkOperator>();
    this.connectedReduceSinks = new LinkedHashSet<ReduceSinkOperator>();
    this.eventOperatorSet = new LinkedHashSet<AppMasterEventOperator>();
    this.abandonedEventOperatorSet = new LinkedHashSet<AppMasterEventOperator>();
    this.tsToEventMap = new LinkedHashMap<TableScanOperator, List<AppMasterEventOperator>>();
    this.opMergeJoinWorkMap = new LinkedHashMap<Operator<?>, MergeJoinWork>();
    this.currentMergeJoinOperator = null;
    this.mapJoinToUnprocessedSmallTableReduceSinks = new HashMap<MapJoinOperator, Set<ReduceSinkOperator>>();
    this.sequenceNumber = parseContext.getContext().getSequencer();

    rootTasks.add(currentTask);
  }

  public int nextSequenceNumber() {
     return sequenceNumber.incrementAndGet();
  }
}
