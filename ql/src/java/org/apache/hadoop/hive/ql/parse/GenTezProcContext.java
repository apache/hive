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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.DependencyCollectionTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DependencyCollectionWork;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;

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
  public final List<UnionOperator> currentUnionOperators;
  public final Set<BaseWork> workWithUnionOperators;
  public final Set<ReduceSinkOperator> clonedReduceSinks;

  // we link filesink that will write to the same final location
  public final Map<Path, List<FileSinkDesc>> linkedFileSinks;
  public final Set<FileSinkOperator> fileSinkSet;

  // remember which reducesinks we've already connected
  public final Set<ReduceSinkOperator> connectedReduceSinks;

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
         new TezWork(conf.getVar(HiveConf.ConfVars.HIVEQUERYID)), conf);
    this.leafOperatorToFollowingWork = new HashMap<Operator<?>, BaseWork>();
    this.linkOpWithWorkMap = new HashMap<Operator<?>, Map<BaseWork, TezEdgeProperty>>();
    this.linkWorkWithReduceSinkMap = new HashMap<BaseWork, List<ReduceSinkOperator>>();
    this.mapJoinWorkMap = new HashMap<MapJoinOperator, List<BaseWork>>();
    this.rootToWorkMap = new HashMap<Operator<?>, BaseWork>();
    this.childToWorkMap = new HashMap<Operator<?>, List<BaseWork>>();
    this.mapJoinParentMap = new HashMap<MapJoinOperator, List<Operator<?>>>();
    this.currentMapJoinOperators = new HashSet<MapJoinOperator>();
    this.linkChildOpWithDummyOp = new HashMap<Operator<?>, List<Operator<?>>>();
    this.dependencyTask = (DependencyCollectionTask)
        TaskFactory.get(new DependencyCollectionWork(), conf);
    this.unionWorkMap = new HashMap<Operator<?>, BaseWork>();
    this.currentUnionOperators = new LinkedList<UnionOperator>();
    this.workWithUnionOperators = new HashSet<BaseWork>();
    this.clonedReduceSinks = new HashSet<ReduceSinkOperator>();
    this.linkedFileSinks = new HashMap<Path, List<FileSinkDesc>>();
    this.fileSinkSet = new HashSet<FileSinkOperator>();
    this.connectedReduceSinks = new HashSet<ReduceSinkOperator>();

    rootTasks.add(currentTask);
  }
}
