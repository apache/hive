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

import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * OptimizeTezProcContext. OptimizeTezProcContext maintains information
 * about the current operator plan as we walk the operator tree
 * to do some additional optimizations on it.
 *
 */
public class OptimizeTezProcContext implements NodeProcessorCtx{

  public final ParseContext parseContext;
  public final HiveConf conf;

  public final Set<ReadEntity> inputs;
  public final Set<WriteEntity> outputs;

  /* Two of the optimization rules, ConvertJoinMapJoin and RemoveDynamicPruningBySize, are put into
     stats dependent optimizations and run together in TezCompiler. There's no guarantee which one
     runs first, but in either case, the prior one may have removed a chain which the latter one is
     not aware of. So we need to remember the leaf node(s) of that chain so it can be skipped.

     For example, as ConvertJoinMapJoin is removing the reduce sink, it may also have removed a
     dynamic partition pruning operator chain. However, RemoveDynamicPruningBySize doesn't know this
     and still tries to traverse that removed chain which will cause NPE.

     This may also happen when RemoveDynamicPruningBySize happens first.
    */
  public HashSet<AppMasterEventOperator> pruningOpsRemovedByPriorOpt;

  public final Set<ReduceSinkOperator> visitedReduceSinks
    = new HashSet<ReduceSinkOperator>();

  public final Multimap<AppMasterEventOperator, TableScanOperator> eventOpToTableScanMap =
      HashMultimap.create();

  // rootOperators are all the table scan operators in sequence
  // of traversal
  public Deque<Operator<? extends OperatorDesc>> rootOperators;

  public OptimizeTezProcContext(HiveConf conf, ParseContext parseContext, Set<ReadEntity> inputs,
      Set<WriteEntity> outputs) {

    this.conf = conf;
    this.parseContext = parseContext;
    this.inputs = inputs;
    this.outputs = outputs;
    this.pruningOpsRemovedByPriorOpt = new HashSet<AppMasterEventOperator>();
  }

  public void setRootOperators(Deque<Operator<? extends OperatorDesc>> roots) {
    this.rootOperators = roots;
  }
}
