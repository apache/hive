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
  }

  public void setRootOperators(Deque<Operator<? extends OperatorDesc>> roots) {
    this.rootOperators = roots;
  }
}
