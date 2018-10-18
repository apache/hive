/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * OptimizeSparkProcContext. OptimizeSparkProcContext maintains information
 * about the current operator plan as we walk the operator tree
 * to do some additional optimizations on it. clone from OptimizeTezProcContext.
 *
 */
public class OptimizeSparkProcContext implements NodeProcessorCtx {

  private final ParseContext parseContext;
  private final HiveConf conf;
  private final Set<ReadEntity> inputs;
  private final Set<WriteEntity> outputs;
  private final Set<ReduceSinkOperator> visitedReduceSinks = new HashSet<ReduceSinkOperator>();
  private final Map<MapJoinOperator, Long> mjOpSizes = new HashMap<MapJoinOperator, Long>();

  public OptimizeSparkProcContext(HiveConf conf, ParseContext parseContext,
    Set<ReadEntity> inputs, Set<WriteEntity> outputs) {
    this.conf = conf;
    this.parseContext = parseContext;
    this.inputs = inputs;
    this.outputs = outputs;
  }

  public ParseContext getParseContext() {
    return parseContext;
  }

  public HiveConf getConf() {
    return conf;
  }

  public Set<ReadEntity> getInputs() {
    return inputs;
  }

  public Set<WriteEntity> getOutputs() {
    return outputs;
  }

  public Set<ReduceSinkOperator> getVisitedReduceSinks() {
    return visitedReduceSinks;
  }

  public Map<MapJoinOperator, Long> getMjOpSizes() {
    return mjOpSizes;
  }
}
