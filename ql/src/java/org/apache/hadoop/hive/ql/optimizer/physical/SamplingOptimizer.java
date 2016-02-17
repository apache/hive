/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hive.ql.optimizer.physical;

import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;

/**
 * Mark final MapredWork for ORDER BY to use sampling and set number of reduce task as -1
 */
public class SamplingOptimizer implements PhysicalPlanResolver {

  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    for (Task<?> task : pctx.getRootTasks()) {
      if (!(task instanceof MapRedTask) || !((MapRedTask)task).getWork().isFinalMapRed()) {
        continue; // this could be replaced by bucketing on RS + bucketed fetcher for next MR
      }
      MapredWork mrWork = ((MapRedTask) task).getWork();
      MapWork mapWork = mrWork.getMapWork();
      ReduceWork reduceWork = mrWork.getReduceWork();

      if (reduceWork == null || reduceWork.getNumReduceTasks() != 1
          || mapWork.getAliasToWork().size() != 1 || mapWork.getSamplingType() > 0
          || reduceWork.getReducer() == null) {
        continue;
      }
      // GROUPBY operator in reducer may not be processed in parallel. Skip optimizing.
      if (OperatorUtils.findSingleOperator(reduceWork.getReducer(), GroupByOperator.class) != null) {
        continue;
      }
      Operator<?> operator = mapWork.getAliasToWork().values().iterator().next();
      if (!(operator instanceof TableScanOperator)) {
        continue;
      }
      ReduceSinkOperator child =
          OperatorUtils.findSingleOperator(operator, ReduceSinkOperator.class);
      if (child == null ||
          child.getConf().getNumReducers() != 1 || !child.getConf().getPartitionCols().isEmpty()) {
        continue;
      }
      child.getConf().setNumReducers(-1);
      reduceWork.setNumReduceTasks(-1);
      mapWork.setSamplingType(MapWork.SAMPLING_ON_START);
    }
    return pctx;
  }
}
