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

package org.apache.hadoop.hive.ql.reexec;

import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;

public class ReExecutionLockAcquisitionPlugin implements IReExecutionPlugin {

  private Driver coreDriver;

  @Override
  public void initialize(Driver driver) {
    coreDriver = driver;
  }

  @Override
  public void beforeExecute(int executionIndex, boolean explainReOptimization) {
  }

  @Override
  public boolean shouldReExecute(int executionNum, CommandProcessorException ex) {
    return ex != null && ex.getMessage().contains(Driver.SNAPSHOT_WAS_OUTDATED_WHEN_LOCKS_WERE_ACQUIRED);
  }

  @Override
  public void prepareToReExecute() {
  }

  @Override
  public boolean shouldReExecute(int executionNum, PlanMapper oldPlanMapper, PlanMapper newPlanMapper) {
    return executionNum == 1;
  }

  @Override
  public void afterExecute(PlanMapper planMapper, boolean successfull) {
  }
}
