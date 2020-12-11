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

package org.apache.hadoop.hive.ql.impala.plan;

import com.google.common.base.Preconditions;

import org.apache.hadoop.hive.ql.engine.EngineEventSequence;
import org.apache.hadoop.hive.ql.impala.ImpalaEventSequence;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.fs.Path;
import org.apache.impala.planner.PlanNode;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.util.EventSequence;

public class ImpalaCompiledPlan {

  private TExecRequest execRequest;
  private final EngineEventSequence timeline;
  private ImpalaPlanner planner;
  private PlanNode planNode;
  private final boolean isExplain;

  public ImpalaCompiledPlan(ImpalaPlanner planner, PlanNode planNode, EngineEventSequence timeline,
      boolean isExplain) {
    this.timeline = timeline;
    this.planner = planner;
    this.planNode = planNode;
    this.isExplain = isExplain;
  }

  public EngineEventSequence getTimeline() {
    return timeline;
  }

  public boolean getIsExplain() {
    return isExplain;
  }

  public String getExplain() {
    return execRequest.getQuery_exec_request().getQuery_plan();
  }

  public void createExecRequest(Path location, boolean isOverwrite, long writeTxn)
       throws HiveException {
    Preconditions.checkState(execRequest == null, "Impala execution request should only be created once");
    this.execRequest = planner.createExecRequest(planNode, isExplain, location, isOverwrite, writeTxn);
  }

  public TExecRequest getExecRequest() {
    // Update the TExecRequest with up to date timeline
    EventSequence eventSequence = ((ImpalaEventSequence) timeline).getEventSequence();
    execRequest.setTimeline(eventSequence.toThrift());
    return execRequest;
  }
}
