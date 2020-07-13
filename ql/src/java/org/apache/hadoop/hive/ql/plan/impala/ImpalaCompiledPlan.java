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

package org.apache.hadoop.hive.ql.plan.impala;

import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.util.EventSequence;

public class ImpalaCompiledPlan {

  private final TExecRequest execRequest;
  private final EventSequence timeline;

  public ImpalaCompiledPlan(TExecRequest execRequest, EventSequence timeline) {
    this.execRequest = execRequest;
    this.timeline = timeline;
  }

  public EventSequence getTimeline() {
    return timeline;
  }

  public String getExplain() {
    return execRequest.getQuery_exec_request().getQuery_plan();
  }

  public TExecRequest getExecRequest() {
    // Update the TExecRequest with up to date timeline
    execRequest.setTimeline(getTimeline().toThrift());
    return execRequest;
  }
}
