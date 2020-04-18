/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.daemon.impl.comparator;

import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorService;
import org.apache.hadoop.hive.llap.daemon.impl.TaskRunnerCallable;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;

import java.util.Comparator;

// Non-guaranteed tasks always have higher-priority for preemption and then
// tasks that can not Finish. If both tasks are non-guaranteed and canNotFinish
// we first preempt the task that would loose less work (lower completed percentage)
// and finally, if tasks have done the same ammount of progress pick the one that waited longer
public class PreemptionQueueComparator implements Comparator<TaskExecutorService.TaskWrapper> {

  @Override
  public int compare(TaskExecutorService.TaskWrapper t1, TaskExecutorService.TaskWrapper t2) {
    TaskRunnerCallable o1 = t1.getTaskRunnerCallable();
    TaskRunnerCallable o2 = t2.getTaskRunnerCallable();
    LlapDaemonProtocolProtos.FragmentRuntimeInfo fri1 = o1.getFragmentRuntimeInfo();
    LlapDaemonProtocolProtos.FragmentRuntimeInfo fri2 = o2.getFragmentRuntimeInfo();

    // If one of the tasks is not guaranteed it is returned for preemption
    boolean v1 = o1.isGuaranteed(), v2 = o2.isGuaranteed();
    if (v1 != v2) {
      return v1 ? 1 : -1;
    }

    // If one of the tasks is non-finishable it is returned for preemption
    v1 = o1.canFinishForPriority();
    v2 = o2.canFinishForPriority();
    if (v1 != v2) {
      return v1 ? 1 : -1;
    }

    // The task that has the LEAST complete percentage will be preempted first (less lost work)
    double completePercentTask1 = (double)  fri1.getNumSelfAndUpstreamCompletedTasks() /
        (double) fri1.getNumSelfAndUpstreamTasks();
    double completePercentTask2 = (double)  fri2.getNumSelfAndUpstreamCompletedTasks() /
        (double) fri2.getNumSelfAndUpstreamTasks();

    if (completePercentTask1 > completePercentTask2) {
      return 1;
    } else if (completePercentTask1 < completePercentTask2) {
      return -1;
    }

    // When completion percentage is the same, preempt the task that waited LESS first
    long waitTime1 = fri1.getCurrentAttemptStartTime() - fri1.getFirstAttemptStartTime();
    long waitTime2 = fri2.getCurrentAttemptStartTime() - fri2.getFirstAttemptStartTime();
    // TODO: Should we check equality as well?
    return Long.compare(waitTime2, waitTime1);
  }
}
