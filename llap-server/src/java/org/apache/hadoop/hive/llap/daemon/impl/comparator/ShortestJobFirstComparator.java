/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.daemon.impl.comparator;

import java.util.Comparator;

import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorService.TaskWrapper;
import org.apache.hadoop.hive.llap.daemon.impl.TaskRunnerCallable;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;

// if map tasks and reduce tasks are in finishable state then priority is given to the task
// that has less number of pending tasks (shortest job)
public class ShortestJobFirstComparator implements Comparator<TaskWrapper> {

  @Override
  public int compare(TaskWrapper t1, TaskWrapper t2) {
    TaskRunnerCallable o1 = t1.getTaskRunnerCallable();
    TaskRunnerCallable o2 = t2.getTaskRunnerCallable();
    boolean o1CanFinish = o1.canFinish();
    boolean o2CanFinish = o2.canFinish();
    if (o1CanFinish == true && o2CanFinish == false) {
      return -1;
    } else if (o1CanFinish == false && o2CanFinish == true) {
      return 1;
    }

    LlapDaemonProtocolProtos.FragmentRuntimeInfo fri1 = o1.getFragmentRuntimeInfo();
    LlapDaemonProtocolProtos.FragmentRuntimeInfo fri2 = o2.getFragmentRuntimeInfo();

    // Check if these belong to the same task, and work with withinDagPriority
    if (o1.getQueryId().equals(o2.getQueryId())) {
      // Same Query
      // Within dag priority - lower values indicate higher priority.
      return Integer.compare(fri1.getWithinDagPriority(), fri2.getWithinDagPriority());
    }

    // Compute knownPending tasks. selfAndUpstream indicates task counts for current vertex and
    // it's parent hierarchy. selfAndUpstreamComplete indicates how many of these have completed.
    int knownPending1 = fri1.getNumSelfAndUpstreamTasks() - fri1.getNumSelfAndUpstreamCompletedTasks();
    int knownPending2 = fri2.getNumSelfAndUpstreamTasks() - fri2.getNumSelfAndUpstreamCompletedTasks();
    // longer the wait time for an attempt wrt to its start time, higher the priority it gets
    long waitTime1 = fri1.getCurrentAttemptStartTime() - fri1.getFirstAttemptStartTime();
    long waitTime2 = fri2.getCurrentAttemptStartTime() - fri2.getFirstAttemptStartTime();

    if (waitTime1 == 0 || waitTime2 == 0) {
      return knownPending1 - knownPending2;
    }

    double ratio1 = (double) knownPending1 / (double) waitTime1;
    double ratio2 = (double) knownPending2 / (double) waitTime2;
    if (ratio1 < ratio2) {
      return -1;
    } else if (ratio1 > ratio2) {
      return 1;
    }

    return 0;
  }
}
