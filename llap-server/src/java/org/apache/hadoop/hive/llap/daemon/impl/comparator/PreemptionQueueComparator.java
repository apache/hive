package org.apache.hadoop.hive.llap.daemon.impl.comparator;

import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorService;
import org.apache.hadoop.hive.llap.daemon.impl.TaskRunnerCallable;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;

import java.util.Comparator;

// Non-guaranteed tasks always have higher-priority for preemption and then
// tasks that can not Finish. If they are both non-guaranteed and canNotFinish
// we first preempt tasks that have more pending tasks and then the ones that waited longer
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

    // The task that has the LEAST upstream tasks will be preempted first
    if (fri1.getNumSelfAndUpstreamTasks() > fri2.getNumSelfAndUpstreamTasks()) {
      return 1;
    } else if (fri1.getNumSelfAndUpstreamTasks() < fri2.getNumSelfAndUpstreamTasks()) {
      return -1;
    }

    // When upstream tasks are the same, preempt the task that waited LESS first
    long waitTime1 = fri1.getCurrentAttemptStartTime() - fri1.getFirstAttemptStartTime();
    long waitTime2 = fri2.getCurrentAttemptStartTime() - fri2.getFirstAttemptStartTime();
    // TODO: Should we check equality as well?
    return Long.compare(waitTime2, waitTime1);
  }
}
