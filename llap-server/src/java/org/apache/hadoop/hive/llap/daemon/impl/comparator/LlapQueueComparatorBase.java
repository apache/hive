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

import org.apache.hadoop.hive.llap.daemon.impl.TaskRunnerCallable;
import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorService.TaskWrapper;

/**
 * The base class for LLAP queue comparators that checks the criteria that always apply for task
 * priorities, and then lets the specific implementations deal with heuristics, etc.
 */
public abstract class LlapQueueComparatorBase implements Comparator<TaskWrapper> {
  @Override
  public int compare(TaskWrapper t1, TaskWrapper t2) {
    TaskRunnerCallable o1 = t1.getTaskRunnerCallable();
    TaskRunnerCallable o2 = t2.getTaskRunnerCallable();

    // Regardless of other criteria, ducks are always more important than non-ducks.
    boolean v1 = o1.isGuaranteed(), v2 = o2.isGuaranteed();
    if (v1 != v2) return v1 ? -1 : 1;

    // Then, finishable must always precede non-finishable.
    v1 = o1.canFinishForPriority();
    v2 = o2.canFinishForPriority();
    if (v1 != v2) return v1 ? -1 : 1;

    // Note: query priorities, if we add them, might go here.

    // After that, a heuristic is used to decide.
    return compareInternal(o1, o2);
  }

  public abstract int compareInternal(TaskRunnerCallable o1, TaskRunnerCallable o2);
}