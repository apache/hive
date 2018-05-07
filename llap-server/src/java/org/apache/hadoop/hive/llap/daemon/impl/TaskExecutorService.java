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
package org.apache.hadoop.hive.llap.daemon.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.llap.counters.FragmentCountersMap;
import org.apache.hadoop.hive.llap.daemon.FinishableStateUpdateHandler;
import org.apache.hadoop.hive.llap.daemon.SchedulerFragmentCompletingListener;
import org.apache.hadoop.hive.llap.daemon.impl.comparator.LlapQueueComparatorBase;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentRuntimeInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.hive.llap.tezplugins.helpers.MonotonicClock;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.task.EndReason;
import org.apache.tez.runtime.task.TaskRunner2Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Task executor service provides method for scheduling tasks. Tasks submitted to executor service
 * are submitted to wait queue for scheduling. Wait queue tasks are ordered based on the priority
 * of the task. The internal wait queue scheduler moves tasks from wait queue when executor slots
 * are available or when a higher priority task arrives and will schedule it for execution.
 * When pre-emption is enabled, the tasks from wait queue can replace(pre-empt) a running task.
 * The pre-empted task is reported back to the Application Master(AM) for it to be rescheduled.
 * <p/>
 * Because of the concurrent nature of task submission, the position of the task in wait queue is
 * held as long the scheduling of the task from wait queue (with or without pre-emption) is complete.
 * The order of pre-emption is based on the ordering in the pre-emption queue. All tasks that cannot
 * run to completion immediately (canFinish = false) are added to pre-emption queue.
 * <p/>
 * When all the executor threads are occupied and wait queue is full, the task scheduler will
 * return SubmissionState.REJECTED response
 * <p/>
 * Task executor service can be shut down which will terminated all running tasks and reject all
 * new tasks. Shutting down of the task executor service can be done gracefully or immediately.
 */
public class TaskExecutorService extends AbstractService
    implements Scheduler<TaskRunnerCallable>, SchedulerFragmentCompletingListener {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorService.class);
  private static final String TASK_EXECUTOR_THREAD_NAME_FORMAT = "Task-Executor-%d";
  private static final String WAIT_QUEUE_SCHEDULER_THREAD_NAME_FORMAT = "Wait-Queue-Scheduler-%d";
  private static final long PREEMPTION_KILL_GRACE_MS = 500; // 500ms
  private static final int PREEMPTION_KILL_GRACE_SLEEP_MS = 50; // 50ms


  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  // Thread pool for actual execution of work.
  private final ListeningExecutorService executorService;
  @VisibleForTesting
  final EvictingPriorityBlockingQueue<TaskWrapper> waitQueue;
  // Thread pool for taking entities off the wait queue.
  private final ListeningExecutorService waitQueueExecutorService;
  // Thread pool for callbacks on completion of execution of a work unit.
  private final ListeningExecutorService executionCompletionExecutorService;

  @VisibleForTesting
  final BlockingQueue<TaskWrapper> preemptionQueue;
  private final boolean enablePreemption;
  private final ThreadPoolExecutor threadPoolExecutor;
  private final AtomicInteger numSlotsAvailable;
  private final int maxParallelExecutors;
  private final Clock clock;

  // Tracks running fragments, and completing fragments.
  // Completing since we have a race in the AM being notified and the task actually
  // falling off, and the executor service being ready to schedule a new task.
  private final AtomicInteger runningFragmentCount = new AtomicInteger(0);


  @VisibleForTesting
  /**
   * Accessed under the epic lock unless it's used for reporting.
   * Added when calling schedule.
   * Removed if evicted, killed, or finished with success or failure.
   * In the former two cases, killTask is called on TaskRunnerCallable. In most cases, some
   * cleanup is performed under the epic lock.
   */
  final ConcurrentMap<String, TaskWrapper> knownTasks = new ConcurrentHashMap<>();

  private final Object lock = new Object();
  private final LlapDaemonExecutorMetrics metrics;

  public TaskExecutorService(int numExecutors, int waitQueueSize,
      String waitQueueComparatorClassName, boolean enablePreemption,
      ClassLoader classLoader, final LlapDaemonExecutorMetrics metrics, Clock clock) {
    super(TaskExecutorService.class.getSimpleName());
    LOG.info("TaskExecutorService is being setup with parameters: "
        + "numExecutors=" + numExecutors
        + ", waitQueueSize=" + waitQueueSize
        + ", waitQueueComparatorClassName=" + waitQueueComparatorClassName
        + ", enablePreemption=" + enablePreemption);

    final LlapQueueComparatorBase waitQueueComparator = createComparator(
        waitQueueComparatorClassName);
    this.maxParallelExecutors = numExecutors;
    this.waitQueue = new EvictingPriorityBlockingQueue<>(waitQueueComparator, waitQueueSize);
    this.clock = clock == null ? new MonotonicClock() : clock;
    this.threadPoolExecutor = new ThreadPoolExecutor(numExecutors, // core pool size
        numExecutors, // max pool size
        1, TimeUnit.MINUTES, new SynchronousQueue<Runnable>(), // direct hand-off
        new ExecutorThreadFactory(classLoader));
    this.executorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
    this.preemptionQueue = new PriorityBlockingQueue<>(numExecutors,
        new PreemptionQueueComparator());
    this.enablePreemption = enablePreemption;
    this.numSlotsAvailable = new AtomicInteger(numExecutors);
    this.metrics = metrics;
    if (metrics != null) {
      metrics.setNumExecutorsAvailable(numSlotsAvailable.get());
    }

    // single threaded scheduler for tasks from wait queue to executor threads
    ExecutorService wes = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
      .setDaemon(true).setNameFormat(WAIT_QUEUE_SCHEDULER_THREAD_NAME_FORMAT).build());
    this.waitQueueExecutorService = MoreExecutors.listeningDecorator(wes);

    ExecutorService executionCompletionExecutorServiceRaw = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ExecutionCompletionThread #%d")
            .build());
    executionCompletionExecutorService = MoreExecutors.listeningDecorator(
        executionCompletionExecutorServiceRaw);
    ListenableFuture<?> future = waitQueueExecutorService.submit(new WaitQueueWorker());
    Futures.addCallback(future, new WaitQueueWorkerCallback());
  }

  private LlapQueueComparatorBase createComparator(
      String waitQueueComparatorClassName) {
    final LlapQueueComparatorBase waitQueueComparator;
    try {
      Class<? extends LlapQueueComparatorBase> waitQueueComparatorClazz =
          (Class<? extends LlapQueueComparatorBase>) Class.forName(waitQueueComparatorClassName);
      Constructor<? extends LlapQueueComparatorBase> ctor =
          waitQueueComparatorClazz.getConstructor(null);
      waitQueueComparator = ctor.newInstance(null);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          "Failed to load wait queue comparator, class=" + waitQueueComparatorClassName, e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Failed to find constructor for wait queue comparator, class=" +
          waitQueueComparatorClassName, e);
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Failed to find instantiate wait queue comparator, class="
      + waitQueueComparatorClassName, e);
    }
    return waitQueueComparator;
  }

  @Override
  public void serviceStop() {
    shutDown(false);
  }

  private static final ThreadLocal<SimpleDateFormat> sdf = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }
  };

  @Override
  public int getNumActiveForReporting() {
    int result = 0;
    for (Map.Entry<String, TaskWrapper> e : knownTasks.entrySet()) {
      TaskWrapper task = e.getValue();
      if (task.isInWaitQueue()) continue;
      TaskRunnerCallable c = task.getTaskRunnerCallable();
      // Count the tasks in intermediate state as waiting.
      if (c == null || c.getStartTime() == 0) continue;
      ++result;
    }
    return result;
  }

  @Override
  public Set<String> getExecutorsStatusForReporting() {
    // TODO Change this method to make the output easier to parse (parse programmatically)
    Set<String> result = new LinkedHashSet<>();
    Set<String> running = new LinkedHashSet<>();
    Set<String> waiting = new LinkedHashSet<>();
    StringBuilder value = new StringBuilder();

    final List<TaskWrapper> queueState = new ArrayList<>();
    // Note: we don't take the scheduling lock here, although the call to queue is still
    //       synchronized. Best-effort to display the queue in order.
    waitQueue.apply(new Function<TaskWrapper, Boolean>() {
      public Boolean apply(TaskWrapper input) {
        queueState.add(input);
        return true;
      }});

    HashSet<TaskWrapper> queueHs = new HashSet<>();
    for (TaskWrapper task : queueState) {
      describeTask(value, task.getRequestId(), task, true);
      waiting.add(value.toString());
      queueHs.add(task);
    }

    for (Map.Entry<String, TaskWrapper> e : knownTasks.entrySet()) {
      String attemptId = e.getKey();
      TaskWrapper task = e.getValue();
      if (queueHs.contains(task)) {
        // Even if the state has changed, don't log it twice.
        continue;
      }
      boolean isWaiting = describeTask(value, attemptId, task, false);
      if (isWaiting) {
        waiting.add(value.toString());
      } else {
        running.add(value.toString());
      }
    }
    result.addAll(waiting);
    result.addAll(running);
    return result;
  }

  private boolean describeTask(
      StringBuilder value, String attemptId, TaskWrapper task, boolean fromQueue) {
    value.setLength(0);
    boolean isFirst = true;
    TaskRunnerCallable c = task.getTaskRunnerCallable();
    value.append(attemptId);
    if (c != null && c.getVertexSpec() != null) {
      SignableVertexSpec fs = c.getVertexSpec();
      value.append(isFirst ? " (" : ", ").append(c.getQueryId())
        .append("/").append(fs.getVertexName()).append(c.isGuaranteed() ? ", guaranteed" : "");
      isFirst = false;
    }
    value.append(isFirst ? " (" : ", ");
    if (fromQueue) {
      value.append("in queue (in order)");
    }
    boolean isWaiting;
    if (task.isInWaitQueue()) {
      isWaiting = true;
      if (!fromQueue) {
        value.append("in queue (not in order)");
      }
    } else if (c != null) {
      long startTime = c.getStartTime();
      isWaiting = false;
      if (startTime != 0) {
        value.append("started at ").append(sdf.get().format(new Date(startTime)));
      } else {
        value.append("not started");
      }
    } else {
      isWaiting = true;
      value.append("has no callable");
    }
    if (task.isInPreemptionQueue()) {
      value.append(", ").append("in preemption queue");
    }
    boolean canFinish = c.canFinish();
    value.append(", ").append(canFinish ? "can" : "cannot").append(" finish");
    if (canFinish != c.canFinishForPriority()) {
      value.append(" (not updated in queue)");
    }
    value.append(")");
    return isWaiting;
  }

  /**
   * Worker that takes tasks from wait queue and schedule it for execution.
   */
  private final class WaitQueueWorker implements Runnable {
    private static final long SANITY_CHECK_TIMEOUT_MS = 1000;
    private TaskWrapper task;
    private Long nextSanityCheck = null;

    @Override
    public void run() {
      try {
        Long lastKillTimeMs = null;
        SanityChecker sc = null;
        while (!isShutdown.get()) {
          RejectedExecutionException rejectedException = null;
          if (nextSanityCheck != null && ((nextSanityCheck - System.nanoTime()) <= 0)) {
            sc = sanityCheckQueue(sc);
            nextSanityCheck = null;
          }
          synchronized (lock) {
            // Since schedule() can be called from multiple threads, we peek the wait queue, try
            // scheduling the task and then remove the task if scheduling is successful. This
            // will make sure the task's place in the wait queue is held until it gets scheduled.
            task = waitQueue.peek();
            if (task == null) {
              waitOnLock();
              continue;
            }
            // If the task cannot finish and if no slots are available then don't schedule it.
            // Also don't wait if we have a task and we just killed something to schedule it.
            // (numSlotsAvailable can go negative, if the callback after the thread completes is delayed)
            boolean shouldWait = numSlotsAvailable.get() <= 0 && lastKillTimeMs == null;
            boolean canKill = false;
            if (task.canFinishForPriority() || task.isGuaranteed()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Attempting to schedule task {}, canFinish={}. Current state: "
                    + "preemptionQueueSize={}, numSlotsAvailable={}, waitQueueSize={}",
                    task.getRequestId(), task.getTaskRunnerCallable().canFinish(),
                    preemptionQueue.size(), numSlotsAvailable.get(), waitQueue.size());
              }
              canKill = enablePreemption && canPreempt(task, preemptionQueue.peek());
              shouldWait = shouldWait && !canKill;
            }
            if (shouldWait) {
              waitOnLock();
              // Another task at a higher priority may have come in during the wait. Lookup the
              // queue again to pick up the task at the highest priority.
              continue;
            }
            nextSanityCheck = null; // We are going to do something useful now.
            try {
              tryScheduleUnderLock(task);
              // Wait queue could have been re-ordered in the mean time because of concurrent task
              // submission. So remove the specific task instead of the head task.
              if (waitQueue.remove(task)) {
                if (metrics != null) {
                  metrics.setExecutorNumQueuedRequests(waitQueue.size());
                }
              }
              lastKillTimeMs = null; // We have filled the spot we may have killed for (if any).
            } catch (RejectedExecutionException e) {
              rejectedException = e;
            }
          } // synchronized (lock)

          // Handle the rejection outside of the lock
          if (rejectedException != null) {
            if (lastKillTimeMs != null
                && (clock.getTime() - lastKillTimeMs) < PREEMPTION_KILL_GRACE_MS) {
              // We killed something, but still got rejected. Wait a bit to give a chance to our
              // previous victim to actually die.
              synchronized (lock) {
                lock.wait(PREEMPTION_KILL_GRACE_SLEEP_MS);
              }
            } else {
              if (LOG.isDebugEnabled() && lastKillTimeMs != null) {
                LOG.debug("Grace period ended for the previous kill; preemtping more tasks");
              }
              if (handleScheduleAttemptedRejection(task)) {
                lastKillTimeMs = clock.getTime(); // We killed something.
              }
            }
          }
        }
      } catch (InterruptedException e) {
        if (isShutdown.get()) {
          LOG.info(WAIT_QUEUE_SCHEDULER_THREAD_NAME_FORMAT
              + " thread has been interrupted after shutdown.");
        } else {
          LOG.warn(WAIT_QUEUE_SCHEDULER_THREAD_NAME_FORMAT + " interrupted without shutdown", e);
          throw new RuntimeException(e);
        }
      }
    }

    private void waitOnLock() throws InterruptedException {
      if (isShutdown.get()) return;
      nextSanityCheck = System.nanoTime() + SANITY_CHECK_TIMEOUT_MS * 1000000L;
      lock.wait(SANITY_CHECK_TIMEOUT_MS);
    }
  }

  private class WaitQueueWorkerCallback implements FutureCallback<Object> {

    @Override
    public void onSuccess(Object result) {
      if (isShutdown.get()) {
        LOG.info("Wait queue scheduler worker exited with success!");
      } else {
        LOG.error("Wait queue scheduler worker exited with success!");
        Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(),
            new IllegalStateException("WaitQueue worked exited before shutdown"));
      }
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("Wait queue scheduler worker exited with failure!", t);
      Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), t);
    }
  }

  @Override
  public SubmissionState schedule(TaskRunnerCallable task) {
    TaskWrapper taskWrapper = new TaskWrapper(task, this);
    SubmissionState result;
    TaskWrapper evictedTask;
    boolean canFinish;
    synchronized (lock) {
      // If the queue does not have capacity, it does not throw a Rejection. Instead it will
      // return the task with the lowest priority, which could be the task which is currently being processed.

      // TODO HIVE-11687 It's possible for a bunch of tasks to come in around the same time, without the
      // actual executor threads picking up any work. This will lead to unnecessary rejection of tasks.
      // The wait queue should be able to fit at least (waitQueue + currentFreeExecutor slots)
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "Offering to wait queue with: waitQueueSize={}, numSlotsAvailable={}, runningFragmentCount={} ",
            waitQueue.size(), numSlotsAvailable.get(),
            runningFragmentCount.get());
      }

      canFinish = taskWrapper.getTaskRunnerCallable().canFinish();
      taskWrapper.updateCanFinishForPriority(canFinish); // Update the property before offering.
      evictedTask = waitQueue.offer(taskWrapper, maxParallelExecutors - runningFragmentCount.get());
      // Finishable state is checked on the task, via an explicit query to the TaskRunnerCallable

      // null evicted task means offer accepted
      // evictedTask is not equal taskWrapper means current task is accepted and it evicted
      // some other task
      if (evictedTask == null || !evictedTask.equals(taskWrapper)) {
        knownTasks.put(taskWrapper.getRequestId(), taskWrapper);
        taskWrapper.setIsInWaitQueue(true);
        task.setWmCountersQueued();
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} added to wait queue. Current wait queue size={}", task.getRequestId(),
              waitQueue.size());
        }

        result = evictedTask == null ? SubmissionState.ACCEPTED : SubmissionState.EVICTED_OTHER;

        if (LOG.isDebugEnabled() && evictedTask != null) {
          LOG.debug("Eviction: {} {} {}", taskWrapper, result, evictedTask);
        }
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info(
              "wait queue full, size={}. numSlotsAvailable={}, runningFragmentCount={}. {} not added",
              waitQueue.size(), numSlotsAvailable.get(), runningFragmentCount.get(), task.getRequestId());
        }
        evictedTask.getTaskRunnerCallable().killTask();

        result = SubmissionState.REJECTED;

        if (LOG.isDebugEnabled()) {
          LOG.debug("{} is {} as wait queue is full", taskWrapper.getRequestId(), result);
        }
        if (metrics != null) {
          metrics.incrTotalRejectedRequests();
        }
        return result;
      }

      // Register for notifications inside the lock. Should avoid races with unregisterForNotifications
      // happens in a different Submission thread. i.e. Avoid register running for this task
      // after some other submission has evicted it.
      boolean stateChanged = !taskWrapper.maybeRegisterForFinishedStateNotifications(canFinish);
      if (stateChanged) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Finishable state of {} updated to {} during registration for state updates",
              taskWrapper.getRequestId(), !canFinish);
        }
        finishableStateUpdated(taskWrapper, !canFinish);
      }
    }

    // At this point, the task has been added into the queue. It may have caused an eviction for
    // some other task.

    // This registration has to be done after knownTasks has been populated.
    // Register for state change notifications so that the waitQueue can be re-ordered correctly
    // if the fragment moves in or out of the finishable state.

    if (LOG.isDebugEnabled()) {
      LOG.debug("Wait Queue: {}", waitQueue);
    }

    if (evictedTask != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("{} evicted from wait queue in favor of {} because of lower priority",
            evictedTask.getRequestId(), task.getRequestId());
      }
      try {
        knownTasks.remove(evictedTask.getRequestId());
        evictedTask.maybeUnregisterForFinishedStateNotifications();
        evictedTask.setIsInWaitQueue(false);
      } finally {
        // This is dealing with tasks from a different submission, and cause the kill
        // to go out before the previous submissions has completed. Handled in the AM
        evictedTask.getTaskRunnerCallable().killTask();
      }
      if (metrics != null) {
        metrics.incrTotalEvictedFromWaitQueue();
      }
    }
    synchronized (lock) {
      lock.notifyAll();
    }

    if (metrics != null) {
      metrics.setExecutorNumQueuedRequests(waitQueue.size());
    }
    return result;
  }

  @Override
  public boolean updateFragment(String fragmentId, boolean isGuaranteed) {
    synchronized (lock) {
      TaskWrapper taskWrapper = knownTasks.get(fragmentId);
      if (taskWrapper == null) {
        LOG.debug("Fragment not found {}", fragmentId);
        return false;
      }
      if (taskWrapper.isGuaranteed() == isGuaranteed) return true;

      LOG.debug("Fragment {} guaranteed state changed to {}; finishable {}, in wait queue {}, "
          + "in preemption queue {}", taskWrapper.getRequestId(), isGuaranteed,
          taskWrapper.canFinishForPriority(), taskWrapper.isInWaitQueue(),
          taskWrapper.isInPreemptionQueue());
      // Do the removal before we change the element, to avoid invalid queue ordering.
      if (isGuaranteed && taskWrapper.isInPreemptionQueue() && taskWrapper.canFinishForPriority()) {
        removeFromPreemptionQueue(taskWrapper);
      }
      if (taskWrapper.isInWaitQueue()) {
        // Re-order the wait queue. Note: we assume that noone will take our capacity based
        // on the fact that we are doing this under the epic lock. If the epic lock is removed,
        // we'd need to do the steps under the queue lock; we could pass in a f() to update state.
        boolean isRemoved = waitQueue.remove(taskWrapper);
        taskWrapper.updateIsGuaranteed(isGuaranteed);
        forceReinsertIntoQueue(taskWrapper, isRemoved);
      } else {
        taskWrapper.updateIsGuaranteed(isGuaranteed);
        if (!isGuaranteed && !taskWrapper.isInPreemptionQueue()) {
          // No need to check finishable here; if it was set it would already be in the queue.
          addToPreemptionQueue(taskWrapper);
        }
      }
      lock.notifyAll();
      return true;
    }
  }

  private void forceReinsertIntoQueue(TaskWrapper taskWrapper, boolean isRemoved) {
    if (!isRemoved) {
      LOG.warn("Failed to remove {} from waitQueue", taskWrapper.getTaskRunnerCallable());
    } else {
      waitQueue.forceOffer(taskWrapper);
    }
  }

  @Override
  public QueryIdentifier findQueryByFragment(String fragmentId) {
    synchronized (lock) {
      TaskWrapper taskWrapper = knownTasks.get(fragmentId);
      return taskWrapper == null ? null : taskWrapper.getTaskRunnerCallable()
          .getFragmentInfo().getQueryInfo().getQueryIdentifier();
    }
  }

  @Override
  public void killFragment(String fragmentId) {
    synchronized (lock) {
      TaskWrapper taskWrapper = knownTasks.remove(fragmentId);
      // Can be null since the task may have completed meanwhile.
      if (taskWrapper != null) {
        if (taskWrapper.isInWaitQueue()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Removing {} from waitQueue", fragmentId);
          }
          taskWrapper.setIsInWaitQueue(false);
          taskWrapper.getTaskRunnerCallable().setWmCountersDone();
          if (waitQueue.remove(taskWrapper)) {
            if (metrics != null) {
              metrics.setExecutorNumQueuedRequests(waitQueue.size());
            }
          }
        }
        if (taskWrapper.isInPreemptionQueue()) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Removing {} from preemptionQueue", fragmentId);
          }
          removeFromPreemptionQueue(taskWrapper);
        }
        taskWrapper.getTaskRunnerCallable().setWmCountersDone();
        // TODO: this will probably send a message to AM. Is that needed here?
        taskWrapper.getTaskRunnerCallable().killTask();
      } else {
        LOG.info("Ignoring killFragment request for {} since it isn't known", fragmentId);
      }
      lock.notifyAll();
    }
  }

  private static final class FragmentCompletion {

    public FragmentCompletion(
        State state, long completingTime) {
      this.state = state;
      this.completingTime = completingTime;
    }

    State state;
    long completingTime;
  }

  /** A map where fragments live between when the TezTaskRunner2 calls taskSucceeded/Failed, or
   *  an external caller calls killTask, and when the TaskRunnerCallable comes out of the
   *  threadpool.
   */
  @VisibleForTesting
  final ConcurrentMap<String, FragmentCompletion>
      completingFragmentMap = new ConcurrentHashMap<>();

  @Override
  public void fragmentCompleting(String fragmentId, State state) {
    int val = 0;
    do {
      // Tez internals may register the same task as completing multiple times.
      val = runningFragmentCount.get();
      if (val == 0) {
        LOG.warn("RunningFragmentCount is already 0. Multiple calls for the same completion.");
        return;
      }
    } while (!runningFragmentCount.compareAndSet(val, val - 1));

    completingFragmentMap.put(fragmentId, new FragmentCompletion(state, clock.getTime()));
  }

  @VisibleForTesting
  /** Assumes the epic lock is already taken. */
  void tryScheduleUnderLock(final TaskWrapper taskWrapper) throws RejectedExecutionException {
    if (LOG.isInfoEnabled()) {
      LOG.info("Attempting to execute {}", taskWrapper);
    }
    TaskRunnerCallable task = taskWrapper.getTaskRunnerCallable();
    task.setWmCountersRunning();
    ListenableFuture<TaskRunner2Result> future = executorService.submit(task);
    runningFragmentCount.incrementAndGet();
    taskWrapper.setIsInWaitQueue(false);

    FutureCallback<TaskRunner2Result> wrappedCallback = createInternalCompletionListener(
      taskWrapper);
    // Callback on a separate thread so that when a task completes, the thread in the main queue
    // is actually available for execution and will not potentially result in a RejectedExecution
    Futures.addCallback(future, wrappedCallback, executionCompletionExecutorService);

    boolean canFinish = taskWrapper.getTaskRunnerCallable().canFinish(),
        isGuaranteed = taskWrapper.isGuaranteed();
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} scheduled for execution. canFinish={}, isGuaranteed={}",
          taskWrapper.getRequestId(), canFinish, isGuaranteed);
    }

    // only tasks that cannot finish immediately are pre-emptable. In other words, if all inputs
    // to the tasks are not ready yet, the task is eligible for pre-emptable.
    if (enablePreemption) {
      if (!canFinish || !isGuaranteed) {
        if (LOG.isInfoEnabled()) {
          LOG.info("Adding {} to pre-emption queue", taskWrapper.getRequestId());
        }
        addToPreemptionQueue(taskWrapper);
      }
    }
    numSlotsAvailable.decrementAndGet();
    if (metrics != null) {
      metrics.setNumExecutorsAvailable(numSlotsAvailable.get());
    }
  }

  private boolean handleScheduleAttemptedRejection(TaskWrapper rejected) {
    // TODO: is this check even needed given what the caller checks?
    if (!enablePreemption || preemptionQueue.isEmpty()) {
      return false;
    }
    LOG.debug("Preemption Queue: {}", preemptionQueue);

    // This call checks under lock if we can actually preempt the task.
    // It is possible to have a race where the update (that's also under lock) makes the
    // task finishable or guaranteed between the remove and kill, but it's the same timing
    // issue as would happen is there was a tiny delay on the network, so we don't care.
    TaskWrapper victim = getSuitableVictimFromPreemptionQueue(rejected);
    if (victim == null) {
      return false; // Woe us.
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Invoking kill task for {} due to pre-emption to run {}",
          victim.getRequestId(), rejected.getRequestId());
    }
    // The task will either be killed or is already in the process of completing, which will
    // trigger the next scheduling run, or result in available slots being higher than 0,
    // which will cause the scheduler loop to continue.
    victim.getTaskRunnerCallable().killTask();
    // We've killed something and may want to wait for it to die.
    return true;
  }

  private static class SanityChecker implements Function<TaskWrapper, Boolean> {
    private TaskWrapper firstCannotFinish = null;
    private TaskWrapper firstProblematic = null;
    private final EvictingPriorityBlockingQueue<TaskWrapper> queue;

    public SanityChecker(EvictingPriorityBlockingQueue<TaskWrapper> queue) {
      this.queue = queue;
    }

    @Override
    public Boolean apply(TaskWrapper input) {
      if (input == null) return true;
      boolean canFinish = input.getTaskRunnerCallable().canFinishForPriority();
      if (firstCannotFinish == null && !canFinish) {
        firstCannotFinish = input;
        return true;
      }
      if (firstCannotFinish != null && canFinish) {
        firstProblematic = input;
        return false;
      }
      return true;
    }

    void run() {
      queue.apply(this);
      if (firstProblematic != null) {
        final StringBuilder sb = new StringBuilder(
            "Found finishable task behind non-finishable in the queue: ");
        sb.append(firstProblematic).append(" was after ").append(firstCannotFinish).append("; ");
        queue.apply(new Function<TaskExecutorService.TaskWrapper, Boolean>() {
          @Override
          public Boolean apply(TaskWrapper input) {
            sb.append(input).append(", ");
            return true;
          }
        });
        LOG.error(sb.toString());
      }
      firstCannotFinish = firstProblematic = null;
    }
  }

  private SanityChecker sanityCheckQueue(SanityChecker sc) {
    if (sc == null) {
      sc = new SanityChecker(waitQueue);
    }
    sc.run();
    return sc;
  }

  private void finishableStateUpdated(TaskWrapper taskWrapper, boolean newFinishableState) {
    synchronized (lock) {
      LOG.debug("Fragment {} guaranteed state changed to {}; finishable {}, in wait queue {}, "
          + "in preemption queue {}", taskWrapper.getRequestId(), taskWrapper.isGuaranteed(),
          newFinishableState, taskWrapper.isInWaitQueue(), taskWrapper.isInPreemptionQueue());
      // Do the removal before we change the element, to avoid invalid queue ordering.
      if (newFinishableState && taskWrapper.isInPreemptionQueue() && taskWrapper.isGuaranteed()) {
        removeFromPreemptionQueue(taskWrapper);
      }
      if (taskWrapper.isInWaitQueue()) {
        // Re-order the wait queue. Note: we assume that noone will take our capacity based
        // on the fact that we are doing this under the epic lock. If the epic lock is removed,
        // we'd need to do the steps under the queue lock; we could pass in a f() to update state.
        boolean isRemoved = waitQueue.remove(taskWrapper);
        taskWrapper.updateCanFinishForPriority(newFinishableState);
        forceReinsertIntoQueue(taskWrapper, isRemoved);
      } else {
        taskWrapper.updateCanFinishForPriority(newFinishableState);
        if (!newFinishableState && !taskWrapper.isInPreemptionQueue()) {
          // No need to check guaranteed here; if it was false we would already be in the queue.
          addToPreemptionQueue(taskWrapper);
        }
      }

      lock.notifyAll();
    }
  }

  private void addToPreemptionQueue(TaskWrapper taskWrapper) {
    synchronized (lock) {
      insertIntoPreemptionQueueOrFailUnlocked(taskWrapper);
      taskWrapper.setIsInPreemptableQueue(true);
      if (metrics != null) {
        metrics.setExecutorNumPreemptableRequests(preemptionQueue.size());
      }
    }
  }

  private void insertIntoPreemptionQueueOrFailUnlocked(TaskWrapper taskWrapper) {
    boolean added = preemptionQueue.offer(taskWrapper);
    if (!added) {
      LOG.warn("Failed to add element {} to preemption queue. Terminating", taskWrapper);
      Thread.getDefaultUncaughtExceptionHandler().uncaughtException(Thread.currentThread(),
          new IllegalStateException("Preemption queue full. Cannot proceed"));
    }
  }

  /**
   * Remove the specified taskWrapper from the preemption queue
   * @param taskWrapper the taskWrapper to be removed
   * @return true if the element existed in the queue and wasa removed, false otherwise
   */
  private boolean removeFromPreemptionQueue(TaskWrapper taskWrapper) {
    synchronized (lock) {
      return removeFromPreemptionQueueUnlocked(taskWrapper);
    }
  }

  private boolean removeFromPreemptionQueueUnlocked(
      TaskWrapper taskWrapper) {
    boolean removed = preemptionQueue.remove(taskWrapper);
    taskWrapper.setIsInPreemptableQueue(false);
    if (metrics != null) {
      metrics.setExecutorNumPreemptableRequests(preemptionQueue.size());
    }
    return removed;
  }

  private TaskWrapper getSuitableVictimFromPreemptionQueue(TaskWrapper candidate) {
    TaskWrapper taskWrapper;
    synchronized (lock) {
      taskWrapper = preemptionQueue.poll();
      // Note that the code updating the state of the task does it when it's out of the queue.
      // So, the priorities in the queue should be correct; if the top task is not killable then
      // no task in the queue would be killable.
      if (taskWrapper == null) return null;
      if (!canPreempt(candidate, taskWrapper)) {
        // The "most preemptable" task is still too important for us to kill. Put it back.
        insertIntoPreemptionQueueOrFailUnlocked(taskWrapper);
        return null;
      }
      taskWrapper.setIsInPreemptableQueue(false);
      if (metrics != null) {
        metrics.setExecutorNumPreemptableRequests(preemptionQueue.size());
      }
      return taskWrapper;
    }
  }

  private static boolean canPreempt(TaskWrapper candidate, TaskWrapper victim) {
    if (victim == null) return false;
    if (candidate.isGuaranteed() && !victim.isGuaranteed()) return true;
    return ((candidate.isGuaranteed() == victim.isGuaranteed())
        && candidate.canFinishForPriority() && !victim.canFinishForPriority());
  }

  @VisibleForTesting
  InternalCompletionListener createInternalCompletionListener(TaskWrapper taskWrapper) {
    return new InternalCompletionListener(taskWrapper);
  }

  @VisibleForTesting
  class InternalCompletionListener implements
      FutureCallback<TaskRunner2Result> {
    private final TaskWrapper taskWrapper;

    public InternalCompletionListener(TaskWrapper taskWrapper) {
      this.taskWrapper = taskWrapper;
    }

    // By the time either success / failed are called, the task itself knows that it has terminated,
    // and will ignore subsequent kill requests if they go out.

    // There's a race between removing the current task from the preemption queue and the actual scheduler
    // attempting to take an element from the preemption queue to make space for another task.
    // If the current element is removed to make space - that is OK, since the current task is completing and
    // will end up making space for execution. Any kill message sent out by the scheduler to the task will
    // be ignored, since the task knows it has completed (otherwise it would not be in this callback).
    //
    // If the task is removed from the queue as a result of this callback, and the scheduler happens to
    // be in the section where it's looking for a preemptible task - the scheuler may end up pulling the
    // next pre-emptible task and killing it (an extra preemption).
    // TODO: This potential extra preemption can be avoided by synchronizing the entire tryScheduling block.\
    // This would essentially synchronize all operations - it would be better to see if there's an
    // approach where multiple locks could be used to avoid single threaded operation.
    // - It checks available and preempts (which could be this task)
    // - Or this task completes making space, and removing the need for preemption

    @Override
    public void onSuccess(TaskRunner2Result result) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received successful completion for: {}",
            taskWrapper.getRequestId());
      }
      updateFallOffStats(taskWrapper.getRequestId());
      knownTasks.remove(taskWrapper.getRequestId());
      taskWrapper.setIsInPreemptableQueue(false);
      taskWrapper.maybeUnregisterForFinishedStateNotifications();
      taskWrapper.getTaskRunnerCallable().setWmCountersDone();
      updatePreemptionListAndNotify(result.getEndReason());
      taskWrapper.getTaskRunnerCallable().getCallback().onSuccess(result);
    }

    @Override
    public void onFailure(Throwable t) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Received failed completion for: {}",
            taskWrapper.getRequestId());
      }
      updateFallOffStats(taskWrapper.getRequestId());
      knownTasks.remove(taskWrapper.getRequestId());
      taskWrapper.setIsInPreemptableQueue(false);
      taskWrapper.maybeUnregisterForFinishedStateNotifications();
      taskWrapper.getTaskRunnerCallable().setWmCountersDone();
      updatePreemptionListAndNotify(null);
      taskWrapper.getTaskRunnerCallable().getCallback().onFailure(t);
      LOG.error("Failed notification received: Stacktrace: " + ExceptionUtils.getStackTrace(t));
    }

    private void updatePreemptionListAndNotify(EndReason reason) {
      // if this task was added to pre-emption list, remove it
      if (enablePreemption) {
        String state = reason == null ? "FAILED" : reason.name();
        boolean removed = removeFromPreemptionQueueUnlocked(taskWrapper);
        if (removed && LOG.isInfoEnabled()) {
          TaskRunnerCallable trc = taskWrapper.getTaskRunnerCallable();
          LOG.info(TaskRunnerCallable.getTaskIdentifierString(trc.getRequest(),
              trc.getVertexSpec(), trc.getQueryId()) + " request " + state + "! Removed from preemption list.");
        }
      }

      numSlotsAvailable.incrementAndGet();
      if (metrics != null) {
        metrics.setNumExecutorsAvailable(numSlotsAvailable.get());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Task {} complete. WaitQueueSize={}, numSlotsAvailable={}, preemptionQueueSize={}",
          taskWrapper.getRequestId(), waitQueue.size(), numSlotsAvailable.get(),
          preemptionQueue.size());
      }
      synchronized (lock) {
        if (!waitQueue.isEmpty()) {
          lock.notifyAll();
        }
      }
    }

    private void updateFallOffStats(
        String requestId) {
      long now = clock.getTime();
      FragmentCompletion fragmentCompletion =
          completingFragmentMap.remove(requestId);
      if (fragmentCompletion == null) {
        LOG.warn(
            "Received onSuccess/onFailure for a fragment for which a completing message was not received: {}",
            requestId);
        // Happens due to AM side pre-emption, or the AM asking for a task to die.
        // There's no hooks at the moment to get information over.
        // For now - decrement the count to avoid accounting errors.
        runningFragmentCount.decrementAndGet();
        // TODO: Extend TaskRunner2 or see if an API with callbacks will work
      } else {
        long timeTaken = now - fragmentCompletion.completingTime;
        switch (fragmentCompletion.state) {
          case SUCCESS:
            if (metrics != null) {
              metrics.addMetricsFallOffSuccessTimeLost(timeTaken);
            }
            break;
          case FAILED:
            if (metrics != null) {
              metrics.addMetricsFallOffFailedTimeLost(timeTaken);
            }
            break;
          case KILLED:
            if (metrics != null) {
              metrics.addMetricsFallOffKilledTimeLost(timeTaken);
            }
            break;
        }
      }
    }

  }

  public void shutDown(boolean awaitTermination) {
    if (!isShutdown.getAndSet(true)) {
      if (awaitTermination) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("awaitTermination: " + awaitTermination + " shutting down task executor" +
              " service gracefully");
        }
        shutdownExecutor(waitQueueExecutorService);
        shutdownExecutor(executorService);
        shutdownExecutor(executionCompletionExecutorService);
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("awaitTermination: " + awaitTermination + " shutting down task executor" +
              " service immediately");
        }
        executorService.shutdownNow();
        waitQueueExecutorService.shutdownNow();
        executionCompletionExecutorService.shutdownNow();
      }
    }
  }

  private void shutdownExecutor(ExecutorService executorService) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
    }
  }



  @VisibleForTesting
  public static class PreemptionQueueComparator implements Comparator<TaskWrapper> {

    @Override
    public int compare(TaskWrapper t1, TaskWrapper t2) {
      TaskRunnerCallable o1 = t1.getTaskRunnerCallable();
      TaskRunnerCallable o2 = t2.getTaskRunnerCallable();
      FragmentRuntimeInfo fri1 = o1.getFragmentRuntimeInfo();
      FragmentRuntimeInfo fri2 = o2.getFragmentRuntimeInfo();

      // Regardless of other criteria, ducks are always more important than non-ducks.
      boolean v1 = o1.isGuaranteed(), v2 = o2.isGuaranteed();
      if (v1 != v2) return v1 ? 1 : -1;

      // Then, non-finishable must always precede finishable.
      v1 = o1.canFinishForPriority();
      v2 = o2.canFinishForPriority();
      if (v1 != v2) return v1 ? 1 : -1;

      // Otherwise, heuristics.
      if (fri1.getNumSelfAndUpstreamTasks() > fri2.getNumSelfAndUpstreamTasks()) {
        return 1;
      } else if (fri1.getNumSelfAndUpstreamTasks() < fri2.getNumSelfAndUpstreamTasks()) {
        return -1;
      }
      return 0;
    }
  }


  public static class TaskWrapper implements FinishableStateUpdateHandler {
    private final TaskRunnerCallable taskRunnerCallable;
    private final AtomicBoolean inWaitQueue = new AtomicBoolean(false);
    private final AtomicBoolean inPreemptionQueue = new AtomicBoolean(false);
    private final AtomicBoolean registeredForNotifications = new AtomicBoolean(false);
    private final TaskExecutorService taskExecutorService;

    public TaskWrapper(TaskRunnerCallable taskRunnerCallable, TaskExecutorService taskExecutorService) {
      this.taskRunnerCallable = taskRunnerCallable;
      this.taskExecutorService = taskExecutorService;
    }

    public void updateIsGuaranteed(boolean isGuaranteed) {
      taskRunnerCallable.setIsGuaranteed(isGuaranteed);
    }

    public boolean canFinishForPriority() {
      return taskRunnerCallable.canFinishForPriority();
    }

    public boolean isGuaranteed() {
      return taskRunnerCallable.isGuaranteed();
    }

    public void updateCanFinishForPriority(boolean newFinishableState) {
      taskRunnerCallable.updateCanFinishForPriority(newFinishableState);
    }

    // Don't invoke from within a scheduler lock


    /**
     *
     * @param currentFinishableState
     * @return true if the state has not changed from currentFinishableState, false otherwise
     */
    public boolean maybeRegisterForFinishedStateNotifications(
        boolean currentFinishableState) {
      if (!registeredForNotifications.getAndSet(true)) {
        return taskRunnerCallable.getFragmentInfo()
            .registerForFinishableStateUpdates(this, currentFinishableState);
      } else {
        // State has not changed / already registered for notifications.
        return true;
      }
    }

    // Don't invoke from within a scheduler lock
    public void maybeUnregisterForFinishedStateNotifications() {
      if (registeredForNotifications.getAndSet(false)) {
        taskRunnerCallable.getFragmentInfo().unregisterForFinishableStateUpdates(this);
      }
    }

    public TaskRunnerCallable getTaskRunnerCallable() {
      return taskRunnerCallable;
    }

    public boolean isInWaitQueue() {
      return inWaitQueue.get();
    }

    public boolean isInPreemptionQueue() {
      return inPreemptionQueue.get();
    }

    public void setIsInWaitQueue(boolean value) {
      this.inWaitQueue.set(value);
    }

    public void setIsInPreemptableQueue(boolean value) {
      this.inPreemptionQueue.set(value);
    }

    public String getRequestId() {
      return taskRunnerCallable.getRequestId();
    }

    @Override
    public String toString() {
      return "TaskWrapper{" +
          "task=" + taskRunnerCallable.getRequestId() +
          ", inWaitQueue=" + inWaitQueue.get() +
          ", inPreemptionQueue=" + inPreemptionQueue.get() +
          ", registeredForNotifications=" + registeredForNotifications.get() +
          ", canFinish=" + taskRunnerCallable.canFinish() +
          ", canFinish(in queue)=" + canFinishForPriority() +
          ", isGuaranteed=" + isGuaranteed() +
          ", firstAttemptStartTime=" + taskRunnerCallable.getFragmentRuntimeInfo().getFirstAttemptStartTime() +
          ", dagStartTime=" + taskRunnerCallable.getFragmentRuntimeInfo().getDagStartTime() +
          ", withinDagPriority=" + taskRunnerCallable.getFragmentRuntimeInfo().getWithinDagPriority() +
          ", vertexParallelism= " + taskRunnerCallable.getVertexSpec().getVertexParallelism() +
          ", selfAndUpstreamParallelism= " + taskRunnerCallable.getFragmentRuntimeInfo().getNumSelfAndUpstreamTasks() +
          ", selfAndUpstreamComplete= " + taskRunnerCallable.getFragmentRuntimeInfo().getNumSelfAndUpstreamCompletedTasks() +
          '}';
    }

    // No task lock. But acquires lock on the scheduler
    @Override
    public void finishableStateUpdated(boolean finishableState) {
      // This method should not by synchronized. Can lead to deadlocks since it calls a sync method.
      // Meanwhile the scheduler could try updating states via a synchronized method.
      LOG.info("Received finishable state update for {}, state={}",
          taskRunnerCallable.getRequestId(), finishableState);
      taskExecutorService.finishableStateUpdated(this, finishableState);
    }


    // TaskWrapper is used in structures, as well as for ordering using Comparators
    // in the waitQueue. Avoid Object comparison.
    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TaskWrapper that = (TaskWrapper) o;

      return taskRunnerCallable.getRequestId()
          .equals(that.taskRunnerCallable.getRequestId());
    }

    @Override
    public int hashCode() {
      return taskRunnerCallable.getRequestId().hashCode();
    }
  }

  private static class ExecutorThreadFactory implements ThreadFactory {
    private final ClassLoader classLoader;
    private final ThreadFactory defaultFactory;
    private final AtomicLong count = new AtomicLong(0);

    public ExecutorThreadFactory(ClassLoader classLoader) {
      this.classLoader = classLoader;
      this.defaultFactory = Executors.defaultThreadFactory();
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread thread = defaultFactory.newThread(r);
      thread.setName(String.format(TASK_EXECUTOR_THREAD_NAME_FORMAT, count.getAndIncrement()));
      thread.setDaemon(true);
      thread.setContextClassLoader(classLoader);
      return thread;
    }
  }
}
