/**
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
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.llap.daemon.FinishableStateUpdateHandler;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentRuntimeInfo;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentSpecProto;
import org.apache.hadoop.service.AbstractService;
import org.apache.tez.runtime.task.EndReason;
import org.apache.tez.runtime.task.TaskRunner2Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
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
public class TaskExecutorService extends AbstractService implements Scheduler<TaskRunnerCallable> {


  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorService.class);
  private static final boolean isInfoEnabled = LOG.isInfoEnabled();
  private static final boolean isDebugEnabled = LOG.isDebugEnabled();
  private static final String TASK_EXECUTOR_THREAD_NAME_FORMAT = "Task-Executor-%d";
  private static final String WAIT_QUEUE_SCHEDULER_THREAD_NAME_FORMAT = "Wait-Queue-Scheduler-%d";

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);

  // Thread pool for actual execution of work.
  private final ListeningExecutorService executorService;
  private final EvictingPriorityBlockingQueue<TaskWrapper> waitQueue;
  // Thread pool for taking entities off the wait queue.
  private final ListeningExecutorService waitQueueExecutorService;
  // Thread pool for callbacks on completion of execution of a work unit.
  private final ListeningExecutorService executionCompletionExecutorService;
  private final BlockingQueue<TaskWrapper> preemptionQueue;
  private final boolean enablePreemption;
  private final ThreadPoolExecutor threadPoolExecutor;
  private final AtomicInteger numSlotsAvailable;


  @VisibleForTesting
  // Tracks known tasks.
  final ConcurrentMap<String, TaskWrapper> knownTasks = new ConcurrentHashMap<>();

  private final Object lock = new Object();

  public TaskExecutorService(int numExecutors, int waitQueueSize, String waitQueueComparatorClassName,
      boolean enablePreemption) {
    super(TaskExecutorService.class.getSimpleName());
    LOG.info("TaskExecutorService is being setup with parameters: "
        + "numExecutors=" + numExecutors
        + ", waitQueueSize=" + waitQueueSize
        + ", waitQueueComparatorClassName=" + waitQueueComparatorClassName
        + ", enablePreemption=" + enablePreemption);

    final Comparator<TaskWrapper> waitQueueComparator;
    try {
      Class<? extends Comparator> waitQueueComparatorClazz =
          (Class<? extends Comparator>) Class.forName(
              waitQueueComparatorClassName);
      Constructor<? extends Comparator> ctor = waitQueueComparatorClazz.getConstructor(null);
      waitQueueComparator = ctor.newInstance(null);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          "Failed to load wait queue comparator, class=" + waitQueueComparatorClassName, e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException("Failed to find constructor for wait queue comparator, class=" +
          waitQueueComparatorClassName, e);
    } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(
          "Failed to find instantiate wait queue comparator, class=" + waitQueueComparatorClassName,
          e);
    }
    this.waitQueue = new EvictingPriorityBlockingQueue<>(waitQueueComparator, waitQueueSize);
    this.threadPoolExecutor = new ThreadPoolExecutor(numExecutors, // core pool size
        numExecutors, // max pool size
        1, TimeUnit.MINUTES,
        new SynchronousQueue<Runnable>(), // direct hand-off
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat(TASK_EXECUTOR_THREAD_NAME_FORMAT)
            .build());
    this.executorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
    this.preemptionQueue = new PriorityBlockingQueue<>(numExecutors,
        new PreemptionQueueComparator());
    this.enablePreemption = enablePreemption;
    this.numSlotsAvailable = new AtomicInteger(numExecutors);

    // single threaded scheduler for tasks from wait queue to executor threads
    ExecutorService wes = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat(WAIT_QUEUE_SCHEDULER_THREAD_NAME_FORMAT).build());
    this.waitQueueExecutorService = MoreExecutors.listeningDecorator(wes);

    ExecutorService executionCompletionExecutorServiceRaw = Executors.newFixedThreadPool(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ExecutionCompletionThread #%d")
            .build());
    executionCompletionExecutorService = MoreExecutors.listeningDecorator(executionCompletionExecutorServiceRaw);
    ListenableFuture<?> future = waitQueueExecutorService.submit(new WaitQueueWorker());
    Futures.addCallback(future, new WaitQueueWorkerCallback());


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
  public Set<String> getExecutorsStatus() {
    Set<String> result = new HashSet<>();
    StringBuilder value = new StringBuilder();
    for (Map.Entry<String, TaskWrapper> e : knownTasks.entrySet()) {
      value.setLength(0);
      value.append(e.getKey());
      TaskWrapper task = e.getValue();
      boolean isFirst = true;
      TaskRunnerCallable c = task.getTaskRunnerCallable();
      if (c != null && c.getRequest() != null && c.getRequest().getFragmentSpec() != null) {
        FragmentSpecProto fs = c.getRequest().getFragmentSpec();
        value.append(isFirst ? " (" : ", ").append(fs.getDagName())
          .append("/").append(fs.getVertexName());
        isFirst = false;
      }
      value.append(isFirst ? " (" : ", ");
      if (task.isInWaitQueue()) {
        value.append("in queue");
      } else if (c != null) {
        long startTime = c.getStartTime();
        if (startTime != 0) {
          value.append("started at ").append(sdf.get().format(new Date(startTime)));
        } else {
          value.append("not started");
        }
      } else {
        value.append("has no callable");
      }
      if (task.isInPreemptionQueue()) {
        value.append(", ").append("preemptable");
      }
      value.append(")");
      result.add(value.toString());
    }
    return result;
  }

  /**
   * Worker that takes tasks from wait queue and schedule it for execution.
   */
  private final class WaitQueueWorker implements Runnable {
    TaskWrapper task;

    @Override
    public void run() {

      try {


        while (!isShutdown.get()) {
          RejectedExecutionException rejectedException = null;
          synchronized (lock) {
            // Since schedule() can be called from multiple threads, we peek the wait queue,
            // try scheduling the task and then remove the task if scheduling is successful.
            // This will make sure the task's place in the wait queue is held until it gets scheduled.
            task = waitQueue.peek();
            if (task == null) {
              if (!isShutdown.get()) {
                lock.wait();
              }
              continue;
            }
            // if the task cannot finish and if no slots are available then don't schedule it.
            boolean shouldWait = false;
            if (task.getTaskRunnerCallable().canFinish()) {
              if (isDebugEnabled) {
                LOG.debug(
                    "Attempting to schedule task {}, canFinish={}. Current state: preemptionQueueSize={}, numSlotsAvailable={}, waitQueueSize={}",
                    task.getRequestId(), task.getTaskRunnerCallable().canFinish(),
                    preemptionQueue.size(), numSlotsAvailable.get(), waitQueue.size());
              }
              if (numSlotsAvailable.get() == 0 && preemptionQueue.isEmpty()) {
                shouldWait = true;
              }
            } else {
              if (numSlotsAvailable.get() == 0) {
                shouldWait = true;
              }
            }
            if (shouldWait) {
              if (!isShutdown.get()) {
                lock.wait();
              }
              // Another task at a higher priority may have come in during the wait. Lookup the
              // queue again to pick up the task at the highest priority.
              continue;
            }
            try {
              trySchedule(task);
              // wait queue could have been re-ordered in the mean time because of concurrent task
              // submission. So remove the specific task instead of the head task.
              waitQueue.remove(task);
            } catch (RejectedExecutionException e) {
              rejectedException = e;
            }
          }

          // Handle the rejection outside of the lock
          if (rejectedException !=null) {
            handleScheduleAttemptedRejection(task);
          }

          synchronized (lock) {
            while (waitQueue.isEmpty()) {
              if (!isShutdown.get()) {
                lock.wait();
              }
            }
          }
        }

      } catch (InterruptedException e) {
        if (isShutdown.get()) {
          LOG.info(WAIT_QUEUE_SCHEDULER_THREAD_NAME_FORMAT + " thread has been interrupted after shutdown.");
        } else {
          LOG.warn(WAIT_QUEUE_SCHEDULER_THREAD_NAME_FORMAT + " interrupted without shutdown", e);
          throw new RuntimeException(e);
        }
      }
    }
  }

  private class WaitQueueWorkerCallback implements FutureCallback {

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
    synchronized (lock) {
      // If the queue does not have capacity, it does not throw a Rejection. Instead it will
      // return the task with the lowest priority, which could be the task which is currently being processed.

      // TODO HIVE-11687 It's possible for a bunch of tasks to come in around the same time, without the
      // actual executor threads picking up any work. This will lead to unnecessary rejection of tasks.
      // The wait queue should be able to fit at least (waitQueue + currentFreeExecutor slots)
      evictedTask = waitQueue.offer(taskWrapper);

      // null evicted task means offer accepted
      // evictedTask is not equal taskWrapper means current task is accepted and it evicted
      // some other task
      if (evictedTask == null || evictedTask != taskWrapper) {
        knownTasks.put(taskWrapper.getRequestId(), taskWrapper);
        taskWrapper.setIsInWaitQueue(true);
        if (isDebugEnabled) {
          LOG.debug("{} added to wait queue. Current wait queue size={}", task.getRequestId(),
              waitQueue.size());
        }

        result = evictedTask == null ? SubmissionState.ACCEPTED : SubmissionState.EVICTED_OTHER;

        if (isDebugEnabled && evictedTask != null) {
          LOG.debug("Eviction: {} {} {}", taskWrapper, result, evictedTask);
        }
      } else {
        if (isInfoEnabled) {
          LOG.info("wait queue full, size={}. {} not added", waitQueue.size(), task.getRequestId());
        }
        evictedTask.getTaskRunnerCallable().killTask();

        result = SubmissionState.REJECTED;

        if (isDebugEnabled) {
          LOG.debug("{} is {} as wait queue is full", taskWrapper.getRequestId(), result);
        }
        return result;
      }
    }

    // At this point, the task has been added into the queue. It may have caused an eviction for
    // some other task.

    // This registration has to be done after knownTasks has been populated.
    // Register for state change notifications so that the waitQueue can be re-ordered correctly
    // if the fragment moves in or out of the finishable state.
    boolean canFinish = taskWrapper.getTaskRunnerCallable().canFinish();
    // It's safe to register outside of the lock since the stateChangeTracker ensures that updates
    // and registrations are mutually exclusive.
    taskWrapper.maybeRegisterForFinishedStateNotifications(canFinish);

    if (isDebugEnabled) {
      LOG.debug("Wait Queue: {}", waitQueue);
    }
    if (evictedTask != null) {
      knownTasks.remove(evictedTask.getRequestId());
      evictedTask.maybeUnregisterForFinishedStateNotifications();
      evictedTask.setIsInWaitQueue(false);
      evictedTask.getTaskRunnerCallable().killTask();
      if (isInfoEnabled) {
        LOG.info("{} evicted from wait queue in favor of {} because of lower priority",
            evictedTask.getRequestId(), task.getRequestId());
      }
    }
    synchronized (lock) {
      lock.notify();
    }

    return result;
  }

  @Override
  public void killFragment(String fragmentId) {
    synchronized (lock) {
      TaskWrapper taskWrapper = knownTasks.remove(fragmentId);
      // Can be null since the task may have completed meanwhile.
      if (taskWrapper != null) {
        if (taskWrapper.inWaitQueue) {
          if (isDebugEnabled) {
            LOG.debug("Removing {} from waitQueue", fragmentId);
          }
          taskWrapper.setIsInWaitQueue(false);
          waitQueue.remove(taskWrapper);
        }
        if (taskWrapper.inPreemptionQueue) {
          if (isDebugEnabled) {
            LOG.debug("Removing {} from preemptionQueue", fragmentId);
          }
          taskWrapper.setIsInPreemptableQueue(false);
          preemptionQueue.remove(taskWrapper);
        }
        taskWrapper.getTaskRunnerCallable().killTask();
      } else {
        LOG.info("Ignoring killFragment request for {} since it isn't known", fragmentId);
      }
      lock.notify();
    }
  }

  private void trySchedule(final TaskWrapper taskWrapper) throws RejectedExecutionException {

      synchronized (lock) {
        boolean canFinish = taskWrapper.getTaskRunnerCallable().canFinish();
        LOG.info("Attempting to execute {}", taskWrapper);
        ListenableFuture<TaskRunner2Result> future = executorService.submit(taskWrapper.getTaskRunnerCallable());
        taskWrapper.setIsInWaitQueue(false);
        FutureCallback<TaskRunner2Result> wrappedCallback = createInternalCompletionListener(taskWrapper);
        // Callback on a separate thread so that when a task completes, the thread in the main queue
        // is actually available for execution and will not potentially result in a RejectedExecution
        Futures.addCallback(future, wrappedCallback, executionCompletionExecutorService);

        if (isDebugEnabled) {
          LOG.debug("{} scheduled for execution. canFinish={}",
              taskWrapper.getRequestId(), canFinish);
        }

        // only tasks that cannot finish immediately are pre-emptable. In other words, if all inputs
        // to the tasks are not ready yet, the task is eligible for pre-emptable.
        if (enablePreemption) {
          if (!canFinish) {
            if (isInfoEnabled) {
              LOG.info("{} is not finishable. Adding it to pre-emption queue",
                  taskWrapper.getRequestId());
            }
            addToPreemptionQueue(taskWrapper);
          }
        }
      }
      numSlotsAvailable.decrementAndGet();
  }

  private void handleScheduleAttemptedRejection(TaskWrapper taskWrapper) {
    if (enablePreemption && taskWrapper.getTaskRunnerCallable().canFinish() && !preemptionQueue.isEmpty()) {

      if (isDebugEnabled) {
        LOG.debug("Preemption Queue: " + preemptionQueue);
      }

      TaskWrapper pRequest = removeAndGetFromPreemptionQueue();

      // Avoid preempting tasks which are finishable - callback still to be processed.
      if (pRequest != null) {
        if (pRequest.getTaskRunnerCallable().canFinish()) {
          LOG.info(
              "Removed {} from preemption queue, but not preempting since it's now finishable",
              pRequest.getRequestId());
        } else {
          if (isInfoEnabled) {
            LOG.info("Invoking kill task for {} due to pre-emption to run {}",
                pRequest.getRequestId(), taskWrapper.getRequestId());
          }
          // The task will either be killed or is already in the process of completing, which will
          // trigger the next scheduling run, or result in available slots being higher than 0,
          // which will cause the scheduler loop to continue.
          pRequest.getTaskRunnerCallable().killTask();
        }
      }
    }
  }

  private void finishableStateUpdated(TaskWrapper taskWrapper, boolean newFinishableState) {
    synchronized (lock) {
      if (taskWrapper.isInWaitQueue()) {
        // Re-order the wait queue
        LOG.debug("Re-ordering the wait queue since {} finishable state moved to {}",
            taskWrapper.getRequestId(), newFinishableState);
        if (waitQueue.remove(taskWrapper)) {
          // Put element back only if it existed.
          waitQueue.offer(taskWrapper);
        } else {
          LOG.warn("Failed to remove {} from waitQueue",
              taskWrapper.getTaskRunnerCallable().getRequestId());
        }
      }

      if (newFinishableState == true && taskWrapper.isInPreemptionQueue()) {
        LOG.debug("Removing {} from preemption queue because it's state changed to {}",
            taskWrapper.getRequestId(), newFinishableState);
        preemptionQueue.remove(taskWrapper.getTaskRunnerCallable());
      } else if (newFinishableState == false && !taskWrapper.isInPreemptionQueue() &&
          !taskWrapper.isInWaitQueue()) {
        LOG.debug("Adding {} to preemption queue since finishable state changed to {}",
            taskWrapper.getRequestId(), newFinishableState);
        preemptionQueue.offer(taskWrapper);
      }
      lock.notify();
    }
  }

  private void addToPreemptionQueue(TaskWrapper taskWrapper) {
    synchronized (lock) {
      preemptionQueue.add(taskWrapper);
      taskWrapper.setIsInPreemptableQueue(true);
    }
  }

  private TaskWrapper removeAndGetFromPreemptionQueue() {
    TaskWrapper taskWrapper;
    synchronized (lock) {
       taskWrapper = preemptionQueue.remove();
      if (taskWrapper != null) {
        taskWrapper.setIsInPreemptableQueue(false);
      }
    }
    return taskWrapper;
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

    @Override
    public void onSuccess(TaskRunner2Result result) {
      knownTasks.remove(taskWrapper.getRequestId());
      taskWrapper.setIsInPreemptableQueue(false);
      taskWrapper.maybeUnregisterForFinishedStateNotifications();
      taskWrapper.getTaskRunnerCallable().getCallback().onSuccess(result);
      updatePreemptionListAndNotify(result.getEndReason());
    }

    @Override
    public void onFailure(Throwable t) {
      knownTasks.remove(taskWrapper.getRequestId());
      taskWrapper.setIsInPreemptableQueue(false);
      taskWrapper.maybeUnregisterForFinishedStateNotifications();
      taskWrapper.getTaskRunnerCallable().getCallback().onFailure(t);
      updatePreemptionListAndNotify(null);
      LOG.error("Failed notification received: Stacktrace: " + ExceptionUtils.getStackTrace(t));
    }

    private void updatePreemptionListAndNotify(EndReason reason) {
      // if this task was added to pre-emption list, remove it
      if (enablePreemption) {
        String state = reason == null ? "FAILED" : reason.name();
        boolean removed = preemptionQueue.remove(taskWrapper);
        if (removed && isInfoEnabled) {
          LOG.info(TaskRunnerCallable
              .getTaskIdentifierString(taskWrapper.getTaskRunnerCallable().getRequest())
              + " request " + state + "! Removed from preemption list.");
        }
      }

      numSlotsAvailable.incrementAndGet();
      if (isDebugEnabled) {
        LOG.debug("Task {} complete. WaitQueueSize={}, numSlotsAvailable={}, preemptionQueueSize={}",
          taskWrapper.getRequestId(), waitQueue.size(), numSlotsAvailable.get(),
          preemptionQueue.size());
      }
      synchronized (lock) {
        if (!waitQueue.isEmpty()) {
          lock.notify();
        }
      }
    }

  }

  public void shutDown(boolean awaitTermination) {
    if (!isShutdown.getAndSet(true)) {
      if (awaitTermination) {
        if (isDebugEnabled) {
          LOG.debug("awaitTermination: " + awaitTermination + " shutting down task executor" +
              " service gracefully");
        }
        shutdownExecutor(waitQueueExecutorService);
        shutdownExecutor(executorService);
        shutdownExecutor(executionCompletionExecutorService);
      } else {
        if (isDebugEnabled) {
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
    private boolean inWaitQueue = false;
    private boolean inPreemptionQueue = false;
    private boolean registeredForNotifications = false;
    private final TaskExecutorService taskExecutorService;

    public TaskWrapper(TaskRunnerCallable taskRunnerCallable, TaskExecutorService taskExecutorService) {
      this.taskRunnerCallable = taskRunnerCallable;
      this.taskExecutorService = taskExecutorService;
    }

    // Methods are synchronized primarily for visibility.
    /**
     *
     * @param currentFinishableState
     * @return true if the current state is the same as the currentFinishableState. false if the state has already changed.
     */
    // Synchronized to avoid register / unregister clobbering each other.
    // Don't invoke from within a scheduler lock
    public synchronized boolean maybeRegisterForFinishedStateNotifications(
        boolean currentFinishableState) {
      if (!registeredForNotifications) {
        registeredForNotifications = true;
        return taskRunnerCallable.getFragmentInfo()
            .registerForFinishableStateUpdates(this, currentFinishableState);
      } else {
        return true;
      }
    }

    // Synchronized to avoid register / unregister clobbering each other.
    // Don't invoke from within a scheduler lock
    public synchronized void maybeUnregisterForFinishedStateNotifications() {
      if (registeredForNotifications) {
        registeredForNotifications = false;
        taskRunnerCallable.getFragmentInfo().unregisterForFinishableStateUpdates(this);
      }
    }

    public TaskRunnerCallable getTaskRunnerCallable() {
      return taskRunnerCallable;
    }

    public synchronized boolean isInWaitQueue() {
      return inWaitQueue;
    }

    public synchronized boolean isInPreemptionQueue() {
      return inPreemptionQueue;
    }

    public synchronized void setIsInWaitQueue(boolean value) {
      this.inWaitQueue = value;
    }

    public synchronized void setIsInPreemptableQueue(boolean value) {
      this.inPreemptionQueue = value;
    }

    public String getRequestId() {
      return taskRunnerCallable.getRequestId();
    }

    @Override
    public String toString() {
      return "TaskWrapper{" +
          "task=" + taskRunnerCallable.getRequestId() +
          ", inWaitQueue=" + inWaitQueue +
          ", inPreemptionQueue=" + inPreemptionQueue +
          ", registeredForNotifications=" + registeredForNotifications +
          ", canFinish=" + taskRunnerCallable.canFinish() +
          ", firstAttemptStartTime=" + taskRunnerCallable.getFragmentRuntimeInfo().getFirstAttemptStartTime() +
          ", dagStartTime=" + taskRunnerCallable.getFragmentRuntimeInfo().getDagStartTime() +
          ", withinDagPriority=" + taskRunnerCallable.getFragmentRuntimeInfo().getWithinDagPriority() +
          ", vertexParallelism= " + taskRunnerCallable.getFragmentSpec().getVertexParallelism() +
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
  }
}
