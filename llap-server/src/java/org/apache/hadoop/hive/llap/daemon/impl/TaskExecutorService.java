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

import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.exception.ExceptionUtils;
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
 * throw RejectedExecutionException.
 * <p/>
 * Task executor service can be shut down which will terminated all running tasks and reject all
 * new tasks. Shutting down of the task executor service can be done gracefully or immediately.
 */
public class TaskExecutorService implements Scheduler<TaskRunnerCallable> {
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorService.class);
  private static final boolean isInfoEnabled = LOG.isInfoEnabled();
  private static final boolean isDebugEnabled = LOG.isDebugEnabled();
  private static final String TASK_EXECUTOR_THREAD_NAME_FORMAT = "Task-Executor-%d";
  private static final String WAIT_QUEUE_SCHEDULER_THREAD_NAME_FORMAT = "Wait-Queue-Scheduler-%d";

  // some object to lock upon. Used by task scheduler to notify wait queue scheduler of new items
  // to wait queue
  private final Object waitLock;
  private final ListeningExecutorService executorService;
  private final EvictingPriorityBlockingQueue<TaskRunnerCallable> waitQueue;
  private final ListeningExecutorService waitQueueExecutorService;
  private final BlockingQueue<TaskRunnerCallable> preemptionQueue;
  private final boolean enablePreemption;
  private final ThreadPoolExecutor threadPoolExecutor;
  private final AtomicInteger numSlotsAvailable;

  public TaskExecutorService(int numExecutors, int waitQueueSize, boolean enablePreemption) {
    this.waitLock = new Object();
    this.waitQueue = new EvictingPriorityBlockingQueue<>(new WaitQueueComparator(), waitQueueSize);
    this.threadPoolExecutor = new ThreadPoolExecutor(numExecutors, // core pool size
        numExecutors, // max pool size
        1, TimeUnit.MINUTES,
        new SynchronousQueue<Runnable>(), // direct hand-off
        new ThreadFactoryBuilder().setNameFormat(TASK_EXECUTOR_THREAD_NAME_FORMAT).build());
    this.executorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
    this.preemptionQueue = new PriorityBlockingQueue<>(numExecutors,
        new PreemptionQueueComparator());
    this.enablePreemption = enablePreemption;
    this.numSlotsAvailable = new AtomicInteger(numExecutors);

    // single threaded scheduler for tasks from wait queue to executor threads
    ExecutorService wes = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
        .setNameFormat(WAIT_QUEUE_SCHEDULER_THREAD_NAME_FORMAT).build());
    this.waitQueueExecutorService = MoreExecutors.listeningDecorator(wes);
    ListenableFuture<?> future = waitQueueExecutorService.submit(new WaitQueueWorker());
    Futures.addCallback(future, new WaitQueueWorkerCallback());
  }

  /**
   * Worker that takes tasks from wait queue and schedule it for execution.
   */
  private final class WaitQueueWorker implements Runnable {
    TaskRunnerCallable task;

    @Override
    public void run() {
      try {

        synchronized (waitLock) {
          while (waitQueue.isEmpty()) {
            waitLock.wait();
          }
        }

        // Since schedule() can be called from multiple threads, we peek the wait queue,
        // try scheduling the task and then remove the task if scheduling is successful.
        // This will make sure the task's place in the wait queue is held until it gets scheduled.
        while ((task = waitQueue.peek()) != null) {

          // if the task cannot finish and if no slots are available then don't schedule it.
          // TODO: Event notifications that change canFinish state should notify waitLock
          synchronized (waitLock) {
            if (!task.canFinish() && numSlotsAvailable.get() == 0) {
              waitLock.wait();
            }
          }

          boolean scheduled = trySchedule(task);
          if (scheduled) {
            // wait queue could have been re-ordered in the mean time because of concurrent task
            // submission. So remove the specific task instead of the head task.
            waitQueue.remove(task);
          }

          synchronized (waitLock) {
            while (waitQueue.isEmpty()) {
              waitLock.wait();
            }
          }
        }

      } catch (InterruptedException e) {
        // Executor service will create new thread if the current thread gets interrupted. We don't
        // need to do anything with the exception.
        LOG.info(WAIT_QUEUE_SCHEDULER_THREAD_NAME_FORMAT + " thread has been interrupted.");
      }
    }
  }

  private static class WaitQueueWorkerCallback implements FutureCallback {

    @Override
    public void onSuccess(Object result) {
      LOG.error("Wait queue scheduler worker exited with success!");
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("Wait queue scheduler worker exited with failure!");
    }
  }

  @Override
  public void schedule(TaskRunnerCallable task) throws RejectedExecutionException {
    TaskRunnerCallable evictedTask = waitQueue.offer(task);
    if (evictedTask == null) {
      if (isInfoEnabled) {
        LOG.info(task.getRequestId() + " added to wait queue.");
      }

      synchronized (waitLock) {
        waitLock.notify();
      }
    } else {
      evictedTask.killTask();
      if (isInfoEnabled) {
        LOG.info(task.getRequestId() + " evicted from wait queue because of low priority");
      }
    }
  }

  private boolean trySchedule(final TaskRunnerCallable task) {

    boolean scheduled = false;
    try {
      ListenableFuture<TaskRunner2Result> future = executorService.submit(task);
      FutureCallback<TaskRunner2Result> wrappedCallback = new InternalCompletionListener(task);
      Futures.addCallback(future, wrappedCallback);

      if (isInfoEnabled) {
        LOG.info(task.getRequestId() + " scheduled for execution.");
      }

      // only tasks that cannot finish immediately are pre-emptable. In other words, if all inputs
      // to the tasks are not ready yet, the task is eligible for pre-emptable.
      if (enablePreemption && !task.canFinish()) {
        if (isInfoEnabled) {
          LOG.info(task.getRequestId() + " is not finishable. Adding it to pre-emption queue.");
        }
        preemptionQueue.add(task);
      }

      numSlotsAvailable.decrementAndGet();
      scheduled = true;
    } catch (RejectedExecutionException e) {

      if (enablePreemption && task.canFinish() && !preemptionQueue.isEmpty()) {

        if (isDebugEnabled) {
          LOG.trace("preemptionQueue: " + preemptionQueue);
        }

        TaskRunnerCallable pRequest = preemptionQueue.remove();
        if (pRequest != null && !pRequest.isCompleted() && !pRequest.isKillInvoked()) {

          if (isInfoEnabled) {
            LOG.info("Kill task invoked for " + pRequest.getRequestId() + " due to pre-emption");
          }

          pRequest.setKillInvoked();
          pRequest.killTask();
        }
      }
    }

    return scheduled;
  }

  private final class InternalCompletionListener implements
      FutureCallback<TaskRunner2Result> {
    private TaskRunnerCallable task;

    public InternalCompletionListener(TaskRunnerCallable task) {
      this.task = task;
    }

    @Override
    public void onSuccess(TaskRunner2Result result) {
      task.setCompleted();
      task.getCallback().onSuccess(result);
      updatePreemptionListAndNotify(result.getEndReason());
    }

    @Override
    public void onFailure(Throwable t) {
      task.setCompleted();
      task.getCallback().onFailure(t);
      updatePreemptionListAndNotify(null);
      LOG.error("Failed notification received: Stacktrace: " + ExceptionUtils.getStackTrace(t));
    }

    private void updatePreemptionListAndNotify(EndReason reason) {
      // if this task was added to pre-emption list, remove it
      if (enablePreemption) {
        String state = reason == null ? "FAILED" : reason.name();
        preemptionQueue.remove(task.getRequest());
        if (isInfoEnabled) {
          LOG.info(TaskRunnerCallable.getTaskIdentifierString(task.getRequest())
              + " request " + state + "! Removed from preemption list.");
        }
      }

      numSlotsAvailable.incrementAndGet();
      if (!waitQueue.isEmpty()) {
        synchronized (waitLock) {
          waitLock.notify();
        }
      }
    }

  }

  // TODO: llap daemon should call this to gracefully shutdown the task executor service
  public void shutDown(boolean awaitTermination) {
    if (awaitTermination) {
      if (isDebugEnabled) {
        LOG.debug("awaitTermination: " + awaitTermination + " shutting down task executor" +
            " service gracefully");
      }
      executorService.shutdown();
      try {
        if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
          executorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        executorService.shutdownNow();
      }

      waitQueueExecutorService.shutdown();
      try {
        if (!waitQueueExecutorService.awaitTermination(1, TimeUnit.MINUTES)) {
          waitQueueExecutorService.shutdownNow();
        }
      } catch (InterruptedException e) {
        waitQueueExecutorService.shutdownNow();
      }
    } else {
      if (isDebugEnabled) {
        LOG.debug("awaitTermination: " + awaitTermination + " shutting down task executor" +
            " service immediately");
      }
      executorService.shutdownNow();
      waitQueueExecutorService.shutdownNow();
    }
  }

  @VisibleForTesting
  public static class WaitQueueComparator implements Comparator<TaskRunnerCallable> {

    @Override
    public int compare(TaskRunnerCallable o1, TaskRunnerCallable o2) {
      boolean newCanFinish = o1.canFinish();
      boolean oldCanFinish = o2.canFinish();
      if (newCanFinish == true && oldCanFinish == false) {
        return -1;
      } else if (newCanFinish == false && oldCanFinish == true) {
        return 1;
      }

      if (o1.getVertexParallelism() > o2.getVertexParallelism()) {
        return 1;
      } else if (o1.getVertexParallelism() < o2.getVertexParallelism()) {
        return -1;
      }
      return 0;
    }
  }

  @VisibleForTesting
  public static class PreemptionQueueComparator implements Comparator<TaskRunnerCallable> {

    @Override
    public int compare(TaskRunnerCallable o1, TaskRunnerCallable o2) {
      if (o1.getVertexParallelism() > o2.getVertexParallelism()) {
        return 1;
      } else if (o1.getVertexParallelism() < o2.getVertexParallelism()) {
        return -1;
      }
      return 0;
    }
  }
}
