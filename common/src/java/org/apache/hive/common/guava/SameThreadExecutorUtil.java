/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.common.guava;

import com.google.common.util.concurrent.*;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Factory and utility methods for {@link java.util.concurrent.Executor}, {@link
 * ExecutorService}, and {@link ThreadFactory}.
 *
 * @author Eric Fellheimer
 * @author Kyle Littlefield
 * @author Justin Mahoney
 * @since 3.0
 */
public final class SameThreadExecutorUtil {
  private SameThreadExecutorUtil() {}

  /**
   * Creates an executor service that runs each task in the thread
   * that invokes {@code execute/submit}, as in {@link CallerRunsPolicy}  This
   * applies both to individually submitted tasks and to collections of tasks
   * submitted via {@code invokeAll} or {@code invokeAny}.  In the latter case,
   * tasks will run serially on the calling thread.  Tasks are run to
   * completion before a {@code Future} is returned to the caller (unless the
   * executor has been shutdown).
   *
   * <p>Although all tasks are immediately executed in the thread that
   * submitted the task, this {@code ExecutorService} imposes a small
   * locking overhead on each task submission in order to implement shutdown
   * and termination behavior.
   *
   * <p>The implementation deviates from the {@code ExecutorService}
   * specification with regards to the {@code shutdownNow} method.  First,
   * "best-effort" with regards to canceling running tasks is implemented
   * as "no-effort".  No interrupts or other attempts are made to stop
   * threads executing tasks.  Second, the returned list will always be empty,
   * as any submitted task is considered to have started execution.
   * This applies also to tasks given to {@code invokeAll} or {@code invokeAny}
   * which are pending serial execution, even the subset of the tasks that
   * have not yet started execution.  It is unclear from the
   * {@code ExecutorService} specification if these should be included, and
   * it's much easier to implement the interpretation that they not be.
   * Finally, a call to {@code shutdown} or {@code shutdownNow} may result
   * in concurrent calls to {@code invokeAll/invokeAny} throwing
   * RejectedExecutionException, although a subset of the tasks may already
   * have been executed.
   *
   * @since 10.0 (<a href="http://code.google.com/p/guava-libraries/wiki/Compatibility"
   *        >mostly source-compatible</a> since 3.0)
   */
  public static ListeningExecutorService sameThreadExecutor() {
    return new SameThreadExecutorService();
  }

  // See sameThreadExecutor javadoc for behavioral notes.
  private static class SameThreadExecutorService
          extends AbstractListeningExecutorService {
    /**
     * Lock used whenever accessing the state variables
     * (runningTasks, shutdown, terminationCondition) of the executor
     */
    private final Lock lock = new ReentrantLock();

    /** Signaled after the executor is shutdown and running tasks are done */
    private final Condition termination = lock.newCondition();

    /*
     * Conceptually, these two variables describe the executor being in
     * one of three states:
     *   - Active: shutdown == false
     *   - Shutdown: runningTasks > 0 and shutdown == true
     *   - Terminated: runningTasks == 0 and shutdown == true
     */
    private int runningTasks = 0;
    private boolean shutdown = false;

    @Override
    public void execute(Runnable command) {
      startTask();
      try {
        command.run();
      } finally {
        endTask();
      }
    }

    @Override
    public boolean isShutdown() {
      lock.lock();
      try {
        return shutdown;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void shutdown() {
      lock.lock();
      try {
        shutdown = true;
      } finally {
        lock.unlock();
      }
    }

    // See sameThreadExecutor javadoc for unusual behavior of this method.
    @Override
    public List<Runnable> shutdownNow() {
      shutdown();
      return Collections.emptyList();
    }

    @Override
    public boolean isTerminated() {
      lock.lock();
      try {
        return shutdown && runningTasks == 0;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
      long nanos = unit.toNanos(timeout);
      lock.lock();
      try {
        for (;;) {
          if (isTerminated()) {
            return true;
          } else if (nanos <= 0) {
            return false;
          } else {
            nanos = termination.awaitNanos(nanos);
          }
        }
      } finally {
        lock.unlock();
      }
    }

    /**
     * Checks if the executor has been shut down and increments the running
     * task count.
     *
     * @throws RejectedExecutionException if the executor has been previously
     *         shutdown
     */
    private void startTask() {
      lock.lock();
      try {
        if (isShutdown()) {
          throw new RejectedExecutionException("Executor already shutdown");
        }
        runningTasks++;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Decrements the running task count.
     */
    private void endTask() {
      lock.lock();
      try {
        runningTasks--;
        if (isTerminated()) {
          termination.signalAll();
        }
      } finally {
        lock.unlock();
      }
    }
  }
}
