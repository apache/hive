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

package org.apache.hadoop.hive.ql.exec;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TaskRunner implementation.
 **/

public class TaskRunner extends Thread {
  protected Task<? extends Serializable> tsk;
  protected TaskResult result;
  protected SessionState ss;
  private static AtomicLong taskCounter = new AtomicLong(0);
  private static ThreadLocal<Long> taskRunnerID = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return taskCounter.incrementAndGet();
    }
  };

  protected Thread runner;

  private static transient final Logger LOG = LoggerFactory.getLogger(TaskRunner.class);

  private final DriverContext driverCtx;

  public TaskRunner(Task<? extends Serializable> tsk, DriverContext ctx) {
    this.tsk = tsk;
    this.result = new TaskResult();
    ss = SessionState.get();
    driverCtx = ctx;
  }

  public Task<? extends Serializable> getTask() {
    return tsk;
  }

  public TaskResult getTaskResult() {
    return result;
  }

  public Thread getRunner() {
    return runner;
  }

  public boolean isRunning() {
    return result.isRunning();
  }

  @Override
  public void run() {
    runner = Thread.currentThread();
    try {
      SessionState.start(ss);
      runSequential();
    } finally {
      try {
        // Call Hive.closeCurrent() that closes the HMS connection, causes
        // HMS connection leaks otherwise.
        Hive.closeCurrent();
      } catch (Exception e) {
        LOG.warn("Exception closing Metastore connection:" + e.getMessage());
      }
      runner = null;
      result.setRunning(false);
    }
  }

  /**
   * Launches a task, and sets its exit value in the result variable.
   */

  public void runSequential() {
    int exitVal = -101;
    try {
      exitVal = tsk.executeTask(ss == null ? null : ss.getHiveHistory());
    } catch (Throwable t) {
      if (tsk.getException() == null) {
        tsk.setException(t);
      }
      LOG.error("Error in executeTask", t);
    }
    result.setExitVal(exitVal);
    driverCtx.releaseRunnable();
    if (tsk.getException() != null) {
      result.setTaskError(tsk.getException());
    }
  }

  public static long getTaskRunnerID () {
    return taskRunnerID.get();
  }
}
