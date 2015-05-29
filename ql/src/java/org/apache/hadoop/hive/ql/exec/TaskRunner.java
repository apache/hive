/**
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

import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * TaskRunner implementation.
 **/

public class TaskRunner extends Thread {

  protected Task<? extends Serializable> tsk;
  protected TaskResult result;
  protected SessionState ss;
  private OperationLog operationLog;
  private static AtomicLong taskCounter = new AtomicLong(0);
  private static ThreadLocal<Long> taskRunnerID = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return taskCounter.incrementAndGet();
    }
  };

  protected Thread runner;

  public TaskRunner(Task<? extends Serializable> tsk, TaskResult result) {
    this.tsk = tsk;
    this.result = result;
    ss = SessionState.get();
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
      OperationLog.setCurrentOperationLog(operationLog);
      SessionState.start(ss);
      runSequential();
    } finally {
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
      exitVal = tsk.executeTask();
    } catch (Throwable t) {
      if (tsk.getException() == null) {
        tsk.setException(t);
      }
      t.printStackTrace();
    }
    result.setExitVal(exitVal, tsk.getException());
  }

  public static long getTaskRunnerID () {
    return taskRunnerID.get();
  }

  public void setOperationLog(OperationLog operationLog) {
    this.operationLog = operationLog;
  }
}
