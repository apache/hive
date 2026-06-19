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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class JobCallable<T> implements Callable<T> {
  private static final Logger LOG = LoggerFactory.getLogger(JobCallable.class);

  static public enum JobState {
    STARTED,
    FAILED,
    COMPLETED
  }

  /*
   * Job state of job request. Changes to the state are synchronized using
   * setStateAndResult. This is required due to two different threads,
   * main thread and job execute thread, tries to change state and organize
   * clean up tasks.
   */
  private JobState jobState = JobState.STARTED;

  /*
   * Result of JobCallable task after successful task completion. This is
   * expected to be set by the thread which executes JobCallable task.
   */
  public T returnResult = null;

  /*
   * Sets the job state to FAILED. Returns true if FAILED status is set.
   * Otherwise, it returns false.
   */
  public boolean setJobStateFailed() {
    return setStateAndResult(JobState.FAILED, null);
  }

  /*
   * Sets the job state to COMPLETED and also sets the results value. Returns true
   * if COMPLETED status is set. Otherwise, it returns false.
   */
  public boolean setJobStateCompleted(T result) {
    return setStateAndResult(JobState.COMPLETED, result);
  }

  /*
   * Sets the job state and result. Returns true if status and result are set.
   * Otherwise, it returns false.
   */
  private synchronized boolean setStateAndResult(JobState jobState, T result) {
    if (this.jobState == JobState.STARTED) {
      this.jobState = jobState;
      this.returnResult = result;
      return true;
    } else {
      LOG.info("Failed to set job state to " + jobState + " due to job state "
                  + this.jobState + ". Expected state is " + JobState.STARTED);
    }

    return false;
  }

  /*
   * Executes the callable task with help of execute() call and gets the result
   * of the task. It also sets job status as COMPLETED if state is not already
   * set to FAILED and returns result to future.
   */
  public T call() throws Exception {

    /*
     * Don't catch any execution exceptions here and let the caller catch it.
     */
    T result = this.execute();

    if (!this.setJobStateCompleted(result)) {
     /*
      * Failed to set job status as COMPLETED which mean the main thread would have
      * exited and not waiting for the result. Call cleanup() to execute any cleanup.
      */
      cleanup();
      return null;
    }

    return this.returnResult;
  }

  /*
   * Abstract method to be overridden for task execution.
   */
  public abstract T execute() throws Exception;

  /*
   * Cleanup method called to run cleanup tasks if job state is FAILED. By default,
   * no cleanup is provided.
   */
  public void cleanup() {}
}
