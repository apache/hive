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

/**
 * TaskResult implementation.
 * Note that different threads may be reading/writing this object
 **/

public class TaskResult {
  protected volatile int exitVal;
  protected volatile boolean runStatus;
  private volatile Throwable taskError;

  public TaskResult() {
    exitVal = -1;
    setRunning(true);
  }

  public void setExitVal(int exitVal) {
    this.exitVal = exitVal;
    setRunning(false);
  }
  public void setTaskError(Throwable taskError) {
    this.taskError = taskError;
  }
  public void setExitVal(int exitVal, Throwable taskError) {
    setExitVal(exitVal);
    setTaskError(taskError);
  }

  public int getExitVal() {
    return exitVal;
  }

  /**
   * @return may contain details of the error which caused the task to fail or null
   */
  public Throwable getTaskError() {
    return taskError;
  }
  public boolean isRunning() {
    return runStatus;
  }

  public void setRunning(boolean val) {
    runStatus = val;
  }
}
