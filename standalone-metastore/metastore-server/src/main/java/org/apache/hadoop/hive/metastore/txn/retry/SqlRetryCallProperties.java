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
package org.apache.hadoop.hive.metastore.txn.retry;

/**
 * Contains all the properties which can alter the behavior of a retry-call in 
 * {@link SqlRetryHandler}.
 */
public class SqlRetryCallProperties {

  private String caller = null;
  private boolean retryOnDuplicateKey = false;
  private boolean lockInternally = false;
  private RetryPropagation retryPropagation = RetryPropagation.CREATE_OR_JOIN;
  private int retryCount;
  private int deadlockCount;

  /**
   * @param caller Sets the caller id to use during the retry-call. This is usually the method name and its parameters.
   * @return itself
   */
  public SqlRetryCallProperties withCallerId(String caller) {
    this.caller = caller;
    return this;
  }

  /**
   * @param retryOnDuplicateKey Indicates if the call should be retried on duplicate key error, false otherwise. 
   *                            The default value is false.
   * @return itself
   */
  public SqlRetryCallProperties withRetryOnDuplicateKey(boolean retryOnDuplicateKey) {
    this.retryOnDuplicateKey = retryOnDuplicateKey;
    return this;
  }

  /**
   * @param lockInternally Indicates if the internal (derby-related) lock mechanism has to be used during the retry-call.
   *                       The default value is false.
   * @return itself
   */
  public SqlRetryCallProperties withLockInternally(boolean lockInternally) {
    this.lockInternally = lockInternally;
    return this;
  }

  /**
   * @param retryPropagation The {@link RetryPropagation} to use during the retry-call execution.
   * @return itself
   */
  public SqlRetryCallProperties withRetryPropagation(RetryPropagation retryPropagation) {
    this.retryPropagation = retryPropagation;
    return this;
  }

  /**
   * Internal usage by {@link SqlRetryHandler}. Sets the remaining retry attemps during retry-calls.
   * @param retryCount The remaining retry attempts.
   * @return itself
   */
  SqlRetryCallProperties setRetryCount(int retryCount) {
    this.retryCount = retryCount;
    return this;
  }

  /**
   * Internal usage by {@link SqlRetryHandler}. Sets the remaining deadlock-retry attemps during retry-calls.
   * @param deadlockCount The remaining deadlock-retry attempts.
   * @return itself
   */
  SqlRetryCallProperties setDeadlockCount(int deadlockCount) {
    this.deadlockCount = deadlockCount;
    return this;
  }

  String getCaller() {
    return caller;
  }

  boolean isRetryOnDuplicateKey() {
    return retryOnDuplicateKey;
  }

  boolean isLockInternally() {
    return lockInternally;
  }

  public RetryPropagation getRetryPropagation() {
    return retryPropagation;
  }

  int getRetryCount() {
    return retryCount;
  }

  int getDeadlockCount() {
    return deadlockCount;
  }

  @Override
  public String toString() {
    return "[Id:" + caller
        + ", retryOnDuplicateKey:" + retryOnDuplicateKey
        + ", retryPropagation:" + retryPropagation
        + ", lockInternally:" + lockInternally
        + ", retryLimit:" + retryCount
        + ", deadlockLimit:" + deadlockCount
        + "]";
  }

}
