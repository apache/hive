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
package org.apache.hadoop.hive.llap.daemon.impl;

import java.util.Set;

/**
 * Task scheduler interface
 */
public interface Scheduler<T> {

  enum SubmissionState {
    ACCEPTED, // request accepted
    REJECTED, // request rejected as wait queue is full
    EVICTED_OTHER; // request accepted but evicted other low priority task
  }

  /**
   * Schedule the task or throw RejectedExecutionException if queues are full
   * @param t - task to schedule
   * @return SubmissionState
   */
  SubmissionState schedule(T t);

  /**
   * Attempt to kill the fragment with the specified fragmentId
   * @param fragmentId
   */
  void killFragment(String fragmentId);

  Set<String> getExecutorsStatusForReporting();

  int getNumActiveForReporting();

  QueryIdentifier findQueryByFragment(String fragmentId);

  boolean updateFragment(String fragmentId, boolean isGuaranteed);

  /**
   * Sets the scheduler executor and queue size.
   * @param newExecutors New number of executors
   * @param newWaitQueueSize New size of the queue
   */
  void setCapacity(int newExecutors, int newWaitQueueSize);
}
