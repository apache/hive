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
package org.apache.hadoop.hive.ql.exec.tez;

import java.util.List;

/**
 * Represents the mapping from logical resource allocations to queries from WM, to actual physical
 * allocations performed using some implementation of a scheduler.
 */
interface QueryAllocationManager {
  void start();
  void stop();
  /**
   * Updates the session allocations asynchronously.
   * @param totalMaxAlloc The total maximum fraction of the cluster to allocate. Used to
   *                      avoid various artifacts, esp. with small numbers and double weirdness.
   *                      Null means the total is unknown.
   * @param sessions Sessions to update based on their allocation fraction.
   * @return The number of executors/cpus allocated.
   */
  int updateSessionsAsync(Double totalMaxAlloc, List<WmTezSession> sessions);

  /**
   * @return the number of CPUs equivalent to percentage allocation, for information purposes.
   */
  int translateAllocationToCpus(double allocation);

  /**
   * Sets a callback to be invoked on cluster changes relevant to resource allocation.
   */
  void setClusterChangedCallback(Runnable clusterChangedCallback);
  
  /**
   * Updates the session asynchronously with the existing allocation.
   */
  void updateSessionAsync(WmTezSession session);
}
