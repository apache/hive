/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client.metrics;

import java.io.Serializable;

import org.apache.spark.executor.TaskMetrics;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;

/**
 * Metrics pertaining to reading shuffle data.
 */
@InterfaceAudience.Private
public class ShuffleReadMetrics implements Serializable {

  /** Number of remote blocks fetched in shuffles by tasks. */
  public final long remoteBlocksFetched;
  /** Number of local blocks fetched in shuffles by tasks. */
  public final long localBlocksFetched;
  /**
   * Time tasks spent waiting for remote shuffle blocks. This only includes the
   * time blocking on shuffle input data. For instance if block B is being
   * fetched while the task is still not finished processing block A, it is not
   * considered to be blocking on block B.
   */
  public final long fetchWaitTime;
  /** Total number of remote bytes read from the shuffle by tasks. */
  public final long remoteBytesRead;
  /** Shuffle data that was read from the local disk (as opposed to from a remote executor). */
  public final long localBytesRead;
  /** Total number of remotes bytes read to disk from the shuffle by this task. */
  public final long remoteBytesReadToDisk;
  /** Total number of records read from the shuffle by this task. */
  public final long recordsRead;

  private ShuffleReadMetrics() {
    // For Serialization only.
    this(0, 0, 0L, 0L, 0L, 0L, 0L);
  }

  public ShuffleReadMetrics(
      long remoteBlocksFetched,
      long localBlocksFetched,
      long fetchWaitTime,
      long remoteBytesRead,
      long localBytesRead,
      long remoteBytesReadToDisk,
      long recordsRead) {
    this.remoteBlocksFetched = remoteBlocksFetched;
    this.localBlocksFetched = localBlocksFetched;
    this.fetchWaitTime = fetchWaitTime;
    this.remoteBytesRead = remoteBytesRead;
    this.localBytesRead = localBytesRead;
    this.remoteBytesReadToDisk = remoteBytesReadToDisk;
    this.recordsRead = recordsRead;
  }

  public ShuffleReadMetrics(TaskMetrics metrics) {
    this(metrics.shuffleReadMetrics().remoteBlocksFetched(),
      metrics.shuffleReadMetrics().localBlocksFetched(),
      metrics.shuffleReadMetrics().fetchWaitTime(),
      metrics.shuffleReadMetrics().remoteBytesRead(),
      metrics.shuffleReadMetrics().localBytesRead(),
      metrics.shuffleReadMetrics().remoteBytesReadToDisk(),
      metrics.shuffleReadMetrics().recordsRead());
  }

  /**
   * Number of blocks fetched in shuffle by tasks (remote or local).
   */
  public long getTotalBlocksFetched() {
    return remoteBlocksFetched + localBlocksFetched;
  }

  @Override
  public String toString() {
    return "ShuffleReadMetrics{" +
            "remoteBlocksFetched=" + remoteBlocksFetched +
            ", localBlocksFetched=" + localBlocksFetched +
            ", fetchWaitTime=" + fetchWaitTime +
            ", remoteBytesRead=" + remoteBytesRead +
            '}';
  }
}
