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

package org.apache.hadoop.hive.ql.io.orc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements a memory manager that keeps a global context of how many ORC
 * writers there are and manages the memory between them. For use cases with
 * dynamic partitions, it is easy to end up with many writers in the same task.
 * By managing the size of each allocation, we try to cut down the size of each
 * allocation and keep the task from running out of memory.
 *
 * This class is thread safe and uses synchronization around the shared state
 * to prevent race conditions.
 */
class MemoryManager {

  private static final Log LOG = LogFactory.getLog(MemoryManager.class);

  /**
   * How often should we check the memory sizes? Measured in rows added
   * to all of the writers.
   */
  private static final int ROWS_BETWEEN_CHECKS = 5000;
  private final long totalMemoryPool;
  private final Map<Path, WriterInfo> writerList =
      new HashMap<Path, WriterInfo>();
  private long totalAllocation = 0;
  private double currentScale = 1;
  private int rowsAddedSinceCheck = 0;

  private static class WriterInfo {
    long allocation;
    Callback callback;
    WriterInfo(long allocation, Callback callback) {
      this.allocation = allocation;
      this.callback = callback;
    }
  }

  public interface Callback {
    /**
     * The writer needs to check its memory usage
     * @param newScale the current scale factor for memory allocations
     * @return true if the writer was over the limit
     * @throws IOException
     */
    boolean checkMemory(double newScale) throws IOException;
  }

  /**
   * Create the memory manager.
   * @param conf use the configuration to find the maximum size of the memory
   *             pool.
   */
  MemoryManager(Configuration conf) {
    HiveConf.ConfVars poolVar = HiveConf.ConfVars.HIVE_ORC_FILE_MEMORY_POOL;
    double maxLoad = conf.getFloat(poolVar.varname, poolVar.defaultFloatVal);
    totalMemoryPool = Math.round(ManagementFactory.getMemoryMXBean().
        getHeapMemoryUsage().getMax() * maxLoad);
  }

  /**
   * Add a new writer's memory allocation to the pool. We use the path
   * as a unique key to ensure that we don't get duplicates.
   * @param path the file that is being written
   * @param requestedAllocation the requested buffer size
   */
  synchronized void addWriter(Path path, long requestedAllocation,
                              Callback callback) throws IOException {
    WriterInfo oldVal = writerList.get(path);
    // this should always be null, but we handle the case where the memory
    // manager wasn't told that a writer wasn't still in use and the task
    // starts writing to the same path.
    if (oldVal == null) {
      oldVal = new WriterInfo(requestedAllocation, callback);
      writerList.put(path, oldVal);
      totalAllocation += requestedAllocation;
    } else {
      // handle a new writer that is writing to the same path
      totalAllocation += requestedAllocation - oldVal.allocation;
      oldVal.allocation = requestedAllocation;
      oldVal.callback = callback;
    }
    updateScale(true);
  }

  /**
   * Remove the given writer from the pool.
   * @param path the file that has been closed
   */
  synchronized void removeWriter(Path path) throws IOException {
    WriterInfo val = writerList.get(path);
    if (val != null) {
      writerList.remove(path);
      totalAllocation -= val.allocation;
      updateScale(false);
    }
  }

  /**
   * Get the total pool size that is available for ORC writers.
   * @return the number of bytes in the pool
   */
  long getTotalMemoryPool() {
    return totalMemoryPool;
  }

  /**
   * The scaling factor for each allocation to ensure that the pool isn't
   * oversubscribed.
   * @return a fraction between 0.0 and 1.0 of the requested size that is
   * available for each writer.
   */
  synchronized double getAllocationScale() {
    return currentScale;
  }

  /**
   * Give the memory manager an opportunity for doing a memory check.
   * @throws IOException
   */
  synchronized void addedRow() throws IOException {
    if (++rowsAddedSinceCheck >= ROWS_BETWEEN_CHECKS) {
      notifyWriters();
    }
  }

  /**
   * Notify all of the writers that they should check their memory usage.
   * @throws IOException
   */
  private void notifyWriters() throws IOException {
    LOG.debug("Notifying writers after " + rowsAddedSinceCheck);
    for(WriterInfo writer: writerList.values()) {
      boolean flushed = writer.callback.checkMemory(currentScale);
      if (LOG.isDebugEnabled() && flushed) {
        LOG.debug("flushed " + writer.toString());
      }
    }
    rowsAddedSinceCheck = 0;
  }

  /**
   * Update the currentScale based on the current allocation and pool size.
   * This also updates the notificationTrigger.
   * @param isAllocate is this an allocation?
   */
  private void updateScale(boolean isAllocate) throws IOException {
    if (totalAllocation <= totalMemoryPool) {
      currentScale = 1;
    } else {
      currentScale = (double) totalMemoryPool / totalAllocation;
    }
  }
}
