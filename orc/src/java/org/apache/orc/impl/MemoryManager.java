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

package org.apache.orc.impl;

import org.apache.orc.OrcConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Implements a memory manager that keeps a global context of how many ORC
 * writers there are and manages the memory between them. For use cases with
 * dynamic partitions, it is easy to end up with many writers in the same task.
 * By managing the size of each allocation, we try to cut down the size of each
 * allocation and keep the task from running out of memory.
 * 
 * This class is not thread safe, but is re-entrant - ensure creation and all
 * invocations are triggered from the same thread.
 */
public class MemoryManager {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryManager.class);

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
  private final OwnedLock ownerLock = new OwnedLock();

  @SuppressWarnings("serial")
  private static class OwnedLock extends ReentrantLock {
    public Thread getOwner() {
      return super.getOwner();
    }
  }

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
  public MemoryManager(Configuration conf) {
    double maxLoad = OrcConf.MEMORY_POOL.getDouble(conf);
    totalMemoryPool = Math.round(ManagementFactory.getMemoryMXBean().
        getHeapMemoryUsage().getMax() * maxLoad);
    ownerLock.lock();
  }

  /**
   * Light weight thread-safety check for multi-threaded access patterns
   */
  private void checkOwner() {
    if (!ownerLock.isHeldByCurrentThread()) {
      LOG.warn("Owner thread expected {}, got {}",
          ownerLock.getOwner(), Thread.currentThread());
    }
  }

  /**
   * Add a new writer's memory allocation to the pool. We use the path
   * as a unique key to ensure that we don't get duplicates.
   * @param path the file that is being written
   * @param requestedAllocation the requested buffer size
   */
  public void addWriter(Path path, long requestedAllocation,
                              Callback callback) throws IOException {
    checkOwner();
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
  public void removeWriter(Path path) throws IOException {
    checkOwner();
    WriterInfo val = writerList.get(path);
    if (val != null) {
      writerList.remove(path);
      totalAllocation -= val.allocation;
      if (writerList.isEmpty()) {
        rowsAddedSinceCheck = 0;
      }
      updateScale(false);
    }
    if(writerList.isEmpty()) {
      rowsAddedSinceCheck = 0;
    }
  }

  /**
   * Get the total pool size that is available for ORC writers.
   * @return the number of bytes in the pool
   */
  public long getTotalMemoryPool() {
    return totalMemoryPool;
  }

  /**
   * The scaling factor for each allocation to ensure that the pool isn't
   * oversubscribed.
   * @return a fraction between 0.0 and 1.0 of the requested size that is
   * available for each writer.
   */
  public double getAllocationScale() {
    return currentScale;
  }

  /**
   * Give the memory manager an opportunity for doing a memory check.
   * @param rows number of rows added
   * @throws IOException
   */
  public void addedRow(int rows) throws IOException {
    rowsAddedSinceCheck += rows;
    if (rowsAddedSinceCheck >= ROWS_BETWEEN_CHECKS) {
      notifyWriters();
    }
  }

  /**
   * Notify all of the writers that they should check their memory usage.
   * @throws IOException
   */
  public void notifyWriters() throws IOException {
    checkOwner();
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
