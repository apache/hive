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
package org.apache.hadoop.hive.ql.exec.mapjoin;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.text.NumberFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

/**
 * Handles the logic around deciding when to throw an MapJoinMemoryExhaustionException
 * for HashTableSinkOperator.
 */
public class MapJoinMemoryExhaustionHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MapJoinMemoryExhaustionHandler.class);

  public final MemoryMXBean memoryMXBean;

  /**
   * The percentage of overall heap that the JVM is allowed
   * to allocate before failing a MapJoin local task.
   */
  private final double maxMemoryUsage;
  /**
   * The max heap of the JVM in bytes.
   */
  private final long maxHeapSize;
  private final LogHelper console;
  private final NumberFormat percentageNumberFormat;
  /**
   * Constructor expects a LogHelper object in addition to the max percent
   * of heap memory which can be consumed before a MapJoinMemoryExhaustionException
   * is thrown.
   */
  public MapJoinMemoryExhaustionHandler(LogHelper console, double maxMemoryUsage) {
    this.console = console;
    this.maxMemoryUsage = maxMemoryUsage;
    this.memoryMXBean = ManagementFactory.getMemoryMXBean();
    this.maxHeapSize = getMaxHeapSize(memoryMXBean);
    percentageNumberFormat = NumberFormat.getInstance();
    percentageNumberFormat.setMinimumFractionDigits(2);
    LOG.info("JVM Max Heap Size: " + this.maxHeapSize);
  }

  public static long getMaxHeapSize() {
    return getMaxHeapSize(ManagementFactory.getMemoryMXBean());
  }

  private static long getMaxHeapSize(MemoryMXBean bean) {
    long maxHeapSize = bean.getHeapMemoryUsage().getMax();
    /*
     * According to the javadoc, getMax() can return -1. In this case
     * default to 200MB. This will probably never actually happen.
     */
    if(maxHeapSize == -1) {
      LOG.warn("MemoryMXBean.getHeapMemoryUsage().getMax() returned -1, " +
          "defaulting maxHeapSize to 200MB");
      return 200L * 1024L * 1024L;
    }
    return maxHeapSize;
  }

  /**
   * Throws MapJoinMemoryExhaustionException when the JVM has consumed the
   * configured percentage of memory. The arguments are used simply for the error
   * message.
   *
   * @param tableContainerSize currently table container size
   * @param numRows number of rows processed
   * @throws MapJoinMemoryExhaustionException
   */
  public void checkMemoryStatus(long tableContainerSize, long numRows)
  throws MapJoinMemoryExhaustionException {
    long usedMemory = memoryMXBean.getHeapMemoryUsage().getUsed();
    double percentage = (double) usedMemory / (double) maxHeapSize;
    String msg = Utilities.now() + "\tProcessing rows:\t" + numRows + "\tHashtable size:\t"
        + tableContainerSize + "\tMemory usage:\t" + usedMemory + "\tpercentage:\t" + percentageNumberFormat.format(percentage);
    console.printInfo(msg);
    if(percentage > maxMemoryUsage) {
      throw new MapJoinMemoryExhaustionException(msg);
    }
   }
}
