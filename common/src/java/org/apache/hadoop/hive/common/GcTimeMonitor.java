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

package org.apache.hadoop.hive.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Based on org.apache.hadoop.util.GcTimeMonitor. However, this class detects
 * GC pauses using the same method as JvmPauseMonitor (by comparing the actual
 * and expected thread sleep time) rather than by reading information from
 * GarbageCollectionMXBean. The latter may sometimes report time spent in
 * concurrent GC operations rather than GC pauses. This may result in inaccurate
 * results when trying to estimate the time that the JVM is "frozen" due to GC.
 *
 * This class monitors the percentage of time the JVM is paused in GC within
 * the specified observation window, say 1 minute. The user can provide a
 * hook which will be called whenever this percentage exceeds the specified
 * threshold.
 */
public class GcTimeMonitor extends Thread {

  private final long maxGcTimePercentage;
  private final long observationWindowNanos, sleepIntervalMs;
  private final GcTimeAlertHandler alertHandler;

  // Ring buffers containing GC timings and timestamps when timings were taken
  private final TsAndData[] gcDataBuf;
  private int bufSize, startIdx, endIdx;

  private long startTimeNanos;
  private final GcData curData = new GcData();
  private volatile boolean shouldRun = true;

  /**
   * Create an instance of GCTimeMonitor. Once it's started, it will stay alive
   * and monitor GC time percentage until shutdown() is called. If you don't
   * put a limit on the number of GCTimeMonitor instances that you create, and
   * alertHandler != null, you should necessarily call shutdown() once the given
   * instance is not needed. Otherwise, you may create a memory leak, because
   * each running GCTimeMonitor will keep its alertHandler object in memory,
   * which in turn may reference and keep in memory many more other objects.
   *
   * @param observationWindowMs the interval over which the percentage
   *   of GC time should be calculated. A practical value would be somewhere
   *   between 30 sec and several minutes.
   * @param sleepIntervalMs how frequently this thread should wake up to check
   *   GC timings. This is also a frequency with which alertHandler will be
   *   invoked if GC time percentage exceeds the specified limit. A practical
   *   value would likely be 500..1000 ms.
   * @param maxGcTimePercentage A GC time percentage limit (0..100) within
   *   observationWindowMs. Once this is exceeded, alertHandler will be
   *   invoked every sleepIntervalMs milliseconds until GC time percentage
   *   falls below this limit.
   * @param alertHandler a single method in this interface is invoked when GC
   *   time percentage exceeds the specified limit.
   */
  public GcTimeMonitor(long observationWindowMs, long sleepIntervalMs,
      int maxGcTimePercentage, GcTimeAlertHandler alertHandler) {
    Preconditions.checkArgument(observationWindowMs > 0);
    Preconditions.checkArgument(
        sleepIntervalMs > 0 && sleepIntervalMs < observationWindowMs);
    Preconditions.checkArgument(
        maxGcTimePercentage >= 0 && maxGcTimePercentage <= 100);

    this.observationWindowNanos = observationWindowMs * 1000000;
    this.sleepIntervalMs = sleepIntervalMs;
    this.maxGcTimePercentage = maxGcTimePercentage;
    this.alertHandler = alertHandler;

    bufSize = (int) (observationWindowMs / sleepIntervalMs + 2);
    // Prevent the user from accidentally creating an abnormally big buffer,
    // which will result in slow calculations and likely inaccuracy.
    Preconditions.checkArgument(bufSize <= 128 * 1024);
    gcDataBuf = new TsAndData[bufSize];
    for (int i = 0; i < bufSize; i++) {
      gcDataBuf[i] = new TsAndData();
    }

    this.setDaemon(true);
    this.setName("GcTimeMonitor obsWindow = " + observationWindowMs +
        ", sleepInterval = " + sleepIntervalMs +
        ", maxGcTimePerc = " + maxGcTimePercentage);
  }

  @Override
  public void run() {
    startTimeNanos = System.nanoTime();
    gcDataBuf[startIdx].setValues(startTimeNanos, 0);

    while (shouldRun) {
      long intervalStartTsNanos = System.nanoTime();
      try {
        Thread.sleep(sleepIntervalMs);
      } catch (InterruptedException ie) {
        return;
      }
      long intervalEndTsNanos = System.nanoTime();

      calculateGCTimePercentageWithinObservedInterval(intervalStartTsNanos, intervalEndTsNanos);
      if (alertHandler != null &&
          curData.gcTimePercentage > maxGcTimePercentage) {
        alertHandler.alert(curData.clone());
      }
    }
  }

  public void shutdown() {
    shouldRun = false;
  }

  /** Returns a copy of the most recent data measured by this monitor. */
  public GcData getLatestGcData() {
    return curData.clone();
  }

  private void calculateGCTimePercentageWithinObservedInterval(
      long intervalStartTsNanos, long intervalEndTsNanos) {
    long gcTimeWithinSleepIntervalNanos =
        intervalEndTsNanos - intervalStartTsNanos - sleepIntervalMs * 1000000;
    long totalGcTimeNanos = curData.totalGcTimeNanos + gcTimeWithinSleepIntervalNanos;

    long gcMonitorRunTimeNanos = intervalEndTsNanos - startTimeNanos;

    endIdx = (endIdx + 1) % bufSize;
    gcDataBuf[endIdx].setValues(intervalEndTsNanos, gcTimeWithinSleepIntervalNanos);

    // Update the observation window so that it covers the last observationWindowNanos
    // period. For that, move startIdx forward until we reach the first buffer entry with
    // timestamp within the observation window.
    long startObsWindowTsNanos = intervalEndTsNanos - observationWindowNanos;
    while (gcDataBuf[startIdx].tsNanos < startObsWindowTsNanos && startIdx != endIdx) {
      startIdx = (startIdx + 1) % bufSize;
    }

    // Calculate total GC time within observationWindowMs.
    // We should be careful about GC time that passed before the first timestamp
    // in our observation window.
    long gcTimeWithinObservationWindowNanos = Math.min(
        gcDataBuf[startIdx].gcPauseNanos, gcDataBuf[startIdx].tsNanos - startObsWindowTsNanos);
    if (startIdx != endIdx) {
      for (int i = (startIdx + 1) % bufSize; i != endIdx;
           i = (i + 1) % bufSize) {
        gcTimeWithinObservationWindowNanos += gcDataBuf[i].gcPauseNanos;
      }
    }

    curData.update(gcMonitorRunTimeNanos, totalGcTimeNanos,
        (int) (gcTimeWithinObservationWindowNanos * 100 /
            Math.min(observationWindowNanos, gcMonitorRunTimeNanos)));
  }

  /**
   * The user can provide an instance of a class implementing this interface
   * when initializing a GcTimeMonitor to receive alerts when GC time
   * percentage exceeds the specified threshold.
   */
  public interface GcTimeAlertHandler {
    void alert(GcData gcData);
  }

  /** Encapsulates data about GC pauses measured at the specific timestamp. */
  public static class GcData implements Cloneable {
    private long gcMonitorRunTimeNanos, totalGcTimeNanos;
    private int gcTimePercentage;

    /** Returns the time since the start of the associated GcTimeMonitor. */
    public long getGcMonitorRunTimeMs() {
      return gcMonitorRunTimeNanos / 1000000;
    }

    /** Returns accumulated GC time since this JVM started. */
    public long getAccumulatedGcTimeMs() {
      return totalGcTimeNanos / 1000000;
    }

    /**
     * Returns the percentage (0..100) of time that the JVM spent in GC pauses
     * within the observation window of the associated GcTimeMonitor.
     */
    public int getGcTimePercentage() {
      return gcTimePercentage;
    }

    private synchronized void update(long gcMonitorRunTimeNanos,
        long totalGcTimeNanos, int inGcTimePercentage) {
      this.gcMonitorRunTimeNanos = gcMonitorRunTimeNanos;
      this.totalGcTimeNanos = totalGcTimeNanos;
      this.gcTimePercentage = inGcTimePercentage;
    }

    @Override
    public synchronized GcData clone() {
      try {
        return (GcData) super.clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class TsAndData {
    private long tsNanos;      // Timestamp when this measurement was taken
    private long gcPauseNanos; // Total GC pause time within the interval between ts
                               // and the timestamp of the previous measurement.

    void setValues(long tsNanos, long gcPauseNanos) {
      this.tsNanos = tsNanos;
      this.gcPauseNanos = gcPauseNanos;
    }
  }

  /**
   * Simple 'main' to facilitate manual testing of the pause monitor.
   *
   * This main function just leaks memory. Running this class will quickly
   * result in a "GC hell" and subsequent alerts from the GcTimeMonitor.
   */
  public static void main(String []args) throws Exception {
    new GcTimeMonitor(20 * 1000, 500, 20,
          new GcTimeMonitor.GcTimeAlertHandler() {
            @Override
            public void alert(GcData gcData) {
              System.err.println(
                  "GcTimeMonitor alert. Current GC time percentage = " +
                  gcData.getGcTimePercentage() +
                  ", total run time = " + (gcData.getGcMonitorRunTimeMs() / 1000) + " sec" +
                  ", total GC time = " + (gcData.getAccumulatedGcTimeMs() / 1000) + " sec");
            }
          }).start();

    List<String> list = Lists.newArrayList();
    for (int i = 0; ; i++) {
      list.add("This is a long string to fill memory quickly " + i);
      if (i % 100000 == 0) {
        System.out.println("Added " + i + " strings");
        Thread.sleep(100);
      }
    }
  }

}

