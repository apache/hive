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
package org.apache.hadoop.hive.llap;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.fs.FileSystem;
import org.apache.tez.common.counters.FileSystemCounter;
import org.apache.tez.common.counters.TezCounters;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Convenience class for handling thread local statistics for different schemes in LLAP.
 * The motivation is that thread local stats (available from FileSystem.getAllStatistics()) are in a List, which
 * doesn't guarantee that for a single scheme, a single Statistics object is returned (e.g. in case of multiple
 * namenodes). This class encapsulates that data, and takes care of merging them transparently.
 * LlapThreadLocalStatistics is used in LLAP's task ThreadPool, where the current thread's statistics is calculated by a
 * simple delta computation (before/after running the task callable), like:
 *
 * LlapThreadLocalStatistics statsBefore = new LlapThreadLocalStatistics(...);
 * LlapThreadLocalStatistics diff = new LlapThreadLocalStatistics(...).subtract(statsBefore);
 */
public class LlapThreadLocalStatistics {

  /**
   * LLAP IO related counters.
   */
  public enum LlapExecutorCounters {
    EXECUTOR_CPU_NS,
    EXECUTOR_USER_NS;
  }

  @VisibleForTesting
  Map<String, LlapFileSystemStatisticsData> schemeToThreadLocalStats = new HashMap<>();
  @VisibleForTesting
  long cpuTime;
  @VisibleForTesting
  long userTime;

  /**
   * In this constructor we create a snapshot of the current thread local statistics and take care of merging the
   * ones that belong to the same scheme.
   */
  public LlapThreadLocalStatistics(ThreadMXBean mxBean) {
    this(mxBean, FileSystem.getAllStatistics());
  }

  /**
   * Merges the list to a map.
   * Input list:
   * 1. FileSystem.Statistics (scheme: file)
   * 2. FileSystem.Statistics (scheme: hdfs)
   * 3. FileSystem.Statistics (scheme: hdfs)
   * Output map:
   * file : LlapThreadLocalStatistics.StatisticsData (1)
   * hdfs : LlapThreadLocalStatistics.StatisticsData (2 + 3)
   */
  public LlapThreadLocalStatistics(ThreadMXBean mxBean, List<FileSystem.Statistics> allStatistics) {
    cpuTime = mxBean == null ? -1 : mxBean.getCurrentThreadCpuTime();
    userTime = mxBean == null ? -1 : mxBean.getCurrentThreadUserTime();

    for (FileSystem.Statistics statistics : allStatistics) {
      schemeToThreadLocalStats.merge(statistics.getScheme(),
          new LlapFileSystemStatisticsData(statistics.getThreadStatistics()),
          (statsCurrent, statsNew) -> statsCurrent.merge(statistics.getThreadStatistics()));
    }
  }

  // This method iterates on the other LlapThreadLocalStatistics's schemes, and subtract them from this one if it's
  // present here too.
  public LlapThreadLocalStatistics subtract(LlapThreadLocalStatistics other) {
    for (Map.Entry<String, LlapFileSystemStatisticsData> otherStats :
        other.schemeToThreadLocalStats.entrySet()){
      schemeToThreadLocalStats.computeIfPresent(otherStats.getKey(),
          (thisScheme, stats) -> stats.subtract(otherStats.getValue()));
    }

    cpuTime -= other.cpuTime;
    userTime -= other.userTime;

    return this;
  }

  public void fill(TezCounters tezCounters) {
    for (Map.Entry<String, LlapFileSystemStatisticsData> threadLocalStats :
        schemeToThreadLocalStats.entrySet()){
      String scheme = threadLocalStats.getKey();
      LlapFileSystemStatisticsData stats = threadLocalStats.getValue();
      tezCounters.findCounter(scheme, FileSystemCounter.BYTES_READ).increment(stats.bytesRead);
      tezCounters.findCounter(scheme, FileSystemCounter.BYTES_WRITTEN).increment(stats.bytesWritten);
      tezCounters.findCounter(scheme, FileSystemCounter.READ_OPS).increment(stats.readOps);
      tezCounters.findCounter(scheme, FileSystemCounter.LARGE_READ_OPS).increment(stats.largeReadOps);
      tezCounters.findCounter(scheme, FileSystemCounter.WRITE_OPS).increment(stats.writeOps);
    }

    if (cpuTime >= 0 && userTime >= 0) {
      tezCounters.findCounter(LlapThreadLocalStatistics.LlapExecutorCounters.EXECUTOR_CPU_NS).increment(cpuTime);
      tezCounters.findCounter(LlapThreadLocalStatistics.LlapExecutorCounters.EXECUTOR_USER_NS).increment(userTime);
    }
  }

  public String toString(){
    return String.format("LlapThreadLocalStatistics: %s", schemeToThreadLocalStats.toString());
  }

  /**
   * Convenience class over Hadoop's FileSystem.Statistics.StatisticsData.
   * Unfortunately, neither the fields, nor the convenience methods (e.g. StatisticsData.add, StatisticsData.negate)
   * are available here as they are package protected, so we cannot reuse them.
   */
  public static class LlapFileSystemStatisticsData {
    long bytesRead;
    long bytesWritten;
    int readOps;
    int largeReadOps;
    int writeOps;

    public LlapFileSystemStatisticsData(FileSystem.Statistics.StatisticsData fsStats) {
      this.bytesRead = fsStats.getBytesRead();
      this.bytesWritten = fsStats.getBytesWritten();
      this.readOps = fsStats.getReadOps();
      this.largeReadOps = fsStats.getLargeReadOps();
      this.writeOps = fsStats.getWriteOps();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(" bytesRead: ").append(bytesRead);
      sb.append(" bytesWritten: ").append(bytesWritten);
      sb.append(" readOps: ").append(readOps);
      sb.append(" largeReadOps: ").append(largeReadOps);
      sb.append(" writeOps: ").append(writeOps);
      return sb.toString();
    }

    public LlapFileSystemStatisticsData merge(FileSystem.Statistics.StatisticsData other) {
      this.bytesRead += other.getBytesRead();
      this.bytesWritten += other.getBytesWritten();
      this.readOps += other.getReadOps();
      this.largeReadOps += other.getLargeReadOps();
      this.writeOps += other.getWriteOps();
      return this;
    }

    public LlapFileSystemStatisticsData subtract(LlapFileSystemStatisticsData other) {
      if (other == null){
        return this;
      }
      this.bytesRead -= other.bytesRead;
      this.bytesWritten -= other.bytesWritten;
      this.readOps -= other.readOps;
      this.largeReadOps -= other.largeReadOps;
      this.writeOps -= other.writeOps;
      return this;
    }
  }
}
