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

package org.apache.hive.spark.client;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hive.spark.client.metrics.DataReadMethod;
import org.apache.hive.spark.client.metrics.InputMetrics;
import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.client.metrics.ShuffleReadMetrics;
import org.apache.hive.spark.client.metrics.ShuffleWriteMetrics;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Provides metrics collected for a submitted job.
 *
 * The collected metrics can be analysed at different levels of granularity:
 * - Global (all Spark jobs triggered by client job)
 * - Spark job
 * - Stage
 * - Task
 *
 * Only successful, non-speculative tasks are considered. Metrics are updated as tasks finish,
 * so snapshots can be retrieved before the whole job completes.
 */
@InterfaceAudience.Private
public class MetricsCollection {

  private final List<TaskInfo> taskMetrics = Lists.newArrayList();
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public Metrics getAllMetrics() {
    return aggregate(Predicates.<TaskInfo>alwaysTrue());
  }

  public Set<Integer> getJobIds() {
    Function<TaskInfo, Integer> fun = new Function<TaskInfo, Integer>() {
      @Override
      public Integer apply(TaskInfo input) {
        return input.jobId;
      }
    };
    return transform(Predicates.<TaskInfo>alwaysTrue(), fun);
  }

  public Metrics getJobMetrics(int jobId) {
    return aggregate(new JobFilter(jobId));
  }

  public Set<Integer> getStageIds(int jobId) {
    Function<TaskInfo, Integer> fun = new Function<TaskInfo, Integer>() {
      @Override
      public Integer apply(TaskInfo input) {
        return input.stageId;
      }
    };
    return transform(new JobFilter(jobId), fun);
  }

  public Metrics getStageMetrics(final int jobId, final int stageId) {
    return aggregate(new StageFilter(jobId, stageId));
  }

  public Set<Long> getTaskIds(int jobId, int stageId) {
    Function<TaskInfo, Long> fun = new Function<TaskInfo, Long>() {
      @Override
      public Long apply(TaskInfo input) {
        return input.taskId;
      }
    };
    return transform(new StageFilter(jobId, stageId), fun);
  }

  public Metrics getTaskMetrics(final int jobId, final int stageId, final long taskId) {
    Predicate<TaskInfo> filter = new Predicate<TaskInfo>() {
      @Override
      public boolean apply(TaskInfo input) {
        return jobId == input.jobId && stageId == input.stageId && taskId == input.taskId;
      }
    };
    lock.readLock().lock();
    try {
      Iterator<TaskInfo> it = Collections2.filter(taskMetrics, filter).iterator();
      if (it.hasNext()) {
        return it.next().metrics;
      } else {
        throw new NoSuchElementException("Task not found.");
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public void addMetrics(int jobId, int stageId, long taskId, Metrics metrics) {
    lock.writeLock().lock();
    try {
      taskMetrics.add(new TaskInfo(jobId, stageId, taskId, metrics));
    } finally {
      lock.writeLock().unlock();
    }
  }

  private <T> Set<T> transform(Predicate<TaskInfo> filter, Function<TaskInfo, T> fun) {
    lock.readLock().lock();
    try {
      Collection<TaskInfo> filtered = Collections2.filter(taskMetrics, filter);
      return Sets.newHashSet(Collections2.transform(filtered, fun));
    } finally {
      lock.readLock().unlock();
    }
  }

  private Metrics aggregate(Predicate<TaskInfo> filter) {
    lock.readLock().lock();
    try {
      // Task metrics.
      long executorDeserializeTime = 0L;
      long executorRunTime = 0L;
      long resultSize = 0L;
      long jvmGCTime = 0L;
      long resultSerializationTime = 0L;
      long memoryBytesSpilled = 0L;
      long diskBytesSpilled = 0L;

      // Input metrics.
      boolean hasInputMetrics = false;
      long bytesRead = 0L;

      // Shuffle read metrics.
      boolean hasShuffleReadMetrics = false;
      int remoteBlocksFetched = 0;
      int localBlocksFetched = 0;
      long fetchWaitTime = 0L;
      long remoteBytesRead = 0L;

      // Shuffle write metrics.
      long shuffleBytesWritten = 0L;
      long shuffleWriteTime = 0L;

      for (TaskInfo info : Collections2.filter(taskMetrics, filter)) {
        Metrics m = info.metrics;
        executorDeserializeTime += m.executorDeserializeTime;
        executorRunTime += m.executorRunTime;
        resultSize += m.resultSize;
        jvmGCTime += m.jvmGCTime;
        resultSerializationTime += m.resultSerializationTime;
        memoryBytesSpilled += m.memoryBytesSpilled;
        diskBytesSpilled += m.diskBytesSpilled;

        if (m.inputMetrics != null) {
          hasInputMetrics = true;
          bytesRead += m.inputMetrics.bytesRead;
        }

        if (m.shuffleReadMetrics != null) {
          hasShuffleReadMetrics = true;
          remoteBlocksFetched += m.shuffleReadMetrics.remoteBlocksFetched;
          localBlocksFetched += m.shuffleReadMetrics.localBlocksFetched;
          fetchWaitTime += m.shuffleReadMetrics.fetchWaitTime;
          remoteBytesRead += m.shuffleReadMetrics.remoteBytesRead;
        }

        if (m.shuffleWriteMetrics != null) {
          shuffleBytesWritten += m.shuffleWriteMetrics.shuffleBytesWritten;
          shuffleWriteTime += m.shuffleWriteMetrics.shuffleWriteTime;
        }
      }

      InputMetrics inputMetrics = null;
      if (hasInputMetrics) {
        inputMetrics = new InputMetrics(bytesRead);
      }

      ShuffleReadMetrics shuffleReadMetrics = null;
      if (hasShuffleReadMetrics) {
        shuffleReadMetrics = new ShuffleReadMetrics(
          remoteBlocksFetched,
          localBlocksFetched,
          fetchWaitTime,
          remoteBytesRead);
      }

      ShuffleWriteMetrics shuffleWriteMetrics = null;
      if (hasShuffleReadMetrics) {
        shuffleWriteMetrics = new ShuffleWriteMetrics(
          shuffleBytesWritten,
          shuffleWriteTime);
      }

      return new Metrics(
        executorDeserializeTime,
        executorRunTime,
        resultSize,
        jvmGCTime,
        resultSerializationTime,
        memoryBytesSpilled,
        diskBytesSpilled,
        inputMetrics,
        shuffleReadMetrics,
        shuffleWriteMetrics);
    } finally {
        lock.readLock().unlock();
    }
  }

  private static class TaskInfo {
    final int jobId;
    final int stageId;
    final long taskId;
    final Metrics metrics;

    TaskInfo(int jobId, int stageId, long taskId, Metrics metrics) {
      this.jobId = jobId;
      this.stageId = stageId;
      this.taskId = taskId;
      this.metrics = metrics;
    }

  }

  private static class JobFilter implements Predicate<TaskInfo> {

    private final int jobId;

    JobFilter(int jobId) {
      this.jobId = jobId;
    }

    @Override
    public boolean apply(TaskInfo input) {
      return jobId == input.jobId;
    }

  }

  private static class StageFilter implements Predicate<TaskInfo> {

    private final int jobId;
    private final int stageId;

    StageFilter(int jobId, int stageId) {
      this.jobId = jobId;
      this.stageId = stageId;
    }

    @Override
    public boolean apply(TaskInfo input) {
      return jobId == input.jobId && stageId == input.stageId;
    }

  }

}
