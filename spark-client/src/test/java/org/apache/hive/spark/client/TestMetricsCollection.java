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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.apache.hive.spark.client.metrics.DataReadMethod;
import org.apache.hive.spark.client.metrics.InputMetrics;
import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.client.metrics.ShuffleReadMetrics;
import org.apache.hive.spark.client.metrics.ShuffleWriteMetrics;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class TestMetricsCollection {

  @Test
  public void testMetricsAggregation() {
    MetricsCollection collection = new MetricsCollection();
    // 2 jobs, 2 stages per job, 2 tasks per stage.
    for (int i : Arrays.asList(1, 2)) {
      for (int j : Arrays.asList(1, 2)) {
        for (long k : Arrays.asList(1L, 2L)) {
          collection.addMetrics(i, j, k, makeMetrics(i, j, k));
        }
      }
    }

    assertEquals(ImmutableSet.of(1, 2), collection.getJobIds());
    assertEquals(ImmutableSet.of(1, 2), collection.getStageIds(1));
    assertEquals(ImmutableSet.of(1L, 2L), collection.getTaskIds(1, 1));

    Metrics task112 = collection.getTaskMetrics(1, 1, 2);
    checkMetrics(task112, taskValue(1, 1, 2));

    Metrics stage21 = collection.getStageMetrics(2, 1);
    checkMetrics(stage21, stageValue(2, 1, 2));

    Metrics job1 = collection.getJobMetrics(1);
    checkMetrics(job1, jobValue(1, 2, 2));

    Metrics global = collection.getAllMetrics();
    checkMetrics(global, globalValue(2, 2, 2));
  }

  @Test
  public void testOptionalMetrics() {
    long value = taskValue(1, 1, 1L);
    Metrics metrics = new Metrics(value, value, value, value, value, value, value,
        null, null, null);

    MetricsCollection collection = new MetricsCollection();
    for (int i : Arrays.asList(1, 2)) {
      collection.addMetrics(i, 1, 1, metrics);
    }

    Metrics global = collection.getAllMetrics();
    assertNull(global.inputMetrics);
    assertNull(global.shuffleReadMetrics);
    assertNull(global.shuffleWriteMetrics);

    collection.addMetrics(3, 1, 1, makeMetrics(3, 1, 1));

    Metrics global2 = collection.getAllMetrics();
    assertNotNull(global2.inputMetrics);
    assertEquals(taskValue(3, 1, 1), global2.inputMetrics.bytesRead);

    assertNotNull(global2.shuffleReadMetrics);
    assertNotNull(global2.shuffleWriteMetrics);
  }

  @Test
  public void testInputReadMethodAggregation() {
    MetricsCollection collection = new MetricsCollection();

    long value = taskValue(1, 1, 1);
    Metrics metrics1 = new Metrics(value, value, value, value, value, value, value,
      new InputMetrics(DataReadMethod.Memory, value), null, null);
    Metrics metrics2 = new Metrics(value, value, value, value, value, value, value,
      new InputMetrics(DataReadMethod.Disk, value), null, null);

    collection.addMetrics(1, 1, 1, metrics1);
    collection.addMetrics(1, 1, 2, metrics2);

    Metrics global = collection.getAllMetrics();
    assertNotNull(global.inputMetrics);
    assertEquals(DataReadMethod.Multiple, global.inputMetrics.readMethod);
  }

  private Metrics makeMetrics(int jobId, int stageId, long taskId) {
    long value = 1000000 * jobId + 1000 * stageId + taskId;
    return new Metrics(value, value, value, value, value, value, value,
      new InputMetrics(DataReadMethod.Memory, value),
      new ShuffleReadMetrics((int) value, (int) value, value, value),
      new ShuffleWriteMetrics(value, value));
  }

  /**
   * The metric values will all be the same. This makes it easy to calculate the aggregated values
   * of jobs and stages without fancy math.
   */
  private long taskValue(int jobId, int stageId, long taskId) {
    return 1000000 * jobId + 1000 * stageId + taskId;
  }

  private long stageValue(int jobId, int stageId, int taskCount) {
    long value = 0;
    for (int i = 1; i <= taskCount; i++) {
      value += taskValue(jobId, stageId, i);
    }
    return value;
  }

  private long jobValue(int jobId, int stageCount, int tasksPerStage) {
    long value = 0;
    for (int i = 1; i <= stageCount; i++) {
      value += stageValue(jobId, i, tasksPerStage);
    }
    return value;
  }

  private long globalValue(int jobCount, int stagesPerJob, int tasksPerStage) {
    long value = 0;
    for (int i = 1; i <= jobCount; i++) {
      value += jobValue(i, stagesPerJob, tasksPerStage);
    }
    return value;
  }

  private void checkMetrics(Metrics metrics, long expected) {
    assertEquals(expected, metrics.executorDeserializeTime);
    assertEquals(expected, metrics.executorRunTime);
    assertEquals(expected, metrics.resultSize);
    assertEquals(expected, metrics.jvmGCTime);
    assertEquals(expected, metrics.resultSerializationTime);
    assertEquals(expected, metrics.memoryBytesSpilled);
    assertEquals(expected, metrics.diskBytesSpilled);

    assertEquals(DataReadMethod.Memory, metrics.inputMetrics.readMethod);
    assertEquals(expected, metrics.inputMetrics.bytesRead);

    assertEquals(expected, metrics.shuffleReadMetrics.remoteBlocksFetched);
    assertEquals(expected, metrics.shuffleReadMetrics.localBlocksFetched);
    assertEquals(expected, metrics.shuffleReadMetrics.fetchWaitTime);
    assertEquals(expected, metrics.shuffleReadMetrics.remoteBytesRead);

    assertEquals(expected, metrics.shuffleWriteMetrics.shuffleBytesWritten);
    assertEquals(expected, metrics.shuffleWriteMetrics.shuffleWriteTime);
  }

}
