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
package org.apache.hadoop.hive.ql.exec.spark;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.spark.Statistic.SparkStatisticsBuilder;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.status.RemoteSparkJobMonitor;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobStatus;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.RemoteSparkJobStatus;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.spark.client.JobHandle.State;

import org.apache.spark.SparkException;

import org.junit.Assert;
import org.junit.Test;

public class TestSparkTask {

  @Test
  public void sparkTask_updates_Metrics() throws IOException {

    Metrics mockMetrics = mock(Metrics.class);

    SparkTask sparkTask = new SparkTask();
    sparkTask.updateTaskMetrics(mockMetrics);

    verify(mockMetrics, times(1)).incrementCounter(MetricsConstant.HIVE_SPARK_TASKS);
    verify(mockMetrics, never()).incrementCounter(MetricsConstant.HIVE_TEZ_TASKS);
    verify(mockMetrics, never()).incrementCounter(MetricsConstant.HIVE_MR_TASKS);
  }

  @Test
  public void removeEmptySparkTask() {
    SparkTask grandpa = new SparkTask();
    SparkWork grandpaWork = new SparkWork("grandpa");
    grandpaWork.add(new MapWork());
    grandpa.setWork(grandpaWork);

    SparkTask parent = new SparkTask();
    SparkWork parentWork = new SparkWork("parent");
    parentWork.add(new MapWork());
    parent.setWork(parentWork);

    SparkTask child1 = new SparkTask();
    SparkWork childWork1 = new SparkWork("child1");
    childWork1.add(new MapWork());
    child1.setWork(childWork1);


    grandpa.addDependentTask(parent);
    parent.addDependentTask(child1);

    Assert.assertEquals(grandpa.getChildTasks().size(), 1);
    Assert.assertEquals(child1.getParentTasks().size(), 1);
    if (isEmptySparkWork(parent.getWork())) {
      SparkUtilities.removeEmptySparkTask(parent);
    }

    Assert.assertEquals(grandpa.getChildTasks().size(), 0);
    Assert.assertEquals(child1.getParentTasks().size(), 0);
  }

  @Test
  public void testRemoteSparkCancel() {
    RemoteSparkJobStatus jobSts = mock(RemoteSparkJobStatus.class);
    when(jobSts.getRemoteJobState()).thenReturn(State.CANCELLED);
    when(jobSts.isRemoteActive()).thenReturn(true);
    HiveConf hiveConf = new HiveConf();
    SessionState.start(hiveConf);
    RemoteSparkJobMonitor remoteSparkJobMonitor = new RemoteSparkJobMonitor(hiveConf, jobSts);
    Assert.assertEquals(remoteSparkJobMonitor.startMonitor(), 3);
  }

  @Test
  public void testSparkStatisticsToString() {
    SparkStatisticsBuilder statsBuilder = new SparkStatisticsBuilder();
    statsBuilder.add("TEST", "stat1", "1");
    statsBuilder.add("TEST", "stat2", "1");
    String statsString = SparkTask.sparkStatisticsToString(statsBuilder.build(), 10);

    Assert.assertTrue(statsString.contains("10"));
    Assert.assertTrue(statsString.contains("TEST"));
    Assert.assertTrue(statsString.contains("stat1"));
    Assert.assertTrue(statsString.contains("stat2"));
    Assert.assertTrue(statsString.contains("1"));
  }

  @Test
  public void testSetSparkExceptionWithJobError() {
    SparkTask sparkTask = new SparkTask();
    SparkJobStatus mockSparkJobStatus = mock(SparkJobStatus.class);

    ExecutionException ee = new ExecutionException("Exception thrown by job",
            new SparkException("Job aborted due to stage failure: Not a task or OOM error"));

    when(mockSparkJobStatus.getSparkJobException()).thenReturn(ee);

    sparkTask.setSparkException(mockSparkJobStatus, 3);

    Assert.assertTrue(sparkTask.getException() instanceof HiveException);
    Assert.assertEquals(((HiveException) sparkTask.getException()).getCanonicalErrorMsg(),
            ErrorMsg.SPARK_JOB_RUNTIME_ERROR);
    Assert.assertTrue(sparkTask.getException().getMessage().contains("Not a task or OOM error"));
  }

  @Test
  public void testSetSparkExceptionWithTimeoutError() {
    SparkTask sparkTask = new SparkTask();
    SparkJobStatus mockSparkJobStatus = mock(SparkJobStatus.class);
    when(mockSparkJobStatus.getMonitorError()).thenReturn(new HiveException(ErrorMsg
            .SPARK_JOB_MONITOR_TIMEOUT, Long.toString(60)));

    sparkTask.setSparkException(mockSparkJobStatus, 3);

    Assert.assertTrue(sparkTask.getException() instanceof HiveException);
    Assert.assertEquals(((HiveException) sparkTask.getException()).getCanonicalErrorMsg(),
            ErrorMsg.SPARK_JOB_MONITOR_TIMEOUT);
    Assert.assertTrue(sparkTask.getException().getMessage().contains("60s"));
  }

  @Test
  public void testSetSparkExceptionWithOOMError() {
    SparkTask sparkTask = new SparkTask();
    SparkJobStatus mockSparkJobStatus = mock(SparkJobStatus.class);

    ExecutionException jobError = new ExecutionException(
            new SparkException("Container killed by YARN for exceeding memory limits"));
    when(mockSparkJobStatus.getSparkJobException()).thenReturn(jobError);

    sparkTask.setSparkException(mockSparkJobStatus, 3);

    Assert.assertTrue(sparkTask.getException() instanceof HiveException);
    Assert.assertEquals(((HiveException) sparkTask.getException()).getCanonicalErrorMsg(),
            ErrorMsg.SPARK_RUNTIME_OOM);
  }

  @Test
  public void testSparkExceptionAndMonitorError() {
    SparkTask sparkTask = new SparkTask();
    SparkJobStatus mockSparkJobStatus = mock(SparkJobStatus.class);
    when(mockSparkJobStatus.getMonitorError()).thenReturn(new RuntimeException());
    when(mockSparkJobStatus.getSparkJobException()).thenReturn(
            new ExecutionException(new SparkException("")));

    sparkTask.setSparkException(mockSparkJobStatus, 3);

    Assert.assertTrue(sparkTask.getException() instanceof HiveException);
    Assert.assertEquals(((HiveException) sparkTask.getException()).getCanonicalErrorMsg(),
            ErrorMsg.SPARK_JOB_RUNTIME_ERROR);
  }

  @Test
  public void testHandleInterruptedException() throws Exception {
    HiveConf hiveConf = new HiveConf();

    SparkTask sparkTask = new SparkTask();
    sparkTask.setWork(mock(SparkWork.class));

    DriverContext mockDriverContext = mock(DriverContext.class);

    QueryState mockQueryState = mock(QueryState.class);
    when(mockQueryState.getConf()).thenReturn(hiveConf);

    sparkTask.initialize(mockQueryState, null, mockDriverContext, null);

    SparkJobStatus mockSparkJobStatus = mock(SparkJobStatus.class);
    when(mockSparkJobStatus.getMonitorError()).thenReturn(new InterruptedException());

    SparkSession mockSparkSession = mock(SparkSession.class);
    SparkJobRef mockSparkJobRef = mock(SparkJobRef.class);

    when(mockSparkJobRef.monitorJob()).thenReturn(2);
    when(mockSparkJobRef.getSparkJobStatus()).thenReturn(mockSparkJobStatus);
    when(mockSparkSession.submit(any(), any())).thenReturn(mockSparkJobRef);

    SessionState.start(hiveConf);
    SessionState.get().setSparkSession(mockSparkSession);

    sparkTask.execute(mockDriverContext);

    verify(mockSparkJobRef, atLeastOnce()).cancelJob();

    when(mockSparkJobStatus.getMonitorError()).thenReturn(
            new HiveException(new InterruptedException()));

    sparkTask.execute(mockDriverContext);

    verify(mockSparkJobRef, atLeastOnce()).cancelJob();
  }

  private boolean isEmptySparkWork(SparkWork sparkWork) {
    List<BaseWork> allWorks = sparkWork.getAllWork();
    boolean allWorksIsEmtpy = true;
    for (BaseWork work : allWorks) {
      if (work.getAllOperators().size() > 0) {
        allWorksIsEmtpy = false;
        break;
      }
    }
    return allWorksIsEmtpy;
  }
}
