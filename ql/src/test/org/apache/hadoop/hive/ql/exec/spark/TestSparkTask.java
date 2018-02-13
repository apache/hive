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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.spark.status.RemoteSparkJobMonitor;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.RemoteSparkJobStatus;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hive.spark.client.JobHandle.State;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class TestSparkTask {

  @Test
  public void sparkTask_updates_Metrics() throws IOException {

    Metrics mockMetrics = Mockito.mock(Metrics.class);

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
    RemoteSparkJobStatus jobSts = Mockito.mock(RemoteSparkJobStatus.class);
    when(jobSts.getRemoteJobState()).thenReturn(State.CANCELLED);
    when(jobSts.isRemoteActive()).thenReturn(true);
    HiveConf hiveConf = new HiveConf();
    RemoteSparkJobMonitor remoteSparkJobMonitor = new RemoteSparkJobMonitor(hiveConf, jobSts);
    Assert.assertEquals(remoteSparkJobMonitor.startMonitor(), 3);
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
