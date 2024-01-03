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
package org.apache.hadoop.hive.ql.exec.mr;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.PROXY;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.TaskQueue;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.shims.Utils;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class TestMapRedTask {

  @Test
  public void mrTask_updates_Metrics() throws IOException {

    Metrics mockMetrics = Mockito.mock(Metrics.class);

    MapRedTask mapRedTask = new MapRedTask();
    mapRedTask.updateTaskMetrics(mockMetrics);

    verify(mockMetrics, times(1)).incrementCounter(MetricsConstant.HIVE_MR_TASKS);
    verify(mockMetrics, never()).incrementCounter(MetricsConstant.HIVE_TEZ_TASKS);
  }

  @Test
  public void mrTaskSumbitViaChildWithImpersonation() throws IOException, LoginException {
    Utils.getUGI().setAuthenticationMethod(PROXY);

    Context ctx = Mockito.mock(Context.class);
    when(ctx.getLocalTmpPath()).thenReturn(new Path(System.getProperty("java.io.tmpdir")));

    TaskQueue taskQueue = new TaskQueue(ctx);

    QueryState queryState = new QueryState.Builder().build();
    HiveConf conf= queryState.getConf();
    conf.setBoolVar(HiveConf.ConfVars.SUBMIT_VIA_CHILD, true);

    MapredWork mrWork = new MapredWork();
    mrWork.setMapWork(Mockito.mock(MapWork.class));

    MapRedTask mrTask = Mockito.spy(new MapRedTask());
    mrTask.setWork(mrWork);

    mrTask.initialize(queryState, null, taskQueue, ctx);

    mrTask.jobExecHelper = Mockito.mock(HadoopJobExecHelper.class);
    when(mrTask.jobExecHelper.progressLocal(Mockito.any(Process.class), Mockito.anyString())).thenReturn(0);

    mrTask.execute();

    ArgumentCaptor<String[]> captor = ArgumentCaptor.forClass(String[].class);
    verify(mrTask).spawn(Mockito.anyString(), Mockito.anyString(), captor.capture());

    String expected = "HADOOP_PROXY_USER=" + Utils.getUGI().getUserName();
    Assert.assertTrue(Arrays.asList(captor.getValue()).contains(expected));
  }

}
