/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.parse.repl.metric;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.repl.ReplAck;
import org.apache.hadoop.hive.ql.exec.repl.ReplDumpWork;
import org.apache.hadoop.hive.ql.exec.repl.ReplLoadWork;
import org.apache.hadoop.hive.ql.parse.ExplainConfiguration;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.BootstrapDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.dump.metric.IncrementalDumpMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.BootstrapLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.load.metric.IncrementalLoadMetricCollector;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Progress;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.Status;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class TestReplicationMetricUpdateOnFailure {

  FileSystem fs;
  HiveConf conf;
  String TEST_PATH;
  
  @Rule
  public final TestName testName = new TestName();
  
  RuntimeException recoverableException = new RuntimeException();
  RuntimeException nonRecoverableException = new RuntimeException(ErrorMsg.REPL_FAILED_WITH_NON_RECOVERABLE_ERROR.getMsg());
  
  @Before
  public void setup() throws Exception {
    
    conf = new HiveConf();
    conf.set(HiveConf.ConfVars.HIVE_IN_TEST.varname, "false");
    conf.set(Constants.SCHEDULED_QUERY_SCHEDULENAME, "repl");
    conf.set(Constants.SCHEDULED_QUERY_EXECUTIONID, "1");
    
    final String tid = 
            TestReplicationMetricUpdateOnFailure.class.getCanonicalName().toLowerCase().replace('.','_')  
            + "_" + System.currentTimeMillis();
    TEST_PATH = System.getProperty("test.warehouse.dir", "/tmp") + Path.SEPARATOR + tid;
    Path testPath = new Path(TEST_PATH);
    fs = FileSystem.get(testPath.toUri(), conf);
    fs.mkdirs(testPath);
  }

  @Test
  public void testReplDumpFailure() throws Exception {
    String dumpDir = TEST_PATH + Path.SEPARATOR + testName.getMethodName();
    IncrementalDumpMetricCollector metricCollector =
            new IncrementalDumpMetricCollector(null, TEST_PATH, conf, 0L);
    ReplDumpWork replDumpWork = Mockito.mock(ReplDumpWork.class);
    Mockito.when(replDumpWork.getCurrentDumpPath()).thenReturn(new Path(dumpDir));
    Mockito.when(replDumpWork.getMetricCollector()).thenReturn(metricCollector);
    Mockito.when(replDumpWork.dataCopyIteratorsInitialized()).thenThrow(recoverableException, nonRecoverableException);
    Task replDumpTask = TaskFactory.get(replDumpWork, conf);

    String stageName = "REPL_DUMP";
    metricCollector.reportStageStart(stageName, new HashMap<>());
    Assert.assertThrows(RuntimeException.class, () -> replDumpTask.execute());
    performRecoverableChecks(stageName);

    metricCollector.reportStageStart(stageName, new HashMap<>());
    Assert.assertThrows(RuntimeException.class, () -> replDumpTask.execute());
    performNonRecoverableChecks(dumpDir, stageName);
  }
  
  @Test
  public void testReplDumpRecoverableMissingStage() throws Exception {
    String dumpDir = TEST_PATH + Path.SEPARATOR + testName.getMethodName();
    MetricCollector.getInstance().deinit();
    BootstrapDumpMetricCollector metricCollector =
            new BootstrapDumpMetricCollector(null, TEST_PATH, conf, 0L);
    ReplDumpWork replDumpWork = Mockito.mock(ReplDumpWork.class);
    Mockito.when(replDumpWork.getMetricCollector()).thenReturn(metricCollector);
    Mockito.when(replDumpWork.getCurrentDumpPath()).thenReturn(new Path(dumpDir));
    Mockito.when(replDumpWork.dataCopyIteratorsInitialized()).thenThrow(recoverableException);
    Task<ReplDumpWork> replDumpTask = TaskFactory.get(replDumpWork, conf);

    //ensure stages are missing initially and execute without reporting start metrics
    Assert.assertEquals(0, MetricCollector.getInstance().getMetrics().size());
    
    Assert.assertThrows(RuntimeException.class, () -> replDumpTask.execute());
    performRecoverableChecks("REPL_DUMP");
  }
  
  @Test
  public void testReplDumpNonRecoverableMissingStage() throws Exception {
    String dumpDir = TEST_PATH + Path.SEPARATOR + testName.getMethodName();
    MetricCollector.getInstance().deinit();
    IncrementalDumpMetricCollector metricCollector =
            new IncrementalDumpMetricCollector(null, TEST_PATH, conf, 0L);
    ReplDumpWork replDumpWork = Mockito.mock(ReplDumpWork.class);
    Mockito.when(replDumpWork.getCurrentDumpPath()).thenReturn(new Path(dumpDir));
    Mockito.when(replDumpWork.getMetricCollector()).thenReturn(metricCollector);
    Mockito.when(replDumpWork.dataCopyIteratorsInitialized()).thenThrow(nonRecoverableException);
    Task replDumpTask = TaskFactory.get(replDumpWork, conf);

    //ensure stages are missing initially and execute without reporting start metrics
    Assert.assertEquals(0, MetricCollector.getInstance().getMetrics().size());

    Assert.assertThrows(RuntimeException.class, () -> replDumpTask.execute());
    performNonRecoverableChecks(dumpDir, "REPL_DUMP");
  }

  @Test
  public void testReplLoadFailure() throws Exception {
    String dumpDir = TEST_PATH + Path.SEPARATOR + testName.getMethodName();
    MetricCollector.getInstance().deinit();
    IncrementalLoadMetricCollector metricCollector =
            new IncrementalLoadMetricCollector(null, TEST_PATH, 0, conf);
    ReplLoadWork replLoadWork = Mockito.mock(ReplLoadWork.class);
    Mockito.when(replLoadWork.getTargetDatabase()).thenReturn("dummy");
    Mockito.when(replLoadWork.getDumpDirectory()).thenReturn(
            new Path(dumpDir + Path.SEPARATOR + "test").toString());
    Mockito.when(replLoadWork.getMetricCollector()).thenReturn(metricCollector);
    Mockito.when(replLoadWork.getRootTask()).thenThrow(recoverableException, nonRecoverableException);
    Task replLoadTask = TaskFactory.get(replLoadWork, conf);
    
    String stageName = "REPL_LOAD";
    metricCollector.reportStageStart(stageName, new HashMap<>());
    Assert.assertThrows(RuntimeException.class, () -> replLoadTask.execute());
    performRecoverableChecks(stageName);

    metricCollector.reportStageStart(stageName, new HashMap<>());
    Assert.assertThrows(RuntimeException.class, () -> replLoadTask.execute());
    performNonRecoverableChecks(dumpDir, stageName);
  }

  @Test
  public void testReplLoadRecoverableMissingStage() throws Exception {
    String dumpDir = TEST_PATH + Path.SEPARATOR + testName.getMethodName();
    MetricCollector.getInstance().deinit();
    BootstrapLoadMetricCollector metricCollector = 
            new BootstrapLoadMetricCollector(null, TEST_PATH, 0, conf);
    ReplLoadWork replLoadWork = Mockito.mock(ReplLoadWork.class);
    Mockito.when(replLoadWork.getTargetDatabase()).thenReturn("dummy");
    Mockito.when(replLoadWork.getDumpDirectory()).thenReturn(
            new Path(dumpDir + Path.SEPARATOR + "test").toString());
    Mockito.when(replLoadWork.getMetricCollector()).thenReturn(metricCollector);
    Mockito.when(replLoadWork.getRootTask()).thenThrow(recoverableException);
    Task replLoadTask = TaskFactory.get(replLoadWork, conf);

    //ensure stages are missing initially and execute without reporting start metrics
    Assert.assertEquals(0, MetricCollector.getInstance().getMetrics().size());

    Assert.assertThrows(RuntimeException.class, () -> replLoadTask.execute());
    performRecoverableChecks("REPL_LOAD");
  }

  @Test
  public void testReplLoadNonRecoverableMissingStage() throws Exception {
    String dumpDir = TEST_PATH + Path.SEPARATOR + testName.getMethodName();
    MetricCollector.getInstance().deinit();
    IncrementalLoadMetricCollector metricCollector = 
            new IncrementalLoadMetricCollector(null, TEST_PATH, 0, conf);
    ReplLoadWork replLoadWork = Mockito.mock(ReplLoadWork.class);
    Mockito.when(replLoadWork.getTargetDatabase()).thenReturn("dummy");
    Mockito.when(replLoadWork.getDumpDirectory()).thenReturn(
            new Path(dumpDir + Path.SEPARATOR + "test").toString());
    Mockito.when(replLoadWork.getMetricCollector()).thenReturn(metricCollector);
    Mockito.when(replLoadWork.getRootTask()).thenThrow(nonRecoverableException);
    Task replLoadTask = TaskFactory.get(replLoadWork, conf);

    //ensure stages are missing initially and execute without reporting start metrics
    Assert.assertEquals(0, MetricCollector.getInstance().getMetrics().size());

    Assert.assertThrows(RuntimeException.class, () -> replLoadTask.execute());
    performNonRecoverableChecks(dumpDir, "REPL_LOAD");
  }

  /*
   * Check update on metrics upon intermediate task failures(not repl-dump / repl-load).
   * Here, DDLTask is used as the intermediate task, other task failures should behave in similar fashion.
   */
  @Test
  public void testDDLTaskFailure() throws Exception {
    
    //task-setup for DDL-Task
    DDLWork ddlWork = Mockito.mock(DDLWork.class);
    Context context = Mockito.mock(Context.class);
    Mockito.when(context.getExplainAnalyze()).thenReturn(ExplainConfiguration.AnalyzeState.ANALYZING);
    Mockito.when(ddlWork.isReplication()).thenReturn(true);
    String dumpDir = TEST_PATH + Path.SEPARATOR + testName.getMethodName();
    Mockito.when(ddlWork.getDumpDirectory()).thenReturn(dumpDir);
    Task<DDLWork> ddlTask = TaskFactory.get(ddlWork, conf);
    ddlTask.initialize(null, null, null, context);


    IncrementalLoadMetricCollector metricCollector = new
            IncrementalLoadMetricCollector(null, TEST_PATH, 1, conf);
    Mockito.when(ddlWork.getMetricCollector()).thenReturn(metricCollector);

    //setup for 2 runs - first recoverable and second non-recoverable
    Mockito.when(ddlWork.getDDLDesc()).thenThrow(recoverableException, nonRecoverableException);
    
    String stageName = "REPL_LOAD";
    
    //test recoverable error during DDL-Task
    metricCollector.reportStageStart(stageName, new HashMap<>());
    ddlTask.execute();
    performRecoverableChecks(stageName);
   
    //test non-recoverable error during DDL-Task
    metricCollector.reportStageStart(stageName, new HashMap<>());
    ddlTask.execute();
    performNonRecoverableChecks(dumpDir, stageName);
  }
  
  @Test
  public void testRecoverableDDLFailureWithStageMissing() throws Exception {

    //task-setup for DDL-Task
    DDLWork ddlWork = Mockito.mock(DDLWork.class);
    Context context = Mockito.mock(Context.class);
    Mockito.when(context.getExplainAnalyze()).thenReturn(ExplainConfiguration.AnalyzeState.ANALYZING);
    Mockito.when(ddlWork.isReplication()).thenReturn(true);
    String dumpDir = TEST_PATH + Path.SEPARATOR + testName.getMethodName();
    Mockito.when(ddlWork.getDumpDirectory()).thenReturn(dumpDir);
    Task<DDLWork> ddlTask = TaskFactory.get(ddlWork, conf);
    ddlTask.initialize(null, null, null, context);

    MetricCollector.getInstance().deinit();
    IncrementalLoadMetricCollector metricCollector = new
            IncrementalLoadMetricCollector(null, TEST_PATH, 1, conf);
    //ensure stages are missing initially and execute without reporting start metrics
    Assert.assertEquals(0, MetricCollector.getInstance().getMetrics().size());


    Map<String, Long> metricMap = new HashMap<>();
    Mockito.when(ddlWork.getMetricCollector()).thenReturn(metricCollector);
    Mockito.when(ddlWork.getDDLDesc()).thenThrow(recoverableException);

    //test recoverable error during DDL-Task
    ddlTask.execute();
    performRecoverableChecks( "REPL_LOAD");
  }

  @Test
  public void testNonRecoverableDDLFailureWithStageMissing() throws Exception {

    //task-setup for DDL-Task
    DDLWork ddlWork = Mockito.mock(DDLWork.class);
    Context context = Mockito.mock(Context.class);
    Mockito.when(context.getExplainAnalyze()).thenReturn(ExplainConfiguration.AnalyzeState.ANALYZING);
    Mockito.when(ddlWork.isReplication()).thenReturn(true);
    String dumpDir = TEST_PATH + Path.SEPARATOR + testName.getMethodName();
    Mockito.when(ddlWork.getDumpDirectory()).thenReturn(dumpDir);
    Task<DDLWork> ddlTask = TaskFactory.get(ddlWork, conf);
    ddlTask.initialize(null, null, null, context);
    
    MetricCollector.getInstance().deinit();
    IncrementalLoadMetricCollector metricCollector = new
            IncrementalLoadMetricCollector(null, TEST_PATH, 1, conf);
    //ensure stages are missing initially and execute without reporting start metrics
    Assert.assertEquals(0, MetricCollector.getInstance().getMetrics().size());

    Map<String, Long> metricMap = new HashMap<>();
    Mockito.when(ddlWork.getMetricCollector()).thenReturn(metricCollector);
    Mockito.when(ddlWork.getDDLDesc()).thenThrow(nonRecoverableException);

    //test non-recoverable error during DDL-Task, without initializing stage
    ddlTask.execute();
    performNonRecoverableChecks(dumpDir, "REPL_LOAD");
  }


  void performRecoverableChecks(String stageName){
    List<ReplicationMetric> metricList = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, metricList.size());
    ReplicationMetric updatedMetric = metricList.get(0);
    Progress updatedProgress = updatedMetric.getProgress();
    Assert.assertEquals(Status.FAILED, updatedProgress.getStatus());
    Assert.assertEquals(1, updatedProgress.getStages().size());
    Assert.assertEquals(Status.FAILED, updatedProgress.getStageByName(stageName).getStatus());
    Assert.assertNotEquals(0, updatedProgress.getStageByName(stageName).getEndTime());
  }

  void performNonRecoverableChecks(String dumpDir, String stageName) throws IOException {
    List<ReplicationMetric> metricList = MetricCollector.getInstance().getMetrics();
    Assert.assertEquals(1, metricList.size());
    ReplicationMetric updatedMetric = metricList.get(0);
    Progress updatedProgress = updatedMetric.getProgress();
    Assert.assertEquals(Status.FAILED_ADMIN, updatedProgress.getStatus());
    Assert.assertEquals(1, updatedProgress.getStages().size());
    Assert.assertEquals(Status.FAILED_ADMIN, updatedProgress.getStageByName(stageName).getStatus());
    Assert.assertNotEquals(0, updatedProgress.getStageByName(stageName).getEndTime());
    Path expectedNonRecoverablePath = new Path(new Path(dumpDir), ReplAck.NON_RECOVERABLE_MARKER.toString());
    Assert.assertTrue(fs.exists(expectedNonRecoverablePath));
    fs.delete(expectedNonRecoverablePath, true);
    MetricCollector.getInstance().deinit();
  }
}
