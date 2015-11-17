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
package org.apache.hive.jdbc.miniHS2;

import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.MetricsReporting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.SessionHandle;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests HiveServer2 metrics.
 */
public class TestHs2Metrics {

  private static MiniHS2 miniHS2;
  private static Map<String, String> confOverlay;
  private static File jsonReportFile;

  //Check metrics during semantic analysis.
  public static class MetricCheckingHook implements HiveSemanticAnalyzerHook {
    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context,
      ASTNode ast) throws SemanticException {
      try {
        //Pre-analyze hook is fired in the middle of these calls
        MetricsTestUtils.verifyMetricFile(jsonReportFile, MetricsTestUtils.COUNTER, "active_calls_api_semanticAnalyze", 1);
        MetricsTestUtils.verifyMetricFile(jsonReportFile, MetricsTestUtils.COUNTER, "active_calls_api_compile", 1);
        MetricsTestUtils.verifyMetricFile(jsonReportFile, MetricsTestUtils.COUNTER, "active_calls_api_hs2_operation_RUNNING", 1);
      } catch (Exception e) {
        throw new SemanticException("metrics verification failed", e);
      }
      return ast;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {
    }
  }

  @BeforeClass
  public static void setup() throws Exception {
    miniHS2 = new MiniHS2(new HiveConf());
    confOverlay = new HashMap<String, String>();
    confOverlay.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    confOverlay.put(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname, MetricCheckingHook.class.getName());
    miniHS2.start(confOverlay);

    //for Metrics.  MiniHS2 init code-path doesn't go through HiveServer2.startHiveServer2().
    File workDir = new File(System.getProperty("test.tmp.dir"));
    jsonReportFile = new File(workDir, "json_reporting");
    jsonReportFile.delete();
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, MetricsReporting.JSON_FILE.name() + "," + MetricsReporting.JMX.name());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_LOCATION, jsonReportFile.toString());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_INTERVAL, "100ms");
    MetricsFactory.init(conf);
  }

  @Test
  public void testMetrics() throws Exception {
    String tableName = "testMetrics";
    CLIServiceClient serviceClient = miniHS2.getServiceClient();
    SessionHandle sessHandle = serviceClient.openSession("foo", "bar");

    //Block on semantic analysis to check 'active_calls'
    serviceClient.executeStatement(sessHandle, "CREATE TABLE " + tableName + " (id INT)", confOverlay);
    Thread.sleep(2000);

    //check that all calls were recorded.
    MetricsTestUtils.verifyMetricFile(jsonReportFile, MetricsTestUtils.TIMER, "api_hs2_operation_INITIALIZED", 1);
    MetricsTestUtils.verifyMetricFile(jsonReportFile, MetricsTestUtils.TIMER, "api_hs2_operation_PENDING", 1);
    MetricsTestUtils.verifyMetricFile(jsonReportFile, MetricsTestUtils.TIMER, "api_hs2_operation_RUNNING", 1);
    MetricsTestUtils.verifyMetricFile(jsonReportFile, MetricsTestUtils.COUNTER, "hs2_completed_operation_FINISHED", 1);
    MetricsTestUtils.verifyMetricFile(jsonReportFile, MetricsTestUtils.TIMER, "api_Driver.run", 1);

    //but there should be no more active calls.
    MetricsTestUtils.verifyMetricFile(jsonReportFile, MetricsTestUtils.COUNTER, "active_calls_api_semanticAnalyze", 0);
    MetricsTestUtils.verifyMetricFile(jsonReportFile, MetricsTestUtils.COUNTER, "active_calls_api_compile", 0);
    MetricsTestUtils.verifyMetricFile(jsonReportFile, MetricsTestUtils.COUNTER, "active_calls_api_hs2_operation_RUNNING", 0);

  }

}
