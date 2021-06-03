/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.cli.operation;

import java.util.HashMap;

import org.apache.hadoop.hive.UtilsForTest;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.jdbc.miniHS2.MiniHS2.MiniClusterType;
import org.junit.BeforeClass;

/**
 * TestOperationLoggingAPIWithTez
 * Test the FetchResults of TFetchType.LOG in thrift level in Tez mode.
 */
public class TestOperationLoggingAPIWithTez extends OperationLoggingAPITestBase {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    tableName = "testOperationLoggingAPIWithTez_table";
    expectedLogsVerbose = new String[]{
      "Starting Semantic Analysis"
    };
    expectedLogsExecution = new String[]{
      "Compiling command",
      "Completed compiling command",
      "Executing command",
      "Completed executing command",
      "Semantic Analysis Completed",
      "Executing on YARN cluster with App id",
      "Setting Tez DAG access"
    };
    expectedLogsPerformance = new String[]{
      "<PERFLOG method=compile from=org.apache.hadoop.hive.ql.Driver>",
      "<PERFLOG method=parse from=org.apache.hadoop.hive.ql.Driver>",
      "from=org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor",
      "org.apache.tez.common.counters.DAGCounter",
      "NUM_SUCCEEDED_TASKS",
      "TOTAL_LAUNCHED_TASKS",
      "CPU_MILLISECONDS"
    };
    hiveConf = UtilsForTest.getHiveOnTezConfFromDir("../../data/conf/tez/");
    hiveConf.set(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL.varname, "verbose");
    // Set tez execution summary to false.
    hiveConf.setBoolVar(ConfVars.TEZ_EXEC_SUMMARY, false);
    miniHS2 = new MiniHS2(hiveConf, MiniClusterType.TEZ);
    confOverlay = new HashMap<String, String>();
    confOverlay.put(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    miniHS2.start(confOverlay);
  }
}
