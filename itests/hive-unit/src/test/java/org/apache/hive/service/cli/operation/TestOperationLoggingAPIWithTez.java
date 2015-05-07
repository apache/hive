package org.apache.hive.service.cli.operation;

import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
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
      "Parsing command",
      "Parse Completed",
      "Starting Semantic Analysis",
      "Semantic Analysis Completed",
      "Starting command"
    };
    expectedLogsExecution = new String[]{
      "Executing on YARN cluster with App id",
      "Setting Tez DAG access"
    };
    expectedLogsPerformance = new String[]{
      "<PERFLOG method=compile from=org.apache.hadoop.hive.ql.Driver>",
      "<PERFLOG method=parse from=org.apache.hadoop.hive.ql.Driver>",
      "<PERFLOG method=Driver.run from=org.apache.hadoop.hive.ql.Driver>",
      "from=org.apache.hadoop.hive.ql.exec.tez.TezJobMonitor",
      "org.apache.tez.common.counters.DAGCounter",
      "NUM_SUCCEEDED_TASKS",
      "TOTAL_LAUNCHED_TASKS",
      "CPU_TIME_MILLIS"
    };
    hiveConf = new HiveConf();
    hiveConf.set(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL.varname, "verbose");
    // We need to set the below parameter to test performance level logging
    hiveConf.set("hive.ql.log.PerfLogger.level", "INFO,DRFA");
    // Change the engine to tez
    hiveConf.setVar(ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    // Set tez execution summary to false.
    hiveConf.setBoolVar(ConfVars.TEZ_EXEC_SUMMARY, false);
    miniHS2 = new MiniHS2(hiveConf, MiniClusterType.TEZ);
    confOverlay = new HashMap<String, String>();
    confOverlay.put(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    miniHS2.start(confOverlay);
  }
}
