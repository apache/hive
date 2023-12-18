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
package org.apache.hadoop.hive.metastore.tools.schematool.hms;

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.SchemaToolCommandLine;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Set;

/** Generate and run script to create logs table in the SYS schema at the specified location. */
class SchemaToolTaskCreateLogsTable extends MetaStoreTask {
  private static final String RETENTION_PERIOD = "retentionPeriod";
  /** Path of the warehouse/compute logs directory. */
  private String logPath;
  private String retentionPeriod;

  @Override
  protected Set<String> usedCommandLineArguments() {
    return Sets.newHashSet("createLogsTable", RETENTION_PERIOD);
  }

  @Override
  public void execute(TaskContext context) throws HiveMetaException {
    SchemaToolCommandLine commandLine = context.getCommandLine();
    logPath = commandLine.getOptionValue("createLogsTable");
    retentionPeriod = commandLine.getOptionValue(RETENTION_PERIOD) == null ? "7d" : commandLine.getOptionValue(RETENTION_PERIOD);

    testConnectionToMetastore(context);

    System.out.println("Starting creation of logs table");

    File scriptFile = generateLogsTableScript();

    String initScriptDir = scriptFile.getParent();
    String initScriptFile = scriptFile.getName();

    try {
      System.out.println("Initialization script " + initScriptFile);
      if (commandLine.hasOption("dryRun")) {
        context.getScriptExecutor().execSql(initScriptDir, initScriptFile);
        System.out.println("Initialization script completed");
      }
    } catch (IOException e) {
      throw new HiveMetaException("Logs table creation FAILED!", e);
    }
  }

  private File generateLogsTableScript() throws HiveMetaException {
    try {
      File tmpFile = File.createTempFile("schematool", ".sql");
      tmpFile.deleteOnExit();
      FileWriter fstream = new FileWriter(tmpFile.getPath());
      try (BufferedWriter out = new BufferedWriter(fstream)) {
        out.write("USE SYS;" + System.getProperty("line.separator"));

        out.write("CREATE EXTERNAL TABLE logs");
        out.write(" (facility STRING, severity STRING,");
        out.write(" version STRING, ts TIMESTAMP, hostname STRING, app_name STRING,");
        out.write(" proc_id STRING, msg_id STRING, structured_data map<STRING,STRING>, msg BINARY,");
        out.write(" unmatched BINARY)");
        out.write(" PARTITIONED BY (dt DATE, ns STRING, app STRING)");
        out.write(" STORED BY 'org.apache.hadoop.hive.ql.log.syslog.SyslogStorageHandler'");
        out.write(" LOCATION '" + logPath + "'");
        out.write(" TBLPROPERTIES (\"partition.retention.period\"=\"" + retentionPeriod + "\");");
        out.write(System.getProperty("line.separator"));
      }

      return tmpFile;
    } catch (Exception err) {
      throw new HiveMetaException("Error generating logs table script", err);
    }
  }

}
