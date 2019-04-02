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

package org.apache.hadoop.hive.metastore.tools.schematool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.hive.metastore.HiveMetaException;

/** Generate and run script to create logs table in the SYS schema at the specified location. */
public class SchemaToolTaskCreateLogsTable extends SchemaToolTask {
  /** Path of the warehouse/compute logs directory. */
  private String logPath;

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    logPath = cl.getOptionValue("createLogsTable");
  }

  @Override
  void execute() throws HiveMetaException {
    schemaTool.testConnectionToMetastore();

    System.out.println("Starting creation of logs table");

    File scriptFile = generateLogsTableScript();

    String initScriptDir = scriptFile.getParent();
    String initScriptFile = scriptFile.getName();

    try {
      System.out.println("Initialization script " + initScriptFile);
      if (!schemaTool.isDryRun()) {
        schemaTool.execSql(initScriptDir, initScriptFile);
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
        out.write(" (timeMillis TIMESTAMP, thread STRING, level STRING, loggerName STRING,");
        out.write(" message STRING, host STRING, component_id STRING, container_id STRING,");
        out.write(" tag STRING, unmatched_line STRING)");
        out.write(" PARTITIONED BY (dt DATE, ns STRING, app STRING)");
        out.write(" ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'");
        out.write(" WITH SERDEPROPERTIES (\"timestamp.formats\"=\"millis\")");
        out.write(" LOCATION '" + logPath + "'");
        out.write(" TBLPROPERTIES (\"partition.retention.period\"=\"7d\");");
        out.write(System.getProperty("line.separator"));
      }

      return tmpFile;
    } catch (Exception err) {
      throw new HiveMetaException("Error generating logs table script", err);
    }
  }
}
