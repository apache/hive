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

package org.apache.hive.jdbc.beeline;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.jdbc.beeline.OptionsProcessor.PrintMode;
import sqlline.SqlLine;

public class HiveBeeline {

  // TODO: expose from the JDBC connection class
  private static final String URI_PREFIX = "jdbc:hive://";
  private static final String SQLLINE_CLASS = "sqlline.SqlLine";
  private static final String HIVE_JDBC_DRIVER = "org.apache.hadoop.hive.jdbc.HiveDriver";
  private static final String SQLLINE_SILENT = "--silent=true";
  private static final String SQLLINE_VERBOSE = "--verbose=true";
  private static final String SQLLINE_SCRIPT_CMD = "!run";
  private static final String URL_DB_MARKER = "/";
  private static final String URL_HIVE_CONF_MARKER = "?";
  private static final String URL_HIVE_VAR_MARKER = "#";
  private static final String URL_SESS_VAR_MARKER = ";";

  public static void main(String[] args) throws Exception {
    OptionsProcessor oproc = new OptionsProcessor();
    if (!oproc.processArgs(args)) {
      System.exit(1);
    }

    // assemble connection URL
    String jdbcURL = URI_PREFIX;
    if (oproc.getHost() != null) {
      // no, host name indicates an embbeded hive invocation
      jdbcURL += oproc.getHost() + ":" + oproc.getPort();
    }

    if (!oproc.getDatabase().isEmpty()) {
      jdbcURL += URL_DB_MARKER + oproc.getDatabase();
    }
    if (!oproc.getSessVars().isEmpty()) {
      jdbcURL += URL_SESS_VAR_MARKER + oproc.getSessVars();
    }
    if (!oproc.getHiveConfs().isEmpty()) {
      jdbcURL += URL_HIVE_CONF_MARKER + oproc.getHiveConfs();
    }
    if (!oproc.getHiveVars().isEmpty()) {
      jdbcURL += URL_HIVE_VAR_MARKER + oproc.getHiveVars();
    }

    // setup input file or string
    InputStream sqlLineInput = null;
    if (oproc.getFileName() != null) {
      String scriptCmd = SQLLINE_SCRIPT_CMD + " " + oproc.getFileName().trim() + "\n";
      sqlLineInput = new ByteArrayInputStream(scriptCmd.getBytes());
    } else if (oproc.getExecString() != null) {
      // process the string to make each stmt a separate line
      String execString = oproc.getExecString().trim();
      String execCommand = "";
      String command = "";
      for (String oneCmd : execString.split(";")) {
        if (StringUtils.endsWith(oneCmd, "\\")) {
          command += StringUtils.chop(oneCmd) + ";";
          continue;
        } else {
          command += oneCmd;
        }
        if (StringUtils.isBlank(command)) {
          continue;
        }
        execCommand += command + ";\n"; // stmt should end with ';' for sqlLine
        command = "";
      }
      sqlLineInput = new ByteArrayInputStream(execCommand.getBytes());
    }

    // setup SQLLine args
    List<String> argList = new ArrayList<String> ();
    argList.add("-u");
    argList.add(jdbcURL);
    argList.add("-d");
    argList.add(HIVE_JDBC_DRIVER); // TODO: make it configurable for HS or HS2
    if (oproc.getpMode() == PrintMode.SILENT) {
      argList.add(SQLLINE_SILENT);
    } else if (oproc.getpMode() == PrintMode.VERBOSE) {
      argList.add(SQLLINE_VERBOSE);
    }

     // Invoke sqlline
     SqlLine.mainWithInputRedirection(argList.toArray(new String[0]), sqlLineInput);
  }
}
