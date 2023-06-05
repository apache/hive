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
package org.apache.hive.beeline.schematool.tasks;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.hive.metastore.tools.schematool.CommandBuilder;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParser;
import org.apache.hadoop.hive.metastore.tools.schematool.scriptexecution.ScriptExecutor;
import org.apache.hive.beeline.BeeLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

/**
 * {@link BeeLine} based {@link ScriptExecutor} implementation. Able to execute the given scripts using {@link BeeLine} internally.
 * Can be used to execute scripts against the Hive schema.
 */
class BeelineScriptExecutor implements ScriptExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(BeelineScriptExecutor.class);
  private static final String LINE_SEPARATOR = "line.separator";

  private final NestedScriptParser dbCommandParser;
  private final CommandBuilder commandBuilder;

  @Override
  public void execSql(String scriptDir, String sqlScriptFile) throws IOException {
    // expand the nested script
    // If the metaDbType is set, this is setting up the information
    // schema in Hive. That specifically means that the sql commands need
    // to be adjusted for the underlying RDBMS (correct quotation
    // strings, etc).
    String sqlCommands = dbCommandParser.buildCommand(scriptDir, sqlScriptFile, true);
    File tmpFile = File.createTempFile("schematool", ".sql");
    tmpFile.deleteOnExit();

    // write out the buffer into a file. Add beeline commands for autocommit and close
    FileWriter fstream = new FileWriter(tmpFile.getPath());
    try (BufferedWriter out = new BufferedWriter(fstream)) {
      if (!commandBuilder.getConnectionInfo().getDbType().equalsIgnoreCase(HiveSchemaHelper.DB_HIVE)) {
        out.write("!autocommit off" + System.getProperty(LINE_SEPARATOR));
        out.write(sqlCommands);
        out.write("!commit" + System.getProperty(LINE_SEPARATOR));
      } else {
        out.write("!autocommit on" + System.getProperty(LINE_SEPARATOR));
        out.write(sqlCommands);
      }
      out.write("!closeall" + System.getProperty(LINE_SEPARATOR));
    }
    execSql(tmpFile.getPath());
  }

  public void execSql(String sqlScriptFile) throws IOException {
    // run the script using Beeline
    try (BeeLine beeLine = new BeeLine()) {
      if (!commandBuilder.isVerbose()) {
        beeLine.setOutputStream(new PrintStream(NullOutputStream.NULL_OUTPUT_STREAM));
        beeLine.getOpts().setSilent(true);
      }
      beeLine.getOpts().setAllowMultiLineCommand(false);
      beeLine.getOpts().setIsolation(TRANSACTION_READ_COMMITTED);
      // We can be pretty sure that an entire line can be processed as a single command since
      // we always add a line separator at the end while calling dbCommandParser.buildCommand.
      beeLine.getOpts().setEntireLineAsCommand(true);
      LOG.debug("Going to run command <" + commandBuilder.buildToLog(sqlScriptFile) + ">");
      int status = beeLine.begin(commandBuilder.buildToRun(sqlScriptFile), null, false);
      if (status != 0) {
        throw new IOException("Schema script failed, errorcode " + status);
      }
    }
  }

  public BeelineScriptExecutor(NestedScriptParser dbCommandParser, CommandBuilder commandBuilder) {
    this.dbCommandParser = dbCommandParser;
    this.commandBuilder = commandBuilder;
  }
}
