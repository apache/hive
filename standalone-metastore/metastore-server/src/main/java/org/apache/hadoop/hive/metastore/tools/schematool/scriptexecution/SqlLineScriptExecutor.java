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
package org.apache.hadoop.hive.metastore.tools.schematool.scriptexecution;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.hive.metastore.tools.schematool.CommandBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sqlline.SqlLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

/**
 * {@link SqlLine} based {@link ScriptExecutor} implementation. Able to execute the given scripts using {@link SqlLine} internally.
 * Can be used to execute scripts against the HMS databse.
 */
public class SqlLineScriptExecutor implements ScriptExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(SqlLineScriptExecutor.class);

  private final CommandBuilder builder;

  @Override
  public void execSql(String scriptPath) throws IOException {
    // run the script using SqlLine
    SqlLine sqlLine = new SqlLine();
    ByteArrayOutputStream outputForLog = null;
    if (!builder.isVerbose()) {
      OutputStream out;
      if (LOG.isDebugEnabled()) {
        out = outputForLog = new ByteArrayOutputStream();
      } else {
        out = new NullOutputStream();
      }
      sqlLine.setOutputStream(new PrintStream(out));
      System.setProperty("sqlline.silent", "true");
    }
    LOG.info("Going to run command <" + builder.buildToLog(scriptPath) + ">");
    SqlLine.Status status = sqlLine.begin(builder.buildToRun(scriptPath), null, false);
    if (LOG.isDebugEnabled() && outputForLog != null) {
      LOG.debug("Received following output from Sqlline:");
      LOG.debug(outputForLog.toString("UTF-8"));
    }
    if (status != SqlLine.Status.OK) {
      throw new IOException("Schema script failed, errorcode " + status);
    }
  }

  @Override
  public void execSql(String scriptDir, String scriptFile) throws IOException {
    execSql(scriptDir + File.separatorChar + scriptFile);
  }

  public SqlLineScriptExecutor(CommandBuilder builder) {
    this.builder = builder;
  }
}
