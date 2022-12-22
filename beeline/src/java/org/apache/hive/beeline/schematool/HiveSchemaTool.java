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
package org.apache.hive.beeline.schematool;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.MetaStoreConnectionInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.NestedScriptParser;
import org.apache.hive.beeline.BeeLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

public class HiveSchemaTool extends MetastoreSchemaTool {
  private static final Logger LOG = LoggerFactory.getLogger(HiveSchemaTool.class.getName());


  @Override
  protected NestedScriptParser getDbCommandParser(String dbType, String metaDbType) {
    return HiveSchemaHelper.getDbCommandParser(dbType, dbOpts, userName, passWord, conf,
        metaDbType, false);
  }

  @Override
  protected MetaStoreConnectionInfo getConnectionInfo(boolean printInfo) {
    return new MetaStoreConnectionInfo(userName, passWord, url, driver, printInfo, conf,
        dbType, metaDbType);
  }

  /***
   * Run beeline with the given metastore script. Flatten the nested scripts
   * into single file.
   */
  @Override
  protected void execSql(String scriptDir, String scriptFile)
      throws IOException, HiveMetaException {
    NestedScriptParser dbCommandParser = getDbCommandParser(dbType, metaDbType);

    // expand the nested script
    // If the metaDbType is set, this is setting up the information
    // schema in Hive. That specifically means that the sql commands need
    // to be adjusted for the underlying RDBMS (correct quotation
    // strings, etc).
    String sqlCommands = dbCommandParser.buildCommand(scriptDir, scriptFile, metaDbType != null);
    File tmpFile = File.createTempFile("schematool", ".sql");
    tmpFile.deleteOnExit();

    // write out the buffer into a file. Add beeline commands for autocommit and close
    FileWriter fstream = new FileWriter(tmpFile.getPath());
    BufferedWriter out = new BufferedWriter(fstream);
    if (!dbType.equalsIgnoreCase(HiveSchemaHelper.DB_HIVE)) {
      out.write("!autocommit off" + System.getProperty("line.separator"));
      out.write(sqlCommands);
      out.write("!commit" + System.getProperty("line.separator"));
    } else {
      out.write("!autocommit on" + System.getProperty("line.separator"));
      out.write(sqlCommands);
    }
    out.write("!closeall" + System.getProperty("line.separator"));
    out.close();
    execSql(tmpFile.getPath());
  }

  @Override
  protected void execSql(String sqlScriptFile) throws IOException {
    CommandBuilder builder = new HiveSchemaToolCommandBuilder(conf, url, driver,
        userName, passWord, sqlScriptFile);

    // run the script using Beeline
    try (BeeLine beeLine = new BeeLine()) {
      if (!verbose) {
        beeLine.setOutputStream(new PrintStream(new NullOutputStream()));
        beeLine.getOpts().setSilent(true);
      }
      beeLine.getOpts().setAllowMultiLineCommand(false);
      beeLine.getOpts().setIsolation("TRANSACTION_READ_COMMITTED");
      // We can be pretty sure that an entire line can be processed as a single command since
      // we always add a line separator at the end while calling dbCommandParser.buildCommand.
      beeLine.getOpts().setEntireLineAsCommand(true);
      LOG.debug("Going to run command <" + builder.buildToLog() + ">");
      int status = beeLine.begin(builder.buildToRun(), null, false);
      if (status != 0) {
        throw new IOException("Schema script failed, errorcode " + status);
      }
    }
  }

  static class HiveSchemaToolCommandBuilder extends MetastoreSchemaTool.CommandBuilder {

    HiveSchemaToolCommandBuilder(Configuration conf, String url, String driver, String userName,
                                 String password, String sqlScriptFile) throws IOException {
      super(conf, url, driver, userName, password, sqlScriptFile);
    }

    @Override
    protected String[] argsWith(String password) {
      return new String[]
          {
            "-u", url,
            "-d", driver,
            "-n", userName,
            "-p", password,
            "-f", sqlScriptFile
          };
    }
  }

  public static void main(String[] args) {
    MetastoreSchemaTool tool = new HiveSchemaTool();
    OptionGroup additionalGroup = new OptionGroup();
    Option metaDbTypeOpt = OptionBuilder.withArgName("metaDatabaseType")
        .hasArgs().withDescription("Used only if upgrading the system catalog for hive")
        .create("metaDbType");
    additionalGroup.addOption(metaDbTypeOpt);
    System.setProperty(MetastoreConf.ConfVars.SCHEMA_VERIFICATION.getVarname(), "true");
    System.exit(tool.run(findHomeDir(), args, additionalGroup,
        MetastoreConf.newMetastoreConf()));
  }
}
