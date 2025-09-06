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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.MetaStoreConnectionInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.NestedScriptParser;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hive.beeline.BeeLine;
import org.apache.hive.beeline.BeeLineDummyTerminal;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.utils.StringUtils.isEmpty;

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
    replaceLocationForProtoLogTables(sqlScriptFile);
    CommandBuilder builder = new HiveSchemaToolCommandBuilder(conf, url, driver,
        userName, passWord, sqlScriptFile);

    // run the script using Beeline
    try (BeeLine beeLine = new BeeLineDummyTerminal()) {
      if (!verbose) {
        beeLine.setOutputStream(new PrintStream(new NullOutputStream()));
        beeLine.getOpts().setSilent(true);
      }
      beeLine.getOpts().setAllowMultiLineCommand(false);
      beeLine.getOpts().setIsolation("TRANSACTION_READ_COMMITTED");
      // We can be pretty sure that an entire line can be processed as a single command since
      // we always add a line separator at the end while calling dbCommandParser.buildCommand.
      beeLine.getOpts().setEntireLineAsCommand(true);
      LOG.debug("Going to run command <{}>", builder.buildToLog());
      int status = beeLine.begin(builder.buildToRun(), null, false);
      if (status != 0) {
        throw new IOException("Schema script failed, errorcode " + status);
      }
    }
  }

  void replaceLocationForProtoLogTables(String sqlScriptFile) throws IOException {
    TezConfiguration tezConf = new TezConfiguration(true);
    boolean hiveProtoLoggingEnabled = true;
    boolean tezProtoLoggingEnabled = true;
    String hiveProtoBaseDir = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_PROTO_EVENTS_BASE_PATH);
    String tezProtoBaseDir = tezConf.get(TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_BASE_DIR);
    String hiveLocation = "/tmp/query_data"; // if Hive protologging is not enabled, use dummy location for Hive protolog tables
    String tezLocation = "/tmp"; // if Tez protologging is not enabled, use dummy location for Tez protolog tables
    String line;
    StringBuilder newLine = new StringBuilder();
    Map<String, String> replacements = new HashMap<>();

    if (isEmpty(hiveProtoBaseDir)) {
      LOG.warn("Hive conf variable hive.hook.proto.base-directory is not set for creating protologging tables");
      hiveProtoLoggingEnabled = false;
    }
    if (isEmpty(tezProtoBaseDir)) {
      LOG.warn("Tez conf variable tez.history.logging.proto-base-dir is not set for creating protologging tables");
      tezProtoLoggingEnabled = false;
    }

    if (hiveProtoLoggingEnabled) {
      String hiveProtoScheme = new Path(hiveProtoBaseDir).getFileSystem(conf).getScheme() + ":///";
      hiveLocation = new Path(hiveProtoBaseDir).getFileSystem(conf).getUri().isAbsolute() ? hiveProtoBaseDir : hiveProtoScheme + hiveProtoBaseDir;
    }
    if (tezProtoLoggingEnabled) {
      String tezProtoScheme = new Path(tezProtoBaseDir).getFileSystem(tezConf).getScheme() + ":///";
      tezLocation = new Path(tezProtoBaseDir).getFileSystem(tezConf).getUri().isAbsolute() ? tezProtoBaseDir : tezProtoScheme + tezProtoBaseDir;
    }

    replacements.put("_REPLACE_WITH_QUERY_DATA_LOCATION_", hiveLocation);
    replacements.put("_REPLACE_WITH_APP_DATA_LOCATION_", tezLocation + "/app_data");
    replacements.put("_REPLACE_WITH_DAG_DATA_LOCATION_", tezLocation + "/dag_data");
    replacements.put("_REPLACE_WITH_DAG_META_LOCATION_", tezLocation + "/dag_meta");

    try (BufferedReader reader = new BufferedReader(new FileReader(sqlScriptFile))) {
      while ((line = reader.readLine()) != null) {
        for (Map.Entry<String, String> entry : replacements.entrySet()) {
          if (line.contains(entry.getKey())) {
            line = line.replace(entry.getKey(), entry.getValue());
          }
        }
        newLine.append(line).append("\n");
      }
    }

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(sqlScriptFile))) {
      writer.write(newLine.toString());
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
    ExitUtil.terminate(tool.run(findHomeDir(), args, additionalGroup,
        MetastoreConf.newMetastoreConf()));
  }
}
