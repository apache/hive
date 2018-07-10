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

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.IMetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper.MetaStoreConnectionInfo;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper.NestedScriptParser;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.beeline.BeeLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;

public class HiveSchemaTool {
  private static final Logger LOG = LoggerFactory.getLogger(HiveSchemaTool.class.getName());

  private final HiveConf hiveConf;
  private final String dbType;
  private final String metaDbType;
  private final IMetaStoreSchemaInfo metaStoreSchemaInfo;
  private final boolean needsQuotedIdentifier;
  private String quoteCharacter;

  private String url = null;
  private String driver = null;
  private String userName = null;
  private String passWord = null;
  private boolean dryRun = false;
  private boolean verbose = false;
  private String dbOpts = null;
  private URI[] validationServers = null; // The list of servers the database/partition/table can locate on

  private HiveSchemaTool(String dbType, String metaDbType) throws HiveMetaException {
    this(System.getenv("HIVE_HOME"), new HiveConf(HiveSchemaTool.class), dbType, metaDbType);
  }

  @VisibleForTesting
  public HiveSchemaTool(String hiveHome, HiveConf hiveConf, String dbType, String metaDbType)
      throws HiveMetaException {
    if (hiveHome == null || hiveHome.isEmpty()) {
      throw new HiveMetaException("No Hive home directory provided");
    }
    this.hiveConf = hiveConf;
    this.dbType = dbType;
    this.metaDbType = metaDbType;
    NestedScriptParser parser = getDbCommandParser(dbType, metaDbType);
    this.needsQuotedIdentifier = parser.needsQuotedIdentifier();
    this.quoteCharacter = parser.getQuoteCharacter();
    this.metaStoreSchemaInfo = MetaStoreSchemaInfoFactory.get(hiveConf, hiveHome, dbType);
    // If the dbType is "hive", this is setting up the information schema in Hive.
    // We will set the default jdbc url and driver.
    // It is overriden by command line options if passed (-url and -driver
    if (dbType.equalsIgnoreCase(HiveSchemaHelper.DB_HIVE)) {
      url = HiveSchemaHelper.EMBEDDED_HS2_URL;
      driver = HiveSchemaHelper.HIVE_JDBC_DRIVER;
    }
  }

  HiveConf getHiveConf() {
    return hiveConf;
  }

  String getDbType() {
    return dbType;
  }

  IMetaStoreSchemaInfo getMetaStoreSchemaInfo() {
    return metaStoreSchemaInfo;
  }

  private void setUrl(String url) {
    this.url = url;
  }

  private void setDriver(String driver) {
    this.driver = driver;
  }

  @VisibleForTesting
  public void setUserName(String userName) {
    this.userName = userName;
  }

  @VisibleForTesting
  public void setPassWord(String passWord) {
    this.passWord = passWord;
  }

  @VisibleForTesting
  public void setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  boolean isDryRun() {
    return dryRun;
  }

  @VisibleForTesting
  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  boolean isVerbose() {
    return verbose;
  }

  private void setDbOpts(String dbOpts) {
    this.dbOpts = dbOpts;
  }

  private void setValidationServers(String servers) {
    if(StringUtils.isNotEmpty(servers)) {
      String[] strServers = servers.split(",");
      this.validationServers = new URI[strServers.length];
      for (int i = 0; i < validationServers.length; i++) {
        validationServers[i] = new Path(strServers[i]).toUri();
      }
    }
  }

  URI[] getValidationServers() {
    return validationServers;
  }

  Connection getConnectionToMetastore(boolean printInfo) throws HiveMetaException {
    return HiveSchemaHelper.getConnectionToMetastore(userName, passWord, url, driver, printInfo, hiveConf,
        null);
  }

  private NestedScriptParser getDbCommandParser(String dbType, String metaDbType) {
    return HiveSchemaHelper.getDbCommandParser(dbType, dbOpts, userName, passWord, hiveConf,
        metaDbType, false);
  }

  // test the connection metastore using the config property
  void testConnectionToMetastore() throws HiveMetaException {
    Connection conn = getConnectionToMetastore(true);
    try {
      conn.close();
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to close metastore connection", e);
    }
  }

  /**
   * check if the current schema version in metastore matches the Hive version
   * @throws MetaException
   */
  void verifySchemaVersion() throws HiveMetaException {
    // don't check version if its a dry run
    if (dryRun) {
      return;
    }
    String newSchemaVersion = metaStoreSchemaInfo.getMetaStoreSchemaVersion(getConnectionInfo(false));
    // verify that the new version is added to schema
    assertCompatibleVersion(metaStoreSchemaInfo.getHiveSchemaVersion(), newSchemaVersion);
  }

  void assertCompatibleVersion(String hiveSchemaVersion, String dbSchemaVersion)
      throws HiveMetaException {
    if (!metaStoreSchemaInfo.isVersionCompatible(hiveSchemaVersion, dbSchemaVersion)) {
      throw new HiveMetaException("Metastore schema version is not compatible. Hive Version: "
          + hiveSchemaVersion + ", Database Schema Version: " + dbSchemaVersion);
    }
  }

  MetaStoreConnectionInfo getConnectionInfo(boolean printInfo) {
    return new MetaStoreConnectionInfo(userName, passWord, url, driver, printInfo, hiveConf,
        dbType, metaDbType);
  }

  // Quote if the database requires it
  String quote(String stmt) {
    stmt = stmt.replace("<q>", needsQuotedIdentifier ? quoteCharacter : "");
    stmt = stmt.replace("<qa>", quoteCharacter);
    return stmt;
  }

  /***
   * Run beeline with the given metastore script. Flatten the nested scripts
   * into single file.
   */
  void runBeeLine(String scriptDir, String scriptFile)
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
    out.write("!autocommit on" + System.getProperty("line.separator"));
    out.write(sqlCommands);
    out.write("!closeall" + System.getProperty("line.separator"));
    out.close();
    runBeeLine(tmpFile.getPath());
  }

  // Generate the beeline args per hive conf and execute the given script
  void runBeeLine(String sqlScriptFile) throws IOException {
    CommandBuilder builder = new CommandBuilder(hiveConf, url, driver,
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
      int status = beeLine.begin(builder.buildToRun(), null);
      if (status != 0) {
        throw new IOException("Schema script failed, errorcode " + status);
      }
    }
  }

  static class CommandBuilder {
    private final String userName;
    private final String password;
    private final String sqlScriptFile;
    private final String driver;
    private final String url;

    CommandBuilder(HiveConf hiveConf, String url, String driver, String userName, String password,
        String sqlScriptFile) throws IOException {
      this.userName = userName;
      this.password = password;
      this.url = url == null ?
          HiveSchemaHelper.getValidConfVar(MetastoreConf.ConfVars.CONNECT_URL_KEY, hiveConf) : url;
      this.driver = driver == null ?
          HiveSchemaHelper.getValidConfVar(MetastoreConf.ConfVars.CONNECTION_DRIVER, hiveConf) : driver;
      this.sqlScriptFile = sqlScriptFile;
    }

    String[] buildToRun() {
      return argsWith(password);
    }

    String buildToLog() throws IOException {
      logScript();
      return StringUtils.join(argsWith(BeeLine.PASSWD_MASK), " ");
    }

    private String[] argsWith(String password) {
      return new String[]
          {
            "-u", url,
            "-d", driver,
            "-n", userName,
            "-p", password,
            "-f", sqlScriptFile
          };
    }

    private void logScript() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to invoke file that contains:");
        try (BufferedReader reader = new BufferedReader(new FileReader(sqlScriptFile))) {
          String line;
          while ((line = reader.readLine()) != null) {
            LOG.debug("script: " + line);
          }
        }
      }
    }
  }

  public static void main(String[] args) {
    HiveSchemaToolCommandLine line = null;
    try {
      line = new HiveSchemaToolCommandLine(args);
    } catch (ParseException e) {
      System.exit(1);
    }

    System.setProperty(MetastoreConf.ConfVars.SCHEMA_VERIFICATION.getVarname(), "true");
    try {
      HiveSchemaTool schemaTool = createSchemaTool(line);

      HiveSchemaToolTask task = null;
      if (line.hasOption("info")) {
        task = new HiveSchemaToolTaskInfo();
      } else if (line.hasOption("upgradeSchema") || line.hasOption("upgradeSchemaFrom")) {
        task = new HiveSchemaToolTaskUpgrade();
      } else if (line.hasOption("initSchema") || line.hasOption("initSchemaTo")) {
        task = new HiveSchemaToolTaskInit();
      } else if (line.hasOption("validate")) {
        task = new HiveSchemaToolTaskValidate();
      } else if (line.hasOption("createCatalog")) {
        task = new HiveSchemaToolTaskCreateCatalog();
      } else if (line.hasOption("alterCatalog")) {
        task = new HiveSchemaToolTaskAlterCatalog();
      } else if (line.hasOption("moveDatabase")) {
        task = new HiveSchemaToolTaskMoveDatabase();
      } else if (line.hasOption("moveTable")) {
        task = new HiveSchemaToolTaskMoveTable();
      } else {
        throw new HiveMetaException("No task defined!");
      }

      task.setHiveSchemaTool(schemaTool);
      task.setCommandLineArguments(line);
      task.execute();

    } catch (HiveMetaException e) {
      System.err.println(e);
      if (e.getCause() != null) {
        Throwable t = e.getCause();
        System.err.println("Underlying cause: " + t.getClass().getName() + " : " + t.getMessage());
        if (e.getCause() instanceof SQLException) {
          System.err.println("SQL Error code: " + ((SQLException)t).getErrorCode());
        }
      }
      if (line.hasOption("verbose")) {
        e.printStackTrace();
      } else {
        System.err.println("Use --verbose for detailed stacktrace.");
      }
      System.err.println("*** schemaTool failed ***");
      System.exit(1);
    }
    System.out.println("schemaTool completed");
    System.exit(0);
  }

  private static HiveSchemaTool createSchemaTool(HiveSchemaToolCommandLine line) throws HiveMetaException {
    HiveSchemaTool schemaTool = new HiveSchemaTool(line.getDbType(), line.getMetaDbType());

    if (line.hasOption("userName")) {
      schemaTool.setUserName(line.getOptionValue("userName"));
    } else {
      schemaTool.setUserName(
          schemaTool.getHiveConf().get(MetastoreConf.ConfVars.CONNECTION_USER_NAME.getVarname()));
    }
    if (line.hasOption("passWord")) {
      schemaTool.setPassWord(line.getOptionValue("passWord"));
    } else {
      try {
        schemaTool.setPassWord(ShimLoader.getHadoopShims().getPassword(schemaTool.getHiveConf(),
            MetastoreConf.ConfVars.PWD.getVarname()));
      } catch (IOException err) {
        throw new HiveMetaException("Error getting metastore password", err);
      }
    }
    if (line.hasOption("url")) {
      schemaTool.setUrl(line.getOptionValue("url"));
    }
    if (line.hasOption("driver")) {
      schemaTool.setDriver(line.getOptionValue("driver"));
    }
    if (line.hasOption("dryRun")) {
      schemaTool.setDryRun(true);
    }
    if (line.hasOption("verbose")) {
      schemaTool.setVerbose(true);
    }
    if (line.hasOption("dbOpts")) {
      schemaTool.setDbOpts(line.getOptionValue("dbOpts"));
    }
    if (line.hasOption("validate") && line.hasOption("servers")) {
      schemaTool.setValidationServers(line.getOptionValue("servers"));
    }
    return schemaTool;
  }
}
