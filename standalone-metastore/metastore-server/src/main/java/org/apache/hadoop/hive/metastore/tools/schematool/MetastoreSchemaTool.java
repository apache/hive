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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.IMetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.MetaStoreConnectionInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.NestedScriptParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sqlline.SqlLine;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;

public class MetastoreSchemaTool {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreSchemaTool.class);
  private static final String PASSWD_MASK = "[passwd stripped]";

  protected Configuration conf;

  protected String dbOpts = null;
  protected String dbType;
  protected String driver = null;
  protected boolean dryRun = false;
  protected String hiveDb; // Hive database, for use when creating the user, not for connecting
  protected String hivePasswd; // Hive password, for use when creating the user, not for connecting
  protected String hiveUser; // Hive username, for use when creating the user, not for connecting
  protected String metaDbType;
  protected IMetaStoreSchemaInfo metaStoreSchemaInfo;
  protected boolean needsQuotedIdentifier;
  protected String quoteCharacter;
  protected String passWord = null;
  protected String url = null;
  protected String userName = null;
  protected URI[] validationServers = null; // The list of servers the database/partition/table can locate on
  protected boolean verbose = false;
  protected SchemaToolCommandLine cmdLine;

  private static String homeDir;

  protected static String findHomeDir() {
    // If METASTORE_HOME is set, use it, else use HIVE_HOME for backwards compatibility.
    homeDir = homeDir == null ? System.getenv("METASTORE_HOME") : homeDir;
    return homeDir == null ? System.getenv("HIVE_HOME") : homeDir;
  }

  @VisibleForTesting
  public static void setHomeDirForTesting() {
    homeDir = System.getProperty("test.tmp.dir", "target/tmp");
  }

  @VisibleForTesting
  public MetastoreSchemaTool() {

  }

  @VisibleForTesting
  public void init(String metastoreHome, String[] args, OptionGroup additionalOptions,
                   Configuration conf) throws HiveMetaException {
    try {
      cmdLine = new SchemaToolCommandLine(args, additionalOptions);
    } catch (ParseException e) {
      System.err.println("Failed to parse command line. ");
      throw new HiveMetaException(e);
    }

    if (metastoreHome == null || metastoreHome.isEmpty()) {
      throw new HiveMetaException("No Metastore home directory provided");
    }
    this.conf = conf;
    this.dbType = cmdLine.getDbType();
    this.metaDbType = cmdLine.getMetaDbType();
    NestedScriptParser parser = getDbCommandParser(dbType, metaDbType);
    this.needsQuotedIdentifier = parser.needsQuotedIdentifier();
    this.quoteCharacter = parser.getQuoteCharacter();
    this.metaStoreSchemaInfo = MetaStoreSchemaInfoFactory.get(conf, metastoreHome, dbType);
    // If the dbType is "hive", this is setting up the information schema in Hive.
    // We will set the default jdbc url and driver.
    // It is overridden by command line options if passed (-url and -driver)
    if (dbType.equalsIgnoreCase(HiveSchemaHelper.DB_HIVE)) {
      this.url = HiveSchemaHelper.EMBEDDED_HS2_URL;
      this.driver = HiveSchemaHelper.HIVE_JDBC_DRIVER;
    }

    if (cmdLine.hasOption("userName")) {
      setUserName(cmdLine.getOptionValue("userName"));
    } else {
      setUserName(MetastoreConf.getAsString(getConf(), MetastoreConf.ConfVars.CONNECTION_USER_NAME));
    }
    if (cmdLine.hasOption("passWord")) {
      setPassWord(cmdLine.getOptionValue("passWord"));
    } else {
      try {
        setPassWord(MetastoreConf.getPassword(getConf(), ConfVars.PWD));
      } catch (IOException err) {
        throw new HiveMetaException("Error getting metastore password", err);
      }
    }
    if (cmdLine.hasOption("url")) {
      setUrl(cmdLine.getOptionValue("url"));
    }
    if (cmdLine.hasOption("driver")) {
      setDriver(cmdLine.getOptionValue("driver"));
    }
    if (cmdLine.hasOption("dryRun")) {
      setDryRun(true);
    }
    if (cmdLine.hasOption("verbose")) {
      setVerbose(true);
    }
    if (cmdLine.hasOption("dbOpts")) {
      setDbOpts(cmdLine.getOptionValue("dbOpts"));
    }
    if (cmdLine.hasOption("validate") && cmdLine.hasOption("servers")) {
      setValidationServers(cmdLine.getOptionValue("servers"));
    }
    if (cmdLine.hasOption("hiveUser")) {
      setHiveUser(cmdLine.getOptionValue("hiveUser"));
    }
    if (cmdLine.hasOption("hivePassword")) {
      setHivePasswd(cmdLine.getOptionValue("hivePassword"));
    }
    if (cmdLine.hasOption("hiveDb")) {
      setHiveDb(cmdLine.getOptionValue("hiveDb"));
    }
  }

  public Configuration getConf() {
    return conf;
  }

  protected String getDbType() {
    return dbType;
  }

  protected void setUrl(String url) {
    this.url = url;
  }

  protected void setDriver(String driver) {
    this.driver = driver;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public void setPassWord(String passWord) {
    this.passWord = passWord;
  }

  protected boolean isDryRun() {
    return dryRun;
  }

  protected void setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  protected boolean isVerbose() {
    return verbose;
  }

  public MetastoreSchemaTool setVerbose(boolean verbose) {
    this.verbose = verbose;
    return this;
  }

  protected void setDbOpts(String dbOpts) {
    this.dbOpts = dbOpts;
  }

  protected URI[] getValidationServers() {
    return validationServers;
  }

  protected void setValidationServers(String servers) {
    if(StringUtils.isNotEmpty(servers)) {
      String[] strServers = servers.split(",");
      this.validationServers = new URI[strServers.length];
      for (int i = 0; i < validationServers.length; i++) {
        validationServers[i] = new Path(strServers[i]).toUri();
      }
    }
  }

  protected String getHiveUser() {
    return hiveUser;
  }

  protected void setHiveUser(String hiveUser) {
    this.hiveUser = hiveUser;
  }

  protected String getHivePasswd() {
    return hivePasswd;
  }

  protected void setHivePasswd(String hivePasswd) {
    this.hivePasswd = hivePasswd;
  }

  protected String getHiveDb() {
    return hiveDb;
  }

  protected void setHiveDb(String hiveDb) {
    this.hiveDb = hiveDb;
  }

  protected SchemaToolCommandLine getCmdLine() {
    return cmdLine;
  }

  public Connection getConnectionToMetastore(boolean printInfo) throws HiveMetaException {
    return HiveSchemaHelper.getConnectionToMetastore(userName,
        passWord, url, driver, printInfo, conf, null);
  }

  protected NestedScriptParser getDbCommandParser(String dbType, String metaDbType) {
    return HiveSchemaHelper.getDbCommandParser(dbType, dbOpts, userName,
        passWord, conf, null, true);
  }

  protected MetaStoreConnectionInfo getConnectionInfo(boolean printInfo) {
    return new MetaStoreConnectionInfo(userName, passWord, url, driver, printInfo, conf,
        dbType, hiveDb);
  }

  protected IMetaStoreSchemaInfo getMetaStoreSchemaInfo() {
    return metaStoreSchemaInfo;
  }

  /**
   * check if the current schema version in metastore matches the Hive version
   */
  @VisibleForTesting
  void verifySchemaVersion() throws HiveMetaException {
    // don't check version if its a dry run
    if (dryRun) {
      return;
    }
    String newSchemaVersion = metaStoreSchemaInfo.getMetaStoreSchemaVersion(getConnectionInfo(false));
    // verify that the new version is added to schema
    assertCompatibleVersion(metaStoreSchemaInfo.getHiveSchemaVersion(), newSchemaVersion);
  }

  protected void assertCompatibleVersion(String hiveSchemaVersion, String dbSchemaVersion)
      throws HiveMetaException {
    if (!metaStoreSchemaInfo.isVersionCompatible(hiveSchemaVersion, dbSchemaVersion)) {
      throw new HiveMetaException("Metastore schema version is not compatible. Hive Version: "
          + hiveSchemaVersion + ", Database Schema Version: " + dbSchemaVersion);
    }
  }

  /***
   * Execute a given metastore script. This default version uses sqlline to execute the files,
   * which requires only running one file.  Subclasses can use other executors.
   * @param scriptDir directory script is in
   * @param scriptFile file in the directory to run
   * @throws IOException if it cannot read the file or directory
   * @throws HiveMetaException default implementation never throws this
   */
  protected void execSql(String scriptDir, String scriptFile) throws IOException, HiveMetaException {

    execSql(scriptDir + File.separatorChar + scriptFile);
  }

  // Generate the beeline args per hive conf and execute the given script
  protected void execSql(String sqlScriptFile) throws IOException {
    CommandBuilder builder =
        new CommandBuilder(conf, url, driver, userName, passWord, sqlScriptFile)
            .setVerbose(verbose);

    // run the script using SqlLine
    SqlLine sqlLine = new SqlLine();
    ByteArrayOutputStream outputForLog = null;
    if (!verbose) {
      OutputStream out;
      if (LOG.isDebugEnabled()) {
        out = outputForLog = new ByteArrayOutputStream();
      } else {
        out = new NullOutputStream();
      }
      sqlLine.setOutputStream(new PrintStream(out));
      System.setProperty("sqlline.silent", "true");
    }
    LOG.info("Going to run command <" + builder.buildToLog() + ">");
    SqlLine.Status status = sqlLine.begin(builder.buildToRun(), null, false);
    if (LOG.isDebugEnabled() && outputForLog != null) {
      LOG.debug("Received following output from Sqlline:");
      LOG.debug(outputForLog.toString("UTF-8"));
    }
    if (status != SqlLine.Status.OK) {
      throw new IOException("Schema script failed, errorcode " + status);
    }
  }

  // test the connection metastore using the config property
  protected void testConnectionToMetastore() throws HiveMetaException {
    Connection conn = getConnectionToMetastore(true);
    try {
      conn.close();
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to close metastore connection", e);
    }
  }

  // Quote if the database requires it
  protected String quote(String stmt) {
    return quote(stmt, needsQuotedIdentifier, quoteCharacter);
  }

  public static String quote(String stmt, boolean needsQuotedIdentifier, String quoteCharacter) {
    stmt = stmt.replace("<q>", needsQuotedIdentifier ? quoteCharacter : "");
    stmt = stmt.replace("<qa>", quoteCharacter);
    return stmt;
  }

  protected static class CommandBuilder {
    protected final String userName;
    protected final String password;
    protected final String sqlScriptFile;
    protected final String driver;
    protected final String url;
    private boolean verbose = false;

    protected CommandBuilder(Configuration conf, String url, String driver, String userName,
                             String password, String sqlScriptFile) throws IOException {
      this.userName = userName;
      this.password = password;
      this.url = url == null ?
          HiveSchemaHelper.getValidConfVar(MetastoreConf.ConfVars.CONNECT_URL_KEY, conf) : url;
      this.driver = driver == null ?
          HiveSchemaHelper.getValidConfVar(MetastoreConf.ConfVars.CONNECTION_DRIVER, conf) : driver;
      this.sqlScriptFile = sqlScriptFile;
    }

    public CommandBuilder setVerbose(boolean verbose) {
      this.verbose = verbose;
      return this;
    }

    public String[] buildToRun() throws IOException {
      return argsWith(password);
    }

    public String buildToLog() throws IOException {
      if (verbose) {
        logScript();
      }
      return StringUtils.join(argsWith(PASSWD_MASK), " ");
    }

    protected String[] argsWith(String password) throws IOException {
      return new String[]
        {
          "-u", url,
          "-d", driver,
          "-n", userName,
          "-p", password,
          "--isolation=TRANSACTION_READ_COMMITTED",
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

  // Create the required command line options
  private static void logAndPrintToError(String errmsg) {
    LOG.error(errmsg);
    System.err.println(errmsg);
  }

  public static void main(String[] args) {
    MetastoreSchemaTool tool = new MetastoreSchemaTool();
    System.exit(tool.run(args));
  }

  public int run(String[] args) {
    return run(findHomeDir(), args, null, MetastoreConf.newMetastoreConf());
  }

  @VisibleForTesting
  public final int runScript(String[] args, InputStream scriptStream) {
    try {
      init(findHomeDir(), args, null, MetastoreConf.newMetastoreConf());
      // Cannot run script directly from input stream thus copy is necessary.
      File scriptFile = File.createTempFile("schemaToolTmpScript", "sql");
      scriptFile.deleteOnExit();
      FileUtils.copyToFile(scriptStream, scriptFile);
      execSql(scriptFile.getAbsolutePath());
      return 0;
    } catch (HiveMetaException | IOException e) {
      throw new RuntimeException("Failed to run script " + scriptStream, e);
    }
  }
  
  public int run(String metastoreHome, String[] args, OptionGroup additionalOptions,
                 Configuration conf) {
    try {
      init(metastoreHome, args, additionalOptions, conf);
      SchemaToolTask task;
      if (cmdLine.hasOption("info")) {
        task = new SchemaToolTaskInfo();
      } else if (cmdLine.hasOption("upgradeSchema") || cmdLine.hasOption("upgradeSchemaFrom")) {
        task = new SchemaToolTaskUpgrade();
      } else if (cmdLine.hasOption("initSchema") || cmdLine.hasOption("initSchemaTo")) {
        task = new SchemaToolTaskInit();
      } else if (cmdLine.hasOption("initOrUpgradeSchema")) {
        task = new SchemaToolTaskInitOrUpgrade();
      } else if (cmdLine.hasOption("validate")) {
        task = new SchemaToolTaskValidate();
      } else if (cmdLine.hasOption("createCatalog")) {
        task = new SchemaToolTaskCreateCatalog();
      } else if (cmdLine.hasOption("alterCatalog")) {
        task = new SchemaToolTaskAlterCatalog();
      } else if (cmdLine.hasOption("mergeCatalog")) {
        task = new SchemaToolTaskMergeCatalog();
      } else if (cmdLine.hasOption("moveDatabase")) {
        task = new SchemaToolTaskMoveDatabase();
      } else if (cmdLine.hasOption("moveTable")) {
        task = new SchemaToolTaskMoveTable();
      } else if (cmdLine.hasOption("createUser")) {
        task = new SchemaToolTaskCreateUser();
      } else if (cmdLine.hasOption("dropAllDatabases")) {
        task = new SchemaToolTaskDrop();
      } else if (cmdLine.hasOption("createLogsTable")) {
        task = new SchemaToolTaskCreateLogsTable();
      } else {
        throw new HiveMetaException("No task defined!");
      }

      task.setHiveSchemaTool(this);
      task.setCommandLineArguments(cmdLine);
      task.execute();
      return 0;
    } catch (HiveMetaException e) {
      logAndPrintToError(e.getMessage());
      if (e.getCause() != null) {
        Throwable t = e.getCause();
        logAndPrintToError("Underlying cause: "
            + t.getClass().getName() + " : "
            + t.getMessage());
        if (e.getCause() instanceof SQLException) {
          logAndPrintToError("SQL Error code: " + ((SQLException) t).getErrorCode());
        }
      }
      if (cmdLine != null) {
        if (cmdLine.hasOption("verbose")) {
          e.printStackTrace();
        } else {
          logAndPrintToError("Use --verbose for detailed stacktrace.");
        }
      }
      logAndPrintToError("*** schemaTool failed ***");
      return 1;

    }
  }
}
