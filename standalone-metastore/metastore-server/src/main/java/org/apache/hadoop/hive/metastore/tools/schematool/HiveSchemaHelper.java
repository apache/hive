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
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.IllegalFormatException;
import java.util.List;

public class HiveSchemaHelper {
  private static final Logger LOG = LoggerFactory.getLogger(HiveSchemaHelper.class);

  public static final String DB_DERBY = "derby";
  public static final String DB_HIVE = "hive";
  public static final String DB_MSSQL = "mssql";
  public static final String DB_MYSQL = "mysql";
  public static final String DB_POSTGRACE = "postgres";
  public static final String DB_ORACLE = "oracle";
  public static final String EMBEDDED_HS2_URL =
      "jdbc:hive2://?hive.conf.restricted.list=;hive.security.authorization.sqlstd.confwhitelist=.*;"
      + "hive.security.authorization.sqlstd.confwhitelist.append=.*;hive.security.authorization.enabled=false;"
      + "hive.metastore.uris=;hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory;"
      + "hive.support.concurrency=false;hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;"
      + "hive.metastore.rawstore.impl=org.apache.hadoop.hive.metastore.ObjectStore";
  public static final String HIVE_JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";

  /***
   * Get JDBC connection to metastore db
   * @param userName metastore connection username
   * @param password metastore connection password
   * @param url Metastore URL.  If null will be read from config file.
   * @param driver Driver class.  If null will be read from config file.
   * @param printInfo print connection parameters
   * @param conf hive config object
   * @param schema the schema to create the connection for
   * @return metastore connection object
   * @throws org.apache.hadoop.hive.metastore.HiveMetaException
   */
  public static Connection getConnectionToMetastore(String userName, String password, String url,
      String driver, boolean printInfo, Configuration conf, String schema) throws HiveMetaException {
    try {
      url = url == null ? getValidConfVar(MetastoreConf.ConfVars.CONNECT_URL_KEY, conf) : url;
      driver = driver == null ? getValidConfVar(MetastoreConf.ConfVars.CONNECTION_DRIVER, conf) : driver;
      if (printInfo) {
        logAndPrintToStdout("Metastore connection URL:\t " + url);
        logAndPrintToStdout("Metastore connection Driver :\t " + driver);
        logAndPrintToStdout("Metastore connection User:\t " + userName);
        if (MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST)) {
          logAndPrintToStdout("Metastore connection Password:\t " + password);
        }
      }
      if ((userName == null) || userName.isEmpty()) {
        throw new HiveMetaException("UserName empty ");
      }

      // load required JDBC driver
      Class.forName(driver);

      // Connect using the JDBC URL and user/pass from conf
      Connection conn = DriverManager.getConnection(url, userName, password);
      if (schema != null) {
        conn.setSchema(schema);
      }
      return conn;
    } catch (IOException | SQLException e) {
      throw new HiveMetaException("Failed to get schema version.", e);
    } catch (ClassNotFoundException e) {
      LOG.error("Unable to find driver class", e);
      throw new HiveMetaException("Failed to load driver", e);
    }
  }

  public static Connection getConnectionToMetastore(MetaStoreConnectionInfo info, String schema)
      throws HiveMetaException {
    return getConnectionToMetastore(info.getUsername(), info.getPassword(), info.getUrl(), info.getDriver(),
        info.getPrintInfo(), info.getConf(), schema);
  }

  public static String getValidConfVar(MetastoreConf.ConfVars confVar, Configuration conf)
      throws IOException {
    String confVarStr = MetastoreConf.getAsString(conf, confVar);
    if (confVarStr == null || confVarStr.isEmpty()) {
      throw new IOException("Empty " + confVar.getVarname());
    }
    return confVarStr.trim();
  }

  private static void logAndPrintToStdout(String msg) {
    LOG.info(msg);
    System.out.println(msg);
  }

  public interface NestedScriptParser {

    enum CommandType {
      PARTIAL_STATEMENT,
      TERMINATED_STATEMENT,
      COMMENT
    }

    String DEFAULT_DELIMITER = ";";
    String DEFAULT_QUOTE = "\"";

    /**
     * Find the type of given command
     */
    boolean isPartialCommand(String dbCommand) throws IllegalArgumentException;

    /**
     * Parse the DB specific nesting format and extract the inner script name if any
     *
     * @param dbCommand command from parent script
     * @throws IllegalFormatException
     */
    String getScriptName(String dbCommand) throws IllegalArgumentException;

    /**
     * Find if the given command is a nested script execution
     */
    boolean isNestedScript(String dbCommand);

    /**
     * Find if the given command should not be passed to DB
     */
    boolean isNonExecCommand(String dbCommand);

    /**
     * Get the SQL statement delimiter
     */
    String getDelimiter();

    /**
     * Get the SQL indentifier quotation character
     */
    String getQuoteCharacter();

    /**
     * Clear any client specific tags
     */
    String cleanseCommand(String dbCommand);

    /**
     * Does the DB required table/column names quoted
     */
    boolean needsQuotedIdentifier();

    /**
     * Flatten the nested upgrade script into a buffer
     *
     * @param scriptDir  upgrade script directory
     * @param scriptFile upgrade script file
     * @return string of sql commands
     */
    String buildCommand(String scriptDir, String scriptFile)
        throws IllegalFormatException, IOException;

    /**
     * Flatten the nested upgrade script into a buffer
     *
     * @param scriptDir  upgrade script directory
     * @param scriptFile upgrade script file
     * @param fixQuotes whether to replace quote characters
     * @return string of sql commands
     */
    String buildCommand(String scriptDir, String scriptFile, boolean fixQuotes)
        throws IllegalFormatException, IOException;
  }

  /**
   * Base implementation of NestedScriptParser abstractCommandParser.
   */
  private static abstract class AbstractCommandParser implements NestedScriptParser {
    private List<String> dbOpts;
    private String msUsername;
    private String msPassword;
    private Configuration conf;
    // Depending on whether we are using beeline or sqlline the line endings have to be handled
    // differently.
    private final boolean usingSqlLine;

    public AbstractCommandParser(String dbOpts, String msUsername, String msPassword,
        Configuration conf, boolean usingSqlLine) {
      setDbOpts(dbOpts);
      this.msUsername = msUsername;
      this.msPassword = msPassword;
      this.conf = conf;
      this.usingSqlLine = usingSqlLine;
    }

    @Override
    public boolean isPartialCommand(String dbCommand) throws IllegalArgumentException{
      if (dbCommand == null || dbCommand.isEmpty()) {
        throw new IllegalArgumentException("invalid command line " + dbCommand);
      }
      dbCommand = dbCommand.trim();
      if (dbCommand.endsWith(getDelimiter()) || isNonExecCommand(dbCommand)) {
        return false;
      } else {
        return true;
      }
    }

    @Override
    public boolean isNonExecCommand(String dbCommand) {
      return (dbCommand.startsWith("--") || dbCommand.startsWith("#"));
    }

    @Override
    public String getDelimiter() {
      return DEFAULT_DELIMITER;
    }

    @Override
    public String getQuoteCharacter() {
      return DEFAULT_QUOTE;
    }


    @Override
    public String cleanseCommand(String dbCommand) {
      // strip off the delimiter
      if (dbCommand.endsWith(getDelimiter())) {
        dbCommand = dbCommand.substring(0,
            dbCommand.length() - getDelimiter().length());
      }
      return dbCommand;
    }

    @Override
    public boolean needsQuotedIdentifier() {
      return false;
    }

    @Override
    public String buildCommand(
      String scriptDir, String scriptFile) throws IllegalFormatException, IOException {
      return buildCommand(scriptDir, scriptFile, false);
    }

    @Override
    public String buildCommand(
      String scriptDir, String scriptFile, boolean fixQuotes) throws IllegalFormatException, IOException {
      BufferedReader bfReader =
          new BufferedReader(new FileReader(scriptDir + File.separatorChar + scriptFile));
      String currLine;
      StringBuilder sb = new StringBuilder();
      String currentCommand = null;
      while ((currLine = bfReader.readLine()) != null) {
        currLine = currLine.trim();

        if (fixQuotes && !getQuoteCharacter().equals(DEFAULT_QUOTE)) {
          currLine = currLine.replace("\\\"", getQuoteCharacter());
        }

        if (currLine.isEmpty()) {
          continue; // skip empty lines
        }

        if (currentCommand == null) {
          currentCommand = currLine;
        } else {
          currentCommand = currentCommand + " " + currLine;
        }
        if (isPartialCommand(currLine)) {
          // if its a partial line, continue collecting the pieces
          continue;
        }

        // if this is a valid executable command then add it to the buffer
        if (!isNonExecCommand(currentCommand)) {
          currentCommand = cleanseCommand(currentCommand);
          if (isNestedScript(currentCommand)) {
            // if this is a nested sql script then flatten it
            String currScript = getScriptName(currentCommand);
            sb.append(buildCommand(scriptDir, currScript));
          } else {
            // Now we have a complete statement, process it
            // write the line to buffer
            sb.append(currentCommand);
            if (usingSqlLine) sb.append(";");
            sb.append(System.getProperty("line.separator"));
          }
        }
        currentCommand = null;
      }
      bfReader.close();
      return sb.toString();
    }

    private void setDbOpts(String dbOpts) {
      if (dbOpts != null) {
        this.dbOpts = Lists.newArrayList(dbOpts.split(","));
      } else {
        this.dbOpts = Lists.newArrayList();
      }
    }

    protected List<String> getDbOpts() {
      return dbOpts;
    }

    protected String getMsUsername() {
      return msUsername;
    }

    protected String getMsPassword() {
      return msPassword;
    }

    protected Configuration getConf() {
      return conf;
    }
  }

  // Derby commandline parser
  public static class DerbyCommandParser extends AbstractCommandParser {
    private static final String DERBY_NESTING_TOKEN = "RUN";

    public DerbyCommandParser(String dbOpts, String msUsername, String msPassword,
        Configuration conf, boolean usingSqlLine) {
      super(dbOpts, msUsername, msPassword, conf, usingSqlLine);
    }

    @Override
    public String getScriptName(String dbCommand) throws IllegalArgumentException {

      if (!isNestedScript(dbCommand)) {
        throw new IllegalArgumentException("Not a script format " + dbCommand);
      }
      String[] tokens = dbCommand.split(" ");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Couldn't parse line " + dbCommand);
      }
      return tokens[1].replace(";", "").replaceAll("'", "");
    }

    @Override
    public boolean isNestedScript(String dbCommand) {
      // Derby script format is RUN '<file>'
     return dbCommand.startsWith(DERBY_NESTING_TOKEN);
    }
  }

  // Derby commandline parser
  public static class HiveCommandParser extends AbstractCommandParser {
    private static String HIVE_NESTING_TOKEN = "SOURCE";
    private final NestedScriptParser nestedDbCommandParser;

    public HiveCommandParser(String dbOpts, String msUsername, String msPassword,
        Configuration conf, String metaDbType, boolean usingSqlLine) {
      super(dbOpts, msUsername, msPassword, conf, usingSqlLine);
      nestedDbCommandParser = getDbCommandParser(metaDbType, usingSqlLine);
    }

    @Override
    public String getQuoteCharacter() {
      return nestedDbCommandParser.getQuoteCharacter();
    }

    @Override
    public String getScriptName(String dbCommand) throws IllegalArgumentException {

      if (!isNestedScript(dbCommand)) {
        throw new IllegalArgumentException("Not a script format " + dbCommand);
      }
      String[] tokens = dbCommand.split(" ");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Couldn't parse line " + dbCommand);
      }
      return tokens[1].replace(";", "");
    }

    @Override
    public boolean isNestedScript(String dbCommand) {
     return dbCommand.startsWith(HIVE_NESTING_TOKEN);
    }
  }

  // MySQL parser
  public static class MySqlCommandParser extends AbstractCommandParser {
    private static final String MYSQL_NESTING_TOKEN = "SOURCE";
    private static final String DELIMITER_TOKEN = "DELIMITER";
    private String delimiter = DEFAULT_DELIMITER;

    public MySqlCommandParser(String dbOpts, String msUsername, String msPassword,
        Configuration conf, boolean usingSqlLine) {
      super(dbOpts, msUsername, msPassword, conf, usingSqlLine);
    }

    @Override
    public boolean isPartialCommand(String dbCommand) throws IllegalArgumentException{
      boolean isPartial = super.isPartialCommand(dbCommand);
      // if this is a delimiter directive, reset our delimiter
      if (dbCommand.startsWith(DELIMITER_TOKEN)) {
        String[] tokens = dbCommand.split(" ");
        if (tokens.length != 2) {
          throw new IllegalArgumentException("Couldn't parse line " + dbCommand);
        }
        delimiter = tokens[1];
      }
      return isPartial;
    }

    @Override
    public String getScriptName(String dbCommand) throws IllegalArgumentException {
      String[] tokens = dbCommand.split(" ");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Couldn't parse line " + dbCommand);
      }
      // remove ending ';'
      return tokens[1].replace(";", "");
    }

    @Override
    public boolean isNestedScript(String dbCommand) {
      return dbCommand.startsWith(MYSQL_NESTING_TOKEN);
    }

    @Override
    public String getDelimiter() {
      return delimiter;
    }

    @Override
    public String getQuoteCharacter() {
      return "`";
    }

    @Override
    public boolean needsQuotedIdentifier() {
      return true;
    }

    @Override
    public boolean isNonExecCommand(String dbCommand) {
      return super.isNonExecCommand(dbCommand) ||
          (dbCommand.startsWith("/*") && dbCommand.endsWith("*/")) ||
          dbCommand.startsWith(DELIMITER_TOKEN);
    }

    @Override
    public String cleanseCommand(String dbCommand) {
      return super.cleanseCommand(dbCommand).replaceAll("/\\*.*?\\*/[^;]", "");
    }

  }

  // Postgres specific parser
  public static class PostgresCommandParser extends AbstractCommandParser {
    private static final String POSTGRES_NESTING_TOKEN = "\\i";
    @VisibleForTesting
    public static final String POSTGRES_STANDARD_STRINGS_OPT = "SET standard_conforming_strings";
    @VisibleForTesting
    public static final String POSTGRES_SKIP_STANDARD_STRINGS_DBOPT = "postgres.filter.81";

    public PostgresCommandParser(String dbOpts, String msUsername, String msPassword,
        Configuration conf, boolean usingSqlLine) {
      super(dbOpts, msUsername, msPassword, conf, usingSqlLine);
    }

    @Override
    public String getScriptName(String dbCommand) throws IllegalArgumentException {
      String[] tokens = dbCommand.split(" ");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Couldn't parse line " + dbCommand);
      }
      // remove ending ';'
      return tokens[1].replace(";", "");
    }

    @Override
    public boolean isNestedScript(String dbCommand) {
      return dbCommand.startsWith(POSTGRES_NESTING_TOKEN);
    }

    @Override
    public boolean needsQuotedIdentifier() {
      return true;
    }

    @Override
    public boolean isNonExecCommand(String dbCommand) {
      // Skip "standard_conforming_strings" command which is read-only in older
      // Postgres versions like 8.1
      // See: http://www.postgresql.org/docs/8.2/static/release-8-1.html
      if (getDbOpts().contains(POSTGRES_SKIP_STANDARD_STRINGS_DBOPT)) {
        if (dbCommand.startsWith(POSTGRES_STANDARD_STRINGS_OPT)) {
          return true;
        }
      }
      return super.isNonExecCommand(dbCommand);
    }
  }

  //Oracle specific parser
  public static class OracleCommandParser extends AbstractCommandParser {
    private static final String ORACLE_NESTING_TOKEN = "@";

    public OracleCommandParser(String dbOpts, String msUsername, String msPassword,
        Configuration conf, boolean usingSqlLine) {
      super(dbOpts, msUsername, msPassword, conf, usingSqlLine);
    }

    @Override
    public String getScriptName(String dbCommand) throws IllegalArgumentException {
      if (!isNestedScript(dbCommand)) {
        throw new IllegalArgumentException("Not a nested script format " + dbCommand);
      }
      // remove ending ';' and starting '@'
      return dbCommand.replace(";", "").replace(ORACLE_NESTING_TOKEN, "");
    }

    @Override
    public boolean isNestedScript(String dbCommand) {
      return dbCommand.startsWith(ORACLE_NESTING_TOKEN);
    }
  }

  //MSSQL specific parser
  public static class MSSQLCommandParser extends AbstractCommandParser {
    private static final String MSSQL_NESTING_TOKEN = ":r";

    public MSSQLCommandParser(String dbOpts, String msUsername, String msPassword,
        Configuration conf, boolean usingSqlLine) {
      super(dbOpts, msUsername, msPassword, conf, usingSqlLine);
    }

    @Override
    public String getScriptName(String dbCommand) throws IllegalArgumentException {
      String[] tokens = dbCommand.split(" ");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Couldn't parse line " + dbCommand);
      }
      return tokens[1];
    }

    @Override
    public boolean isNestedScript(String dbCommand) {
      return dbCommand.startsWith(MSSQL_NESTING_TOKEN);
    }
  }

  public static NestedScriptParser getDbCommandParser(String dbName, boolean usingSqlLine) {
    return getDbCommandParser(dbName, null, usingSqlLine);
  }

  public static NestedScriptParser getDbCommandParser(String dbName, String metaDbName, boolean usingSqlLine) {
    return getDbCommandParser(dbName, null, null, null, null, metaDbName, usingSqlLine);
  }

  public static NestedScriptParser getDbCommandParser(String dbName,
      String dbOpts, String msUsername, String msPassword,
      Configuration conf, String metaDbType, boolean usingSqlLine) {
    if (dbName.equalsIgnoreCase(DB_DERBY)) {
      return new DerbyCommandParser(dbOpts, msUsername, msPassword, conf, usingSqlLine);
    } else if (dbName.equalsIgnoreCase(DB_HIVE)) {
      return new HiveCommandParser(dbOpts, msUsername, msPassword, conf, metaDbType, usingSqlLine);
    } else if (dbName.equalsIgnoreCase(DB_MSSQL)) {
      return new MSSQLCommandParser(dbOpts, msUsername, msPassword, conf, usingSqlLine);
    } else if (dbName.equalsIgnoreCase(DB_MYSQL)) {
      return new MySqlCommandParser(dbOpts, msUsername, msPassword, conf, usingSqlLine);
    } else if (dbName.equalsIgnoreCase(DB_POSTGRACE)) {
      return new PostgresCommandParser(dbOpts, msUsername, msPassword, conf, usingSqlLine);
    } else if (dbName.equalsIgnoreCase(DB_ORACLE)) {
      return new OracleCommandParser(dbOpts, msUsername, msPassword, conf, usingSqlLine);
    } else {
      throw new IllegalArgumentException("Unknown dbType " + dbName);
    }
  }

  public static class MetaStoreConnectionInfo {
    private final String userName;
    private final String password;
    private final String url;
    private final String driver;
    private final boolean printInfo;
    private final Configuration conf;
    private final String dbType;
    private final String metaDbType;

    public MetaStoreConnectionInfo(String userName, String password, String url, String driver,
                                   boolean printInfo, Configuration conf, String dbType, String metaDbType) {
      super();
      this.userName = userName;
      this.password = password;
      this.url = url;
      this.driver = driver;
      this.printInfo = printInfo;
      this.conf = conf;
      this.dbType = dbType;
      this.metaDbType = metaDbType;
    }

    public String getPassword() {
      return password;
    }

    public String getUrl() {
      return url;
    }

    public String getDriver() {
      return driver;
    }

    public boolean isPrintInfo() {
      return printInfo;
    }

    public Configuration getConf() {
      return conf;
    }

    public String getUsername() {
      return userName;
    }

    public boolean getPrintInfo() {
      return printInfo;
    }

    public String getDbType() {
      return dbType;
    }

    public String getMetaDbType() {
      return metaDbType;
    }
  }
}
