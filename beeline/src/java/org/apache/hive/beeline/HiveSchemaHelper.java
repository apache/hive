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
package org.apache.hive.beeline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaException;

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
  public static final String DB_DERBY = "derby";
  public static final String DB_MSSQL = "mssql";
  public static final String DB_MYSQL = "mysql";
  public static final String DB_POSTGRACE = "postgres";
  public static final String DB_ORACLE = "oracle";

  /***
   * Get JDBC connection to metastore db
   *
   * @param userName metastore connection username
   * @param password metastore connection password
   * @param printInfo print connection parameters
   * @param hiveConf hive config object
   * @return metastore connection object
   * @throws org.apache.hadoop.hive.metastore.api.MetaException
   */
  public static Connection getConnectionToMetastore(String userName,
      String password, boolean printInfo, HiveConf hiveConf)
      throws HiveMetaException {
    try {
      String connectionURL = getValidConfVar(
          HiveConf.ConfVars.METASTORECONNECTURLKEY, hiveConf);
      String driver = getValidConfVar(
          HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, hiveConf);
      if (printInfo) {
        System.out.println("Metastore connection URL:\t " + connectionURL);
        System.out.println("Metastore Connection Driver :\t " + driver);
        System.out.println("Metastore connection User:\t " + userName);
      }
      if ((userName == null) || userName.isEmpty()) {
        throw new HiveMetaException("UserName empty ");
      }

      // load required JDBC driver
      Class.forName(driver);

      // Connect using the JDBC URL and user/pass from conf
      return DriverManager.getConnection(connectionURL, userName, password);
    } catch (IOException e) {
      throw new HiveMetaException("Failed to get schema version.", e);
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get schema version.", e);
    } catch (ClassNotFoundException e) {
      throw new HiveMetaException("Failed to load driver", e);
    }
  }

  public static String getValidConfVar(HiveConf.ConfVars confVar, HiveConf hiveConf)
      throws IOException {
    String confVarStr = hiveConf.get(confVar.varname);
    if (confVarStr == null || confVarStr.isEmpty()) {
      throw new IOException("Empty " + confVar.varname);
    }
    return confVarStr;
  }

  public interface NestedScriptParser {

    public enum CommandType {
      PARTIAL_STATEMENT,
      TERMINATED_STATEMENT,
      COMMENT
    }

    static final String DEFAUTL_DELIMITER = ";";

    /**
     * Find the type of given command
     *
     * @param dbCommand
     * @return
     */
    public boolean isPartialCommand(String dbCommand) throws IllegalArgumentException;

    /**
     * Parse the DB specific nesting format and extract the inner script name if any
     *
     * @param dbCommand command from parent script
     * @return
     * @throws IllegalFormatException
     */
    public String getScriptName(String dbCommand) throws IllegalArgumentException;

    /**
     * Find if the given command is a nested script execution
     *
     * @param dbCommand
     * @return
     */
    public boolean isNestedScript(String dbCommand);

    /**
     * Find if the given command should not be passed to DB
     *
     * @param dbCommand
     * @return
     */
    public boolean isNonExecCommand(String dbCommand);

    /**
     * Get the SQL statement delimiter
     *
     * @return
     */
    public String getDelimiter();

    /**
     * Clear any client specific tags
     *
     * @return
     */
    public String cleanseCommand(String dbCommand);

    /**
     * Does the DB required table/column names quoted
     *
     * @return
     */
    public boolean needsQuotedIdentifier();

    /**
     * Flatten the nested upgrade script into a buffer
     *
     * @param scriptDir  upgrade script directory
     * @param scriptFile upgrade script file
     * @return string of sql commands
     */
    public String buildCommand(String scriptDir, String scriptFile)
        throws IllegalFormatException, IOException;
  }

  /***
   * Base implemenation of NestedScriptParser
   * abstractCommandParser.
   *
   */
  private static abstract class AbstractCommandParser implements NestedScriptParser {
    private List<String> dbOpts;
    private String msUsername;
    private String msPassword;
    private HiveConf hiveConf;

    public AbstractCommandParser(String dbOpts, String msUsername, String msPassword,
        HiveConf hiveConf) {
      setDbOpts(dbOpts);
      this.msUsername = msUsername;
      this.msPassword = msPassword;
      this.hiveConf = hiveConf;
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
      return DEFAUTL_DELIMITER;
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
      BufferedReader bfReader =
          new BufferedReader(new FileReader(scriptDir + File.separatorChar + scriptFile));
      String currLine;
      StringBuilder sb = new StringBuilder();
      String currentCommand = null;
      while ((currLine = bfReader.readLine()) != null) {
        currLine = currLine.trim();
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

    protected HiveConf getHiveConf() {
      return hiveConf;
    }
  }

  // Derby commandline parser
  public static class DerbyCommandParser extends AbstractCommandParser {
    private static String DERBY_NESTING_TOKEN = "RUN";

    public DerbyCommandParser(String dbOpts, String msUsername, String msPassword,
        HiveConf hiveConf) {
      super(dbOpts, msUsername, msPassword, hiveConf);
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

  // MySQL parser
  public static class MySqlCommandParser extends AbstractCommandParser {
    private static final String MYSQL_NESTING_TOKEN = "SOURCE";
    private static final String DELIMITER_TOKEN = "DELIMITER";
    private String delimiter = DEFAUTL_DELIMITER;

    public MySqlCommandParser(String dbOpts, String msUsername, String msPassword,
        HiveConf hiveConf) {
      super(dbOpts, msUsername, msPassword, hiveConf);
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
    private static String POSTGRES_NESTING_TOKEN = "\\i";
    @VisibleForTesting
    public static String POSTGRES_STANDARD_STRINGS_OPT = "SET standard_conforming_strings";
    @VisibleForTesting
    public static String POSTGRES_SKIP_STANDARD_STRINGS_DBOPT = "postgres.filter.81";

    public PostgresCommandParser(String dbOpts, String msUsername, String msPassword,
        HiveConf hiveConf) {
      super(dbOpts, msUsername, msPassword, hiveConf);
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
    private static String ORACLE_NESTING_TOKEN = "@";

    public OracleCommandParser(String dbOpts, String msUsername, String msPassword,
        HiveConf hiveConf) {
      super(dbOpts, msUsername, msPassword, hiveConf);
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
    private static String MSSQL_NESTING_TOKEN = ":r";

    public MSSQLCommandParser(String dbOpts, String msUsername, String msPassword,
        HiveConf hiveConf) {
      super(dbOpts, msUsername, msPassword, hiveConf);
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

  public static NestedScriptParser getDbCommandParser(String dbName) {
    return getDbCommandParser(dbName, null, null, null, null);
  }

  public static NestedScriptParser getDbCommandParser(String dbName,
      String dbOpts, String msUsername, String msPassword,
      HiveConf hiveConf) {
    if (dbName.equalsIgnoreCase(DB_DERBY)) {
      return new DerbyCommandParser(dbOpts, msUsername, msPassword, hiveConf);
    } else if (dbName.equalsIgnoreCase(DB_MSSQL)) {
      return new MSSQLCommandParser(dbOpts, msUsername, msPassword, hiveConf);
    } else if (dbName.equalsIgnoreCase(DB_MYSQL)) {
      return new MySqlCommandParser(dbOpts, msUsername, msPassword, hiveConf);
    } else if (dbName.equalsIgnoreCase(DB_POSTGRACE)) {
      return new PostgresCommandParser(dbOpts, msUsername, msPassword, hiveConf);
    } else if (dbName.equalsIgnoreCase(DB_ORACLE)) {
      return new OracleCommandParser(dbOpts, msUsername, msPassword, hiveConf);
    } else {
      throw new IllegalArgumentException("Unknown dbType " + dbName);
    }
  }
}