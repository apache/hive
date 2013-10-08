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

import java.util.IllegalFormatException;

public class HiveSchemaHelper {
  public static final String DB_DERBY = "derby";
  public static final String DB_MYSQL = "mysql";
  public static final String DB_POSTGRACE = "postgres";
  public static final String DB_ORACLE = "oracle";

  public interface NestedScriptParser {

    public enum CommandType {
      PARTIAL_STATEMENT,
      TERMINATED_STATEMENT,
      COMMENT
    }

    static final String DEFAUTL_DELIMITER = ";";
    /***
     * Find the type of given command
     * @param dbCommand
     * @return
     */
    public boolean isPartialCommand(String dbCommand) throws IllegalArgumentException;

    /** Parse the DB specific nesting format and extract the inner script name if any
     * @param dbCommand command from parent script
     * @return
     * @throws IllegalFormatException
     */
    public String getScriptName(String dbCommand) throws IllegalArgumentException;

    /***
     * Find if the given command is a nested script execution
     * @param dbCommand
     * @return
     */
    public boolean isNestedScript(String dbCommand);

    /***
     * Find if the given command is should be passed to DB
     * @param dbCommand
     * @return
     */
    public boolean isNonExecCommand(String dbCommand);

    /***
     * Get the SQL statement delimiter
     * @return
     */
    public String getDelimiter();

    /***
     * Clear any client specific tags
     * @return
     */
    public String cleanseCommand(String dbCommand);

    /***
     * Does the DB required table/column names quoted
     * @return
     */
    public boolean needsQuotedIdentifier();
  }


  /***
   * Base implemenation of NestedScriptParser
   * abstractCommandParser.
   *
   */
  private static abstract class AbstractCommandParser implements NestedScriptParser {

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
  }


  // Derby commandline parser
  public static class DerbyCommandParser extends AbstractCommandParser {
    private static String DERBY_NESTING_TOKEN = "RUN";

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
  }

  //Oracle specific parser
  public static class OracleCommandParser extends AbstractCommandParser {
    private static String ORACLE_NESTING_TOKEN = "@";
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

  public static NestedScriptParser getDbCommandParser(String dbName) {
    if (dbName.equalsIgnoreCase(DB_DERBY)) {
      return new DerbyCommandParser();
    } else if (dbName.equalsIgnoreCase(DB_MYSQL)) {
      return new MySqlCommandParser();
    } else if (dbName.equalsIgnoreCase(DB_POSTGRACE)) {
      return new PostgresCommandParser();
    } else if (dbName.equalsIgnoreCase(DB_ORACLE)) {
      return new OracleCommandParser();
    } else {
      throw new IllegalArgumentException("Unknown dbType " + dbName);
    }
  }
}
