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
package org.apache.hadoop.hive.metastore.tools.schematool.commandparser;

class MySqlCommandParser extends AbstractCommandParser {
  private static final String MYSQL_NESTING_TOKEN = "SOURCE";
  private static final String DELIMITER_TOKEN = "DELIMITER";
  private String delimiter = DEFAULT_DELIMITER;

  public MySqlCommandParser(String dbOpts, boolean usingSqlLine) {
    super(dbOpts, usingSqlLine);
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
