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

class PostgresCommandParser extends AbstractCommandParser {
  private static final String POSTGRES_NESTING_TOKEN = "\\i";

  public PostgresCommandParser(String dbOpts, boolean usingSqlLine) {
    super(dbOpts, usingSqlLine);
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
