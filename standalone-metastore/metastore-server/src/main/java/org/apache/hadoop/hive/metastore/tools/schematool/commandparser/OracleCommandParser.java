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

class OracleCommandParser extends AbstractCommandParser {
  private static final String ORACLE_NESTING_TOKEN = "@";

  public OracleCommandParser(String dbOpts, boolean usingSqlLine) {
    super(dbOpts, usingSqlLine);
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
