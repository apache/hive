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

class DerbyCommandParser extends AbstractCommandParser {
  private static final String DERBY_NESTING_TOKEN = "RUN";

  public DerbyCommandParser(String dbOpts, boolean usingSqlLine) {
    super(dbOpts, usingSqlLine);
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
    return tokens[1].replace(";", "").replace("'", "");
  }

  @Override
  public boolean isNestedScript(String dbCommand) {
    // Derby script format is RUN '<file>'
    return dbCommand.startsWith(DERBY_NESTING_TOKEN);
  }
}
