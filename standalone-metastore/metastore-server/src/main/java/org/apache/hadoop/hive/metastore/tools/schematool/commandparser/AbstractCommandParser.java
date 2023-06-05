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

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.IllegalFormatException;
import java.util.List;

/**
 * Base implementation of NestedScriptParser abstractCommandParser.
 */
abstract class AbstractCommandParser implements NestedScriptParser {

  private List<String> dbOpts;
  // Depending on whether we are using beeline or sqlline the line endings have to be handled
  // differently.
  private final boolean usingSqlLine;

  public AbstractCommandParser(String dbOpts, boolean usingSqlLine) {
    setDbOpts(dbOpts);
    this.usingSqlLine = usingSqlLine;
  }

  @Override
  public boolean isPartialCommand(String dbCommand) throws IllegalArgumentException{
    if (dbCommand == null || dbCommand.isEmpty()) {
      throw new IllegalArgumentException("invalid command line " + dbCommand);
    }
    dbCommand = dbCommand.trim();
    return !(dbCommand.endsWith(getDelimiter()) || isNonExecCommand(dbCommand));
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
    StringBuilder sb = new StringBuilder();
    try(BufferedReader bfReader = new BufferedReader(new FileReader(scriptDir + File.separatorChar + scriptFile))) {
      String currLine;
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
    }
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

}