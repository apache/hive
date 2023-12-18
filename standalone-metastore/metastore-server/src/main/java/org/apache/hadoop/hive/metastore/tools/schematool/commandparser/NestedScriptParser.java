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

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.util.IllegalFormatException;

/**
 * Implementations must be able to parse SQL scripts written for specific DBMS, and convert them into a format which
 * can be supplied to {@link sqlline.SqlLine} or Beeline.
 */
public interface NestedScriptParser {

  @VisibleForTesting
  String POSTGRES_STANDARD_STRINGS_OPT = "SET standard_conforming_strings";
  @VisibleForTesting
  String POSTGRES_SKIP_STANDARD_STRINGS_DBOPT = "postgres.filter.81";

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
   * @throws IllegalArgumentException Thrown if the given command is not a nested script, or not in a proper format.
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