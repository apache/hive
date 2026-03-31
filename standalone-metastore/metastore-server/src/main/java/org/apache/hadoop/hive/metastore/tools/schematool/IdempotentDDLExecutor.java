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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class IdempotentDDLExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(IdempotentDDLExecutor.class);
  private static final Set<String> DDL_FIRST_TOKENS = Set.of(
      "CREATE", "ALTER", "DROP", "RENAME", "TRUNCATE", "COMMENT");

  private final Connection conn;
  private final DbErrorCodes errorCodes;
  private final HiveSchemaHelper.NestedScriptParser parser;
  private final boolean fixQuotes;
  private final boolean verbose;

  public IdempotentDDLExecutor(
      Connection conn, String dbType, HiveSchemaHelper.NestedScriptParser parser,
      boolean fixQuotes, boolean verbose) {
    this.conn = conn;
    this.errorCodes = DbErrorCodes.forDbType(dbType);
    this.parser = parser;
    this.fixQuotes = fixQuotes;
    this.verbose = verbose;
  }

  public void executeScript(String scriptFile) throws SQLException, IOException {
    LOG.info("Executing script line-by-line via IdempotentDDLExecutor: {}", scriptFile);
    conn.setAutoCommit(true);

    File file = new File(scriptFile);
    List<String> commands = parser.getExecutableCommands(file.getParent(), file.getName(), fixQuotes);

    for (String sqlStmt : commands) {
      if (!sqlStmt.isEmpty()) {
        executeStatement(sqlStmt);
      }
    }
    if (verbose) {
      LOG.info("Completed successfully.");
    }
  }

  private void executeStatement(String sqlStmt) throws SQLException {
    if (verbose) {
      LOG.info("Executing: {}", sqlStmt);
    } 
    if (LOG.isDebugEnabled()) {
      LOG.debug("Executing: {}", sqlStmt);
    }

    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sqlStmt);
    } catch (SQLException e) {
      if (isDdlStatement(sqlStmt) && errorCodes.isIgnorable(e)) {
        LOG.info("Object already exists or was already dropped. Statement: {}, ErrorCode: {}, SQLState: {}",
            sqlStmt, e.getErrorCode(), e.getSQLState());
      } else {
        LOG.error("SQL Error executing: {}. Error Code: {}, SQLState: {}", sqlStmt, e.getErrorCode(), e.getSQLState());
        throw e;
      }
    }
  }

  private boolean isDdlStatement(String sqlStmt) {
    if (sqlStmt == null) {
      return false;
    }

    String trimmed = sqlStmt.trim();
    if (trimmed.isEmpty()) {
      return false;
    }

    // We only inspect the first one or two keywords for statement type classification.
    String[] tokens = trimmed.split("\\s+", 3);
    if (tokens.length == 0) {
      return false;
    }

    String first = normalizeToken(tokens[0]).toUpperCase(Locale.ROOT);
    if (DDL_FIRST_TOKENS.contains(first)) {
      return true;
    }

    if (("EXEC".equals(first) || "EXECUTE".equals(first)) && tokens.length > 1) {
      String second = normalizeToken(tokens[1]);
      return "SP_RENAME".equalsIgnoreCase(second) || isDropHelperProcedure(second);
    }

    return false;
  }

  private String normalizeToken(String token) {
    if (token == null || token.isEmpty()) {
      return "";
    }
    if (token.endsWith(";")) {
      return token.substring(0, token.length() - 1);
    }
    return token;
  }

  private boolean isDropHelperProcedure(String token) {
    return token != null && token.toUpperCase(Locale.ROOT).startsWith("#DROP_");
  }
}
