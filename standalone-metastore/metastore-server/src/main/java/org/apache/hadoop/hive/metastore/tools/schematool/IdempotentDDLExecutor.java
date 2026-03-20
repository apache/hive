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

public class IdempotentDDLExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(IdempotentDDLExecutor.class);

  private final Connection conn;
  private final DbErrorCodes errorCodes;
  private final HiveSchemaHelper.NestedScriptParser parser;
  private final boolean verbose;

  public IdempotentDDLExecutor(
      Connection conn, String dbType, HiveSchemaHelper.NestedScriptParser parser, boolean verbose) {
    this.conn = conn;
    this.errorCodes = DbErrorCodes.forDbType(dbType);
    this.parser = parser;
    this.verbose = verbose;
  }

  public void executeScript(String scriptFile) throws SQLException, IOException {
    LOG.info("Executing script line-by-line via IdempotentDDLExecutor: {}", scriptFile);
    conn.setAutoCommit(true);

    File file = new File(scriptFile);
    List<String> commands = parser.getExecutableCommands(file.getParent(), file.getName());

    for (String sqlStmt : commands) {
      if (!sqlStmt.isEmpty()) {
        executeStatement(sqlStmt);
      }
    }
    if (verbose) {
      System.out.println("Completed successfully.");
    }
  }

  private void executeStatement(String sqlStmt) throws SQLException {
    if (verbose) {
      System.out.println("Executing: " + sqlStmt);
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("Executing: {}", sqlStmt);
    }

    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sqlStmt);
    } catch (SQLException e) {
      if (errorCodes.isIgnorable(e)) {
        String msg = String.format("Object already exists or was already dropped. " +
            "Statement: %s, ErrorCode: %d, SQLState: %s", sqlStmt, e.getErrorCode(), e.getSQLState());
        if (verbose) {
          System.out.println(msg);
        }
        LOG.info(msg);
      } else {
        LOG.error("SQL Error executing: {}. Error Code: {}, SQLState: {}", sqlStmt, e.getErrorCode(), e.getSQLState());
        throw e;
      }
    }
  }
}