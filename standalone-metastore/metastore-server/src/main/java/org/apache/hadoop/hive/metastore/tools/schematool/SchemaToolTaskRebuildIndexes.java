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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SchemaToolTaskRebuildIndexes extends SchemaToolTask {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaToolTaskRebuildIndexes.class);
  static final String REBUILD_INDEXES_FILE_PREFIX = "rebuild-indexes";
  // Neither Oracle nor Derby support DROP INDEX IF EXISTS, so a missing index is tolerated
  // per-statement: ORA-01418 for Oracle, SQL state 42X65 ("Index does not exist") for Derby.
  private static final int ORACLE_INDEX_MISSING_ERROR = 1418;
  private static final String DERBY_INDEX_MISSING_STATE = "42X65";

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
  }

  @Override
  void execute() throws HiveMetaException {
    String dbType = schemaTool.getDbType();
    String scriptDir = schemaTool.getMetaStoreSchemaInfo().getMetaStoreScriptDir();
    String scriptFile = REBUILD_INDEXES_FILE_PREFIX + "." + dbType + ".sql";
    File script = new File(scriptDir, scriptFile);

    if (!script.exists()) {
      throw new HiveMetaException(
          "-rebuildIndexes is not supported for -dbType " + dbType + ". "
              + "Expected script not found: " + script.getAbsolutePath());
    }

    if (schemaTool.isDryRun()) {
      try {
        LOG.info("Dry run: would execute {}", script.getAbsolutePath());
        LOG.info(Files.readString(script.toPath(), StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new HiveMetaException("Failed to read rebuild-indexes script", e);
      }
      return;
    }

    LOG.info("Starting index rebuild using {}", scriptFile);
    try {
      if (HiveSchemaHelper.DB_ORACLE.equalsIgnoreCase(dbType)
          || HiveSchemaHelper.DB_DERBY.equalsIgnoreCase(dbType)) {
        executeJdbcScript(script, dbType);
      } else {
        schemaTool.execSql(scriptDir, scriptFile);
      }
    } catch (IOException | SQLException e) {
      throw new HiveMetaException("Index rebuild failed", e);
    }
    LOG.info("Index rebuild complete.");
  }

  private void executeJdbcScript(File script, String dbType) throws IOException, HiveMetaException, SQLException {
    String content = Files.readString(script.toPath(), StandardCharsets.UTF_8);
    try (Connection connection = schemaTool.getConnectionToMetastore(false);
         Statement statement = connection.createStatement()) {
      StringBuilder sql = new StringBuilder();
      for (String line : content.split("\\r?\\n")) {
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("--")) {
          continue;
        }
        sql.append(line).append('\n');
        if (trimmed.endsWith(";")) {
          executeJdbcStatement(statement, sql.toString(), dbType);
          sql.setLength(0);
        }
      }
      if (!sql.toString().trim().isEmpty()) {
        throw new HiveMetaException(dbType + " rebuild-indexes script contains an unterminated SQL statement.");
      }
    }
  }

  private void executeJdbcStatement(Statement statement, String sql, String dbType) throws SQLException {
    String stmt = sql.trim();
    if (stmt.endsWith(";")) {
      stmt = stmt.substring(0, stmt.length() - 1).trim();
    }
    try {
      statement.execute(stmt);
    } catch (SQLException e) {
      if (stmt.toUpperCase(Locale.ROOT).startsWith("DROP INDEX") && isMissingIndexError(dbType, e)) {
        LOG.info("Skipping missing index during {} rebuild: {}", dbType, stmt);
        return;
      }
      throw e;
    }
  }

  private boolean isMissingIndexError(String dbType, SQLException e) {
    if (HiveSchemaHelper.DB_ORACLE.equalsIgnoreCase(dbType)) {
      return e.getErrorCode() == ORACLE_INDEX_MISSING_ERROR;
    }
    if (HiveSchemaHelper.DB_DERBY.equalsIgnoreCase(dbType)) {
      return DERBY_INDEX_MISSING_STATE.equals(e.getSQLState());
    }
    return false;
  }
}
