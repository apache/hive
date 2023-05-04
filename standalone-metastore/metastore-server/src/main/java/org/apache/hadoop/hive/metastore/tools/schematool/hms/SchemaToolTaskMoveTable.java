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
package org.apache.hadoop.hive.metastore.tools.schematool.hms;

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.tools.schematool.SchemaToolCommandLine;
import org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParser;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

/**
 * Print Hive version and schema version.
 */
class SchemaToolTaskMoveTable extends MetaStoreTask {

  private static final String FROM_CATALOG = "fromCatalog";
  private static final String TO_CATALOG = "toCatalog";
  private static final String FROM_DATABASE = "fromDatabase";
  private static final String TO_DATABASE = "toDatabase";
  private String fromCat;
  private String toCat;
  private String fromDb;
  private String toDb;
  private String tableName;

  @Override
  public Set<String> usedCommandLineArguments() {
    return Sets.newHashSet(FROM_CATALOG, TO_CATALOG, FROM_DATABASE, TO_DATABASE, "moveTable");
  }

  @Override
  public void execute(TaskContext context) throws HiveMetaException {
    SchemaToolCommandLine commandLine = context.getCommandLine();
    if (!commandLine.hasOption(FROM_CATALOG) || !commandLine.hasOption(TO_CATALOG) || !commandLine.hasOption(FROM_DATABASE) || !commandLine.hasOption(TO_DATABASE)) {
      throw new HiveMetaException("fromCatalog, toCatalog, fromDatabase and toDatabase must be set for moveTable");
    }
    fromCat = normalizeIdentifier(commandLine.getOptionValue(FROM_CATALOG));
    toCat = normalizeIdentifier(commandLine.getOptionValue(TO_CATALOG));
    fromDb = normalizeIdentifier(commandLine.getOptionValue(FROM_DATABASE));
    toDb = normalizeIdentifier(commandLine.getOptionValue(TO_DATABASE));
    tableName = normalizeIdentifier(commandLine.getOptionValue("moveTable"));

    NestedScriptParser parser = context.getParser();
    Connection conn = context.getConnectionToMetastore(true);
    boolean success = false;
    try {
      conn.setAutoCommit(false);
      try (Statement stmt = conn.createStatement()) {
        updateTableId(stmt, parser);
        updateDbNameForTable(stmt, "TAB_COL_STATS", "TABLE_NAME", fromCat, toCat, fromDb, toDb, tableName, parser);
        updateDbNameForTable(stmt, "PART_COL_STATS", "TABLE_NAME", fromCat, toCat, fromDb, toDb, tableName, parser);
        updateDbNameForTable(stmt, "PARTITION_EVENTS", "TBL_NAME", fromCat, toCat, fromDb, toDb, tableName, parser);
        updateDbNameForTable(stmt, "NOTIFICATION_LOG", "TBL_NAME", fromCat, toCat, fromDb, toDb, tableName, parser);
        conn.commit();
        success = true;
      }
    } catch (SQLException se) {
      throw new HiveMetaException("Failed to move table", se);
    } finally {
      try {
        if (!success) {
          conn.rollback();
        }
      } catch (SQLException e) {
        // Not really much we can do here.
        LOG.error("Failed to rollback, everything will probably go bad from here.");
      }

    }
  }

  private static final String UPDATE_TABLE_ID_STMT =
      "update <q>TBLS<q> " +
      "   set <q>DB_ID<q> = %d " +
      " where <q>DB_ID<q> = %d " +
      "   and <q>TBL_NAME<q> = '%s'";

  private void updateTableId(Statement stmt, NestedScriptParser parser) throws SQLException, HiveMetaException {
    // Find the old database id
    long oldDbId = getDbId(stmt, fromDb, fromCat, parser);

    // Find the new database id
    long newDbId = getDbId(stmt, toDb, toCat, parser);

    String update = String.format(quote(UPDATE_TABLE_ID_STMT, parser), newDbId, oldDbId, tableName);
    LOG.debug("Going to run " + update);
    int numUpdated = stmt.executeUpdate(update);
    if (numUpdated != 1) {
      throw new HiveMetaException(
          "Failed to properly update TBLS table.  Expected to update " +
              "1 row but instead updated " + numUpdated);
    }
  }

  private static final String DB_ID_QUERY =
      "select <q>DB_ID<q> " +
      "  from <q>DBS<q> " +
      " where <q>NAME<q> = '%s' " +
      "   and <q>CTLG_NAME<q> = '%s'";

  private long getDbId(Statement stmt, String db, String catalog,
                       NestedScriptParser parser) throws SQLException, HiveMetaException {
    String query = String.format(quote(DB_ID_QUERY, parser), db, catalog);
    LOG.debug("Going to run " + query);
    try (ResultSet rs = stmt.executeQuery(query)) {
      if (!rs.next()) {
        throw new HiveMetaException("Unable to find database " + fromDb);
      }
      return rs.getLong(1);
    }
  }

  private static final String UPDATE_DB_NAME_STMT =
      "update <q>%s<q> " +
      "   set <q>CAT_NAME<q> = '%s', " +
      "       <q>DB_NAME<q> = '%s' " +
      " where <q>CAT_NAME<q> = '%s' " +
      "   and <q>DB_NAME<q> = '%s' " +
      "   and <q>%s<q> = '%s'";

  private void updateDbNameForTable(Statement stmt, String tableName, String tableColumnName, String fromCat,
      String toCat, String fromDb, String toDb, String hiveTblName, NestedScriptParser parser)
      throws HiveMetaException, SQLException {
    String update = String.format(quote(UPDATE_DB_NAME_STMT, parser), tableName, toCat, toDb, fromCat, fromDb,
        tableColumnName, hiveTblName);

    LOG.debug("Going to run " + update);
    int numUpdated = stmt.executeUpdate(update);
    if (numUpdated > 1 || numUpdated < 0) {
      throw new HiveMetaException("Failed to properly update the " + tableName +
          " table.  Expected to update 1 row but instead updated " + numUpdated);
    }
  }

}
