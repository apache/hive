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

import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Print Hive version and schema version.
 */
class SchemaToolTaskMoveTable extends SchemaToolTask {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaToolTaskMoveTable.class.getName());

  private String fromCat;
  private String toCat;
  private String fromDb;
  private String toDb;
  private String tableName;

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    fromCat = normalizeIdentifier(cl.getOptionValue("fromCatalog"));
    toCat = normalizeIdentifier(cl.getOptionValue("toCatalog"));
    fromDb = normalizeIdentifier(cl.getOptionValue("fromDatabase"));
    toDb = normalizeIdentifier(cl.getOptionValue("toDatabase"));
    tableName = normalizeIdentifier(cl.getOptionValue("moveTable"));
  }

  @Override
  void execute() throws HiveMetaException {
    Connection conn = schemaTool.getConnectionToMetastore(true);
    boolean success = false;
    try {
      conn.setAutoCommit(false);
      try (Statement stmt = conn.createStatement()) {
        updateTableId(stmt);
        updateDbNameForTable(stmt, "TAB_COL_STATS", "TABLE_NAME", fromCat, toCat, fromDb, toDb, tableName);
        updateDbNameForTable(stmt, "PART_COL_STATS", "TABLE_NAME", fromCat, toCat, fromDb, toDb, tableName);
        updateDbNameForTable(stmt, "PARTITION_EVENTS", "TBL_NAME", fromCat, toCat, fromDb, toDb, tableName);
        updateDbNameForTable(stmt, "NOTIFICATION_LOG", "TBL_NAME", fromCat, toCat, fromDb, toDb, tableName);
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

  private void updateTableId(Statement stmt) throws SQLException, HiveMetaException {
    // Find the old database id
    long oldDbId = getDbId(stmt, fromDb, fromCat);

    // Find the new database id
    long newDbId = getDbId(stmt, toDb, toCat);

    String update = String.format(schemaTool.quote(UPDATE_TABLE_ID_STMT), newDbId, oldDbId, tableName);
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

  private long getDbId(Statement stmt, String db, String catalog) throws SQLException, HiveMetaException {
    String query = String.format(schemaTool.quote(DB_ID_QUERY), db, catalog);
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
      String toCat, String fromDb, String toDb, String hiveTblName) throws HiveMetaException, SQLException {
    String update = String.format(schemaTool.quote(UPDATE_DB_NAME_STMT), tableName, toCat, toDb, fromCat, fromDb,
        tableColumnName, hiveTblName);

    LOG.debug("Going to run " + update);
    int numUpdated = stmt.executeUpdate(update);
    if (numUpdated > 1 || numUpdated < 0) {
      throw new HiveMetaException("Failed to properly update the " + tableName +
          " table.  Expected to update 1 row but instead updated " + numUpdated);
    }
  }
}
