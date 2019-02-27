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
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Print Hive version and schema version.
 */
class SchemaToolTaskMoveDatabase extends SchemaToolTask {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaToolTaskMoveDatabase.class.getName());

  private String fromCatName;
  private String toCatName;
  private String dbName;

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    fromCatName = normalizeIdentifier(cl.getOptionValue("fromCatalog"));
    toCatName = normalizeIdentifier(cl.getOptionValue("toCatalog"));
    dbName = normalizeIdentifier(cl.getOptionValue("moveDatabase"));
  }

  @Override
  void execute() throws HiveMetaException {
    System.out.println(String.format("Moving database %s from catalog %s to catalog %s",
        dbName, fromCatName, toCatName));
    Connection conn = schemaTool.getConnectionToMetastore(true);
    boolean success = false;
    try {
      conn.setAutoCommit(false);
      try (Statement stmt = conn.createStatement()) {
        updateCatalogNameInTable(stmt, "DBS", "CTLG_NAME", "NAME", fromCatName, toCatName, dbName, false);
        updateCatalogNameInTable(stmt, "TAB_COL_STATS", "CAT_NAME", "DB_NAME", fromCatName, toCatName, dbName, true);
        updateCatalogNameInTable(stmt, "PART_COL_STATS", "CAT_NAME", "DB_NAME", fromCatName, toCatName, dbName, true);
        updateCatalogNameInTable(stmt, "PARTITION_EVENTS", "CAT_NAME", "DB_NAME", fromCatName, toCatName, dbName, true);
        updateCatalogNameInTable(stmt, "NOTIFICATION_LOG", "CAT_NAME", "DB_NAME", fromCatName, toCatName, dbName, true);
        conn.commit();
        success = true;
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to move database", e);
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

  private static final String UPDATE_CATALOG_NAME_STMT =
      "update <q>%s<q> " +
      "   set <q>%s<q> = '%s' " +
      " where <q>%s<q> = '%s' " +
      "   and <q>%s<q> = '%s'";

  private void updateCatalogNameInTable(Statement stmt, String tableName, String catColName, String dbColName,
      String fromCatName, String toCatName, String dbName, boolean zeroUpdatesOk)
      throws HiveMetaException, SQLException {
    String update = String.format(schemaTool.quote(UPDATE_CATALOG_NAME_STMT), tableName, catColName, toCatName,
        catColName, fromCatName, dbColName, dbName);
    LOG.debug("Going to run " + update);
    int numUpdated = stmt.executeUpdate(update);
    if (numUpdated != 1 && !(zeroUpdatesOk && numUpdated == 0)) {
      throw new HiveMetaException("Failed to properly update the " + tableName +
          " table.  Expected to update 1 row but instead updated " + numUpdated);
    }
  }
}
