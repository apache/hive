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
class SchemaToolTaskCreateCatalog extends SchemaToolTask {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaToolTaskCreateCatalog.class.getName());

  private String catName;
  private String location;
  private String description;
  private boolean ifNotExists;

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    catName = normalizeIdentifier(cl.getOptionValue("createCatalog"));
    location = cl.getOptionValue("catalogLocation");
    description = cl.getOptionValue("catalogDescription");
    ifNotExists = cl.hasOption("ifNotExists");
  }

  @Override
  void execute() throws HiveMetaException {
    System.out.println("Create catalog " + catName + " at location " + location);

    Connection conn = schemaTool.getConnectionToMetastore(true);
    boolean success = false;
    try {
      conn.setAutoCommit(false);
      try (Statement stmt = conn.createStatement()) {
        // If they set ifNotExists check for existence first, and bail if it exists.  This is
        // more reliable then attempting to parse the error message from the SQLException.
        if (ifNotExists && catalogExists(stmt)) {
          return;
        }

        int catNum = getNextCatalogId(stmt);
        addCatalog(conn, stmt, catNum);
        success = true;
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to add catalog", e);
    } finally {
      try {
        if (!success) {
          conn.rollback();
        }
      } catch (SQLException e) {
        // Not really much we can do here.
        LOG.error("Failed to rollback, everything will probably go bad from here.", e);
      }
    }
  }

  private static final String CATALOG_EXISTS_QUERY =
      "select <q>NAME<q> " +
      "  from <q>CTLGS<q> " +
      " where <q>NAME<q> = '%s'";

  private boolean catalogExists(Statement stmt) throws SQLException {
    String query = String.format(schemaTool.quote(CATALOG_EXISTS_QUERY), catName);
    LOG.debug("Going to run " + query);
    try (ResultSet rs = stmt.executeQuery(query)) {
      if (rs.next()) {
        System.out.println("Catalog " + catName + " already exists");
        return true;
      }
    }

    return false;
  }

  private static final String NEXT_CATALOG_ID_QUERY =
      "select max(<q>CTLG_ID<q>) " +
      "  from <q>CTLGS<q>";

  private int getNextCatalogId(Statement stmt) throws SQLException, HiveMetaException {
    String query = schemaTool.quote(NEXT_CATALOG_ID_QUERY);
    LOG.debug("Going to run " + query);
    try (ResultSet rs = stmt.executeQuery(query)) {
      if (!rs.next()) {
        throw new HiveMetaException("No catalogs found, have you upgraded the database?");
      }
      int nextId = rs.getInt(1) + 1;
      // We need to stay out of the way of any sequences used by the underlying database.
      // Otherwise the next time the client tries to add a catalog we'll get an error.
      // There should never be billions of catalogs, so we'll shift our sequence number up
      // there to avoid clashes.
      int floor = 1 << 30;
      return Math.max(nextId, floor);
    }
  }

  private static final String ADD_CATALOG_STMT =
      "insert into <q>CTLGS<q> (<q>CTLG_ID<q>, <q>NAME<q>, <qa>DESC<qa>, <q>LOCATION_URI<q>) " +
      "     values (%d, '%s', '%s', '%s')";

  private void addCatalog(Connection conn, Statement stmt, int catNum) throws SQLException {
    String update = String.format(schemaTool.quote(ADD_CATALOG_STMT), catNum, catName, description, location);
    LOG.debug("Going to run " + update);
    stmt.execute(update);
    conn.commit();
  }
}
