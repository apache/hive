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
class SchemaToolTaskCreateCatalog extends MetaStoreTask {
  private static final String CATALOG_EXISTS_QUERY =
      "select <q>NAME<q> " +
          "  from <q>CTLGS<q> " +
          " where <q>NAME<q> = '%s'";

  private static final String NEXT_CATALOG_ID_QUERY =
      "select max(<q>CTLG_ID<q>) " +
          "  from <q>CTLGS<q>";

  private static final String ADD_CATALOG_STMT =
      "insert into <q>CTLGS<q> (<q>CTLG_ID<q>, <q>NAME<q>, <qa>DESC<qa>, <q>LOCATION_URI<q>) " +
          "     values (%d, '%s', '%s', '%s')";
  private static final String CATALOG_DESCRIPTION = "catalogDescription";
  private static final String CATALOG_LOCATION = "catalogLocation";

  @Override
  public Set<String> usedCommandLineArguments() {
    return Sets.newHashSet(CATALOG_LOCATION, CATALOG_DESCRIPTION, "createCatalog", "ifNotExists");
  }

  @Override
  public void execute(TaskContext context) throws HiveMetaException {
    SchemaToolCommandLine commandLine = context.getCommandLine();

    if (!commandLine.hasOption(CATALOG_LOCATION)) {
      throw new HiveMetaException("catalogLocation must be set for createCatalog");
    }
    String catName = normalizeIdentifier(commandLine.getOptionValue("createCatalog"));
    String location = commandLine.getOptionValue(CATALOG_LOCATION);
    String description = commandLine.getOptionValue(CATALOG_DESCRIPTION);
    boolean ifNotExists = commandLine.hasOption("ifNotExists");

    System.out.println("Create catalog " + catName + " at location " + location);

    NestedScriptParser parser = context.getParser();
    Connection conn = context.getConnectionToMetastore(true);
    boolean success = false;
    try {
      conn.setAutoCommit(false);
      try (Statement stmt = conn.createStatement()) {
        // If they set ifNotExists check for existence first, and bail if it exists.  This is
        // more reliable than attempting to parse the error message from the SQLException.
        if (ifNotExists && catalogExists(stmt, catName, parser)) {
          return;
        }

        int catNum = getNextCatalogId(stmt, parser);

        String update = String.format(quote(ADD_CATALOG_STMT, parser), catNum, catName, description, location);
        LOG.debug("Going to run " + update);
        stmt.execute(update);
        conn.commit();

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

  private boolean catalogExists(Statement stmt, String catName, NestedScriptParser parser) throws SQLException {
    String query = String.format(quote(CATALOG_EXISTS_QUERY, parser), catName);
    LOG.debug("Going to run " + query);
    try (ResultSet rs = stmt.executeQuery(query)) {
      if (rs.next()) {
        System.out.println("Catalog " + catName + " already exists");
        return true;
      }
    }

    return false;
  }

  private int getNextCatalogId(Statement stmt, NestedScriptParser parser) throws SQLException, HiveMetaException {
    String query = quote(NEXT_CATALOG_ID_QUERY, parser);
    LOG.debug("Going to run " + query);
    try (ResultSet rs = stmt.executeQuery(query)) {
      if (!rs.next()) {
        throw new HiveMetaException("No catalogs found, have you upgraded the database?");
      }
      int nextId = rs.getInt(1) + 1;
      // We need to stay out of the way of any sequences used by the underlying database.
      // Otherwise, the next time the client tries to add a catalog we'll get an error.
      // There should never be billions of catalogs, so we'll shift our sequence number up
      // there to avoid clashes.
      int floor = 1 << 30;
      return Math.max(nextId, floor);
    }
  }

}
