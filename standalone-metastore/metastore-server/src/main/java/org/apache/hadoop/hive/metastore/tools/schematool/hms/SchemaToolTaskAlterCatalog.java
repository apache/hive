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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

/**
 * Print Hive version and schema version.
 */
class SchemaToolTaskAlterCatalog extends MetaStoreTask {

  private static final String CATALOG_DESCRIPTION = "catalogDescription";
  private static final String CATALOG_LOCATION = "catalogLocation";
  private static final String ALTER_CATALOG = "alterCatalog";

  @Override
  public Set<String> usedCommandLineArguments() {
    return Sets.newHashSet(ALTER_CATALOG, CATALOG_LOCATION, CATALOG_DESCRIPTION);
  }

  private static final String UPDATE_CATALOG_STMT =
      "update <q>CTLGS<q> " +
      "   set <q>LOCATION_URI<q> = %s, " +
      "       <qa>DESC<qa> = %s " +
      " where <q>NAME<q> = '%s'";

  @Override
  public void execute(TaskContext context) throws HiveMetaException {
    SchemaToolCommandLine commandLine = context.getCommandLine();

    String catName = normalizeIdentifier(commandLine.getOptionValue(ALTER_CATALOG));
    String location = commandLine.getOptionValue(CATALOG_LOCATION);
    String description = commandLine.getOptionValue(CATALOG_DESCRIPTION);

    if (!commandLine.hasOption(CATALOG_LOCATION) && !commandLine.hasOption(CATALOG_DESCRIPTION)) {
      throw new HiveMetaException("Asked to update catalog " + catName + " but not given any changes to update");
    }

    LOG.info("Updating catalog " + catName);

    NestedScriptParser parser = context.getParser();
    Connection conn = context.getConnectionToMetastore(true);
    boolean success = false;
    try {
      conn.setAutoCommit(false);
      try (Statement stmt = conn.createStatement()) {
        Object updateLocation = location == null ? quote("<q>LOCATION_URI<q>", parser) : "'" + location + "'";
        Object updateDescription = description == null ? quote("<qa>DESC<qa>", parser) : "'" + description + "'";
        String update = String.format(quote(UPDATE_CATALOG_STMT, parser), updateLocation, updateDescription,
            catName);
        LOG.debug("Going to run " + update);
        int count = stmt.executeUpdate(update);
        if (count != 1) {
          throw new HiveMetaException("Failed to find catalog " + catName + " to update");
        }
        conn.commit();
        success = true;
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to update catalog", e);
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

}
