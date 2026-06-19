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
class SchemaToolTaskAlterCatalog extends SchemaToolTask {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaToolTaskAlterCatalog.class.getName());

  private String catName;
  private String location;
  private String description;

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    catName = normalizeIdentifier(cl.getOptionValue("alterCatalog"));
    location = cl.getOptionValue("catalogLocation");
    description = cl.getOptionValue("catalogDescription");
  }

  private static final String UPDATE_CATALOG_STMT =
      "update <q>CTLGS<q> " +
      "   set <q>LOCATION_URI<q> = %s, " +
      "       <qa>DESC<qa> = %s " +
      " where <q>NAME<q> = '%s'";

  @Override
  void execute() throws HiveMetaException {
    if (location == null && description == null) {
      throw new HiveMetaException("Asked to update catalog " + catName + " but not given any changes to update");
    }
    System.out.println("Updating catalog " + catName);

    Connection conn = schemaTool.getConnectionToMetastore(true);
    boolean success = false;
    try {
      conn.setAutoCommit(false);
      try (Statement stmt = conn.createStatement()) {
        Object updateLocation = location == null ? schemaTool.quote("<q>LOCATION_URI<q>") : "'" + location + "'";
        Object updateDescription = description == null ? schemaTool.quote("<qa>DESC<qa>") : "'" + description + "'";
        String update = String.format(schemaTool.quote(UPDATE_CATALOG_STMT), updateLocation, updateDescription,
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
