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
import org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParser;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

class SchemaToolTaskMergeCatalog extends MetaStoreTask {

  private static final String DB_CONFLICTS_STMT =
      "SELECT d.<q>NAME<q> as DB, d.<q>CTLG_NAME<q>, d2.<q>CTLG_NAME<q> FROM <q>DBS<q> d, <q>DBS<q> d2 "
          + "WHERE d.<q>NAME<q> = d2.<q>NAME<q> AND "
          + "d.<q>CTLG_NAME<q> = '%s' AND d2.<q>CTLG_NAME<q> = '%s'";

  private static final String MERGE_CATALOG_STMT =
      "UPDATE <q>DBS<q> " +
          " SET <q>CTLG_NAME<q> = '%s' " + " WHERE <q>CTLG_NAME<q> = '%s'";

  private static final String CONVERT_TABLE_TO_EXTERNAL =
      "update <q>TBLS<q> set <q>TBL_TYPE<q> = '%s' where <q>TBL_ID<q> in (" +
          "select tid from (select <q>TBL_ID<q> as tid from <q>TBLS<q> t2, <q>DBS<q> d where t2.<q>TBL_TYPE<q> = '%s' and t2.<q>DB_ID<q> = d.<q>DB_ID<q> " +
          "and d.<q>CTLG_NAME<q> = '%s') c) ";

  private static final String ADD_AUTOPURGE_TO_TABLE =
      "INSERT INTO <q>TABLE_PARAMS<q> (<q>TBL_ID<q>, <q>PARAM_KEY<q>, <q>PARAM_VALUE<q>) select <q>TBL_ID<q>, "
          + "'%s', '%s' from <q>TBLS<q> t, <q>DBS<q> d, <q>CTLGS<q> c "
          + "where <q>TBL_TYPE<q> = '%s' and t.<q>DB_ID<q> = d.<q>DB_ID<q> and d.<q>CTLG_NAME<q> = c.<q>NAME<q> and c.<q>NAME<q> = '%s' ";
  private static final String TO_CATALOG = "toCatalog";
  private static final String MERGE_CATALOG = "mergeCatalog";

  @Override
  public Set<String> usedCommandLineArguments() {
    return Sets.newHashSet(MERGE_CATALOG, TO_CATALOG);
  }

  @Override
  public void execute(TaskContext context) throws HiveMetaException {
    if (!context.getCommandLine().hasOption(TO_CATALOG)) {
      throw new HiveMetaException("toCatalog must be set for mergeCatalog");
    }
    String fromCatalog = normalizeIdentifier(context.getCommandLine().getOptionValue(MERGE_CATALOG));
    String toCatalog = context.getCommandLine().getOptionValue(TO_CATALOG);

    LOG.info("Merging databases from " + fromCatalog + " to " + toCatalog);

    NestedScriptParser parser = context.getParser();
    Connection conn = context.getConnectionToMetastore(true);
    boolean success = false;
    long initTime, prevTime, curTime;

    try {
      // determine conflicts between catalogs first
      try (Statement stmt = conn.createStatement()) {
        initTime = System.currentTimeMillis();
        // TODO ensure both catalogs exist first.

        // Detect conflicting databases
        String conflicts = String.format(quote(DB_CONFLICTS_STMT, parser), fromCatalog, toCatalog);
        LOG.info("Determining name conflicts between databases across catalogs");
        LOG.info("[DB Conflicts] Executing SQL:" + conflicts);
        ResultSet rs = stmt.executeQuery(conflicts);
        boolean cleanMerge = true;
        while (rs.next()) {
          cleanMerge = false;
          LOG.info(
              "Name conflict(s) between merging catalogs, database " + rs.getString(1) + " exists in catalogs "
                  + rs.getString(2) + " and " + rs.getString(3));
        }

        if (!cleanMerge) {
          LOG.error("[ERROR] Please resolve the database name conflicts shown above manually and retry the mergeCatalog operation.");
          System.exit(1);
        }

        conn.setAutoCommit(false);
        String insert =
            String.format(quote(ADD_AUTOPURGE_TO_TABLE, parser), "EXTERNAL", "TRUE", "MANAGED_TABLE", fromCatalog);
        LOG.info("Setting external=true on all MANAGED tables in catalog " + fromCatalog);
        LOG.debug("[external table property] Executing SQL:" + insert);
        prevTime = System.currentTimeMillis();
        int count = stmt.executeUpdate(insert);
        curTime = System.currentTimeMillis();
        LOG.info("Set external.table.purge on " + count + " tables, time taken (ms):" + (curTime - prevTime));

        insert = String.format(quote(ADD_AUTOPURGE_TO_TABLE, parser), "external.table.purge", "true", "MANAGED_TABLE",
            fromCatalog);
        LOG.info("Setting external.table.purge=true on all MANAGED tables in catalog " + fromCatalog);
        LOG.debug("[external.table.purge] Executing SQL:" + insert);
        prevTime = curTime;
        count = stmt.executeUpdate(insert);
        curTime = System.currentTimeMillis();
        LOG.info("Set external.table.purge on " + count + " tables, time taken (ms):" + (curTime - prevTime));

        String update =
            String.format(quote(CONVERT_TABLE_TO_EXTERNAL, parser), "EXTERNAL_TABLE", "MANAGED_TABLE", fromCatalog);
        LOG.info("Setting tableType to EXTERNAL on all MANAGED tables in catalog " + fromCatalog);
        LOG.debug("[tableType=EXTERNAL_TABLE] Executing SQL:" + update);
        prevTime = curTime;
        count = stmt.executeUpdate(update);
        curTime = System.currentTimeMillis();
        LOG.info("Set tableType=EXTERNAL_TABLE on " + count + " tables, time taken (ms):" + (curTime - prevTime));

        String merge = String.format(quote(MERGE_CATALOG_STMT, parser), toCatalog, fromCatalog);
        LOG.info("Setting catalog names on all databases in catalog " + fromCatalog);
        LOG.debug("[catalog name] Executing SQL:" + merge);
        prevTime = curTime;
        count = stmt.executeUpdate(merge);
        curTime = System.currentTimeMillis();
        LOG.info("Changed catalog names on " + count + " databases, time taken (ms):" + (curTime - prevTime));

        if (count == 0) {
          LOG.info(count + " databases have been merged from catalog " + fromCatalog + " into " + toCatalog);
        }
        if (context.getCommandLine().hasOption("dryRun")) {
          conn.rollback();
        } else {
          conn.commit();
          LOG.info("Committed the changes. Total time taken (ms):" + (curTime - initTime));
        }
        success = true;
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to merge catalog", e);
    } finally {
      try {
        if (!success) {
          LOG.info("Rolling back transaction");
          conn.rollback();
        }
        conn.close();
      } catch (SQLException e) {
        // Not really much we can do here.
        LOG.error("Failed to rollback, everything will probably go bad from here.", e);
        try {
          conn.close();
        } catch (SQLException ex) {
          LOG.warn("Failed to close connection.", ex);
        }
      }
    }
  }

}
