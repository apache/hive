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
package org.apache.hadoop.hive.metastore.tools.schematool.liquibase;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import liquibase.exception.LiquibaseException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.SchemaInfo;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.tools.schematool.SchemaToolCommandLine;
import org.apache.hadoop.hive.metastore.tools.schematool.commandparser.NestedScriptParser;
import org.apache.hadoop.hive.metastore.tools.schematool.task.SchemaToolTask;
import org.apache.hadoop.hive.metastore.tools.schematool.task.TaskContext;

import java.net.URI;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Performs various validations on the HMS schema to check if it matches the scripts.
 */
class MetastoreValidationTask extends SchemaToolTask {

  private URI[] validationServers = null; // The list of servers the database/partition/table can locate on
  private final ScriptScannerFactory scriptScannerFactory;

  @Override
  public Set<String> usedCommandLineArguments() {
    return Sets.newHashSet("servers");
  }

  @Override
  public void execute(TaskContext context) throws HiveMetaException {
    SchemaToolCommandLine commandLine = context.getCommandLine();
    if (commandLine.hasOption("servers")) {
      String servers = commandLine.getOptionValue("servers");
      if (StringUtils.isNotEmpty(servers)) {
        String[] strServers = servers.split(",");
        this.validationServers = new URI[strServers.length];
        for (int i = 0; i < validationServers.length; i++) {
          validationServers[i] = new Path(strServers[i]).toUri();
        }
      }
    }

    LOG.info("Starting metastore validation\n");

    NestedScriptParser parser = context.getParser();
    try (Connection conn = context.getConnectionToMetastore(false)) {
      boolean success = true;

      try {
        context.getLiquibase().validate();
      } catch (LiquibaseException e) {
        throw new HiveMetaException("Liquibase validation failed!", e);
      }

      success &= validateSchemaVersions(context);
      success &= validateSequences(conn, parser);
      success &= validateSchemaTables(conn, context);
      success &= validateLocations(conn, validationServers, parser);
      success &= validateColumnNullValues(conn, parser);

      System.out.print("Done with metastore validation: ");
      if (!success) {
        LOG.info("[FAIL]");
        throw new HiveMetaException("Validation failed");
      } else {
        LOG.info("[SUCCESS]");
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Metastore connection error.", e);
    }
  }

  @VisibleForTesting
  boolean validateSchemaVersions(TaskContext context) throws HiveMetaException {
    LOG.info("Validating schema version");
    try {
      SchemaInfo schemaInfo = context.getSchemaInfo();
      String minimumRequiredVersion = SchemaInfo.getRequiredHmsSchemaVersion();
      String dbVersion = schemaInfo.getSchemaVersion();
      if (!SchemaInfo.isVersionCompatible(minimumRequiredVersion, dbVersion)) {
        System.err.println("The HMS schema version (" + dbVersion +
            ") is not compatible with the minimum required version (" + minimumRequiredVersion + ")");
        LOG.info("[FAIL]\n");
        return false;
      }
    } catch (HiveMetaException hme) {
      if (hme.getMessage().contains("Multiple versions were found in metastore") ||
          hme.getMessage().contains("Could not find version info in metastore") ||
          hme.getMessage().contains("Failed to get schema version, Cause:")) {
        System.err.println(hme.getMessage());
        LOG.info("[FAIL]\n");
        return false;
      } else {
        throw hme;
      }
    }
    LOG.info("[SUCCESS]\n");
    return true;
  }

  private static final String QUERY_SEQ =
      "  select t.<q>NEXT_VAL<q>" +
      "    from <q>SEQUENCE_TABLE<q> t " +
      "   where t.<q>SEQUENCE_NAME<q> = ? " +
      "order by t.<q>SEQUENCE_NAME<q>";

  private static final String QUERY_MAX_ID =
      "select max(<q>%s<q>)" +
      "  from <q>%s<q>";

  @VisibleForTesting
  boolean validateSequences(Connection conn, NestedScriptParser parser) throws HiveMetaException {
    Map<String, Pair<String, String>> seqNameToTable =
        new ImmutableMap.Builder<String, Pair<String, String>>()
        .put("MDatabase", Pair.of("DBS", "DB_ID"))
        .put("MRole", Pair.of("ROLES", "ROLE_ID"))
        .put("MGlobalPrivilege", Pair.of("GLOBAL_PRIVS", "USER_GRANT_ID"))
        .put("MTable", Pair.of("TBLS","TBL_ID"))
        .put("MStorageDescriptor", Pair.of("SDS", "SD_ID"))
        .put("MSerDeInfo", Pair.of("SERDES", "SERDE_ID"))
        .put("MColumnDescriptor", Pair.of("CDS", "CD_ID"))
        .put("MTablePrivilege", Pair.of("TBL_PRIVS", "TBL_GRANT_ID"))
        .put("MTableColumnStatistics", Pair.of("TAB_COL_STATS", "CS_ID"))
        .put("MPartition", Pair.of("PARTITIONS", "PART_ID"))
        .put("MPartitionColumnStatistics", Pair.of("PART_COL_STATS", "CS_ID"))
        .put("MFunction", Pair.of("FUNCS", "FUNC_ID"))
        .put("MStringList", Pair.of("SKEWED_STRING_LIST", "STRING_LIST_ID"))
        .build();

    LOG.info("Validating sequence number for SEQUENCE_TABLE");

    boolean isValid = true;
    try (Statement stmt = conn.createStatement()) {
      for (Map.Entry<String, Pair<String, String>> e : seqNameToTable.entrySet()) {
        String tableName = e.getValue().getLeft();
        String tableKey = e.getValue().getRight();
        String fullSequenceName = "org.apache.hadoop.hive.metastore.model." + e.getKey();
        String seqQuery = quote(QUERY_SEQ, parser);
        String maxIdQuery = String.format(quote(QUERY_MAX_ID, parser), tableKey, tableName);

        ResultSet res = stmt.executeQuery(maxIdQuery);
        if (res.next()) {
          long maxId = res.getLong(1);
          if (maxId > 0) {
            try (PreparedStatement stmtSeq = conn.prepareStatement(seqQuery)) {
              stmtSeq.setString(1, fullSequenceName);
              ResultSet resSeq = stmtSeq.executeQuery();
              if (!resSeq.next()) {
                isValid = false;
                System.err.println("Missing SEQUENCE_NAME " + e.getKey() + " from SEQUENCE_TABLE");
              } else if (resSeq.getLong(1) < maxId) {
                isValid = false;
                System.err.println("NEXT_VAL for " + e.getKey() + " in SEQUENCE_TABLE < max(" + tableKey +
                    ") in " + tableName);
              }
            }
          }
        }
      }

      LOG.info(isValid ? "[SUCCESS]\n" :"[FAIL]\n");
      return isValid;
    } catch (SQLException e) {
        throw new HiveMetaException("Failed to validate sequence number for SEQUENCE_TABLE", e);
    }
  }

  @VisibleForTesting
  boolean validateSchemaTables(Connection conn, TaskContext context) throws HiveMetaException {
    LOG.info("Validating metastore schema tables");

    List<String> dbTables = new ArrayList<>();
    try {
      String metaStoreSchemaVersion = context.getSchemaInfo().getSchemaVersion();
      Connection hmsConn = context.getConnectionToMetastore(false);

      LOG.debug("Validating tables in the schema for version " + metaStoreSchemaVersion);

      String schema = null;
      try {
        schema = hmsConn.getSchema();
      } catch (SQLFeatureNotSupportedException e) {
        LOG.debug("schema is not supported");
      }

      DatabaseMetaData metadata = conn.getMetaData();
      try(ResultSet rs = metadata.getTables(null, schema, "%", new String[] {"TABLE"})) {

        while (rs.next()) {
          String table = rs.getString("TABLE_NAME");
          dbTables.add(table.toLowerCase());
          LOG.debug("Found table " + table + " in HMS dbstore");
        }
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to retrieve schema tables from Hive Metastore DB," +
          e.getMessage(), e);
    }

    // parse the schema file to determine the tables that are expected to exist
    Set<String> schemaTables = new TreeSet<>();

    try {
      for (String schemaFile : context.getSchemaInfo().getAppliedScripts()) {
        LOG.debug("Parsing schema script " + schemaFile);
        ScriptScanner scriptScanner = scriptScannerFactory.getTableFinderForScript(schemaFile);
        scriptScanner.findTablesInScript(schemaFile, context.getCommandLine().getDbType(), schemaTables);
      }
    } catch (HiveMetaException e) {
      System.err.println("Exception while trying to obtain the applied scripts. Cause:" + e.getMessage());
      LOG.info("Failed in schema table validation.");
      return false;
    } catch (Exception e) {
      System.err.println("Exception in parsing schema file. Cause:" + e.getMessage());
      LOG.info("Failed in schema table validation.");
      return false;
    }

    LOG.debug("Schema tables:[ " + Arrays.toString(schemaTables.toArray()) + " ]");
    LOG.debug("DB tables:[ " + Arrays.toString(dbTables.toArray()) + " ]");

    // now diff the lists
    dbTables.forEach(schemaTables::remove);
    if (schemaTables.size() > 0) {
      System.err.println("Table(s) [ " + Arrays.toString(schemaTables.toArray()) + " ] " +
          "are missing from the metastore database schema.");
      LOG.info("[FAIL]\n");
      return false;
    } else {
      LOG.info("[SUCCESS]\n");
      return true;
    }
  }

  @VisibleForTesting
  boolean validateLocations(Connection conn, URI[] defaultServers, NestedScriptParser parser) throws HiveMetaException {
    LOG.info("Validating DFS locations");
    boolean rtn = true;
    rtn &= checkMetaStoreDBLocation(conn, defaultServers, parser);
    rtn &= checkMetaStoreTableLocation(conn, defaultServers, parser);
    rtn &= checkMetaStorePartitionLocation(conn, defaultServers, parser);
    rtn &= checkMetaStoreSkewedColumnsLocation(conn, defaultServers, parser);
    LOG.info(rtn ? "[SUCCESS]\n" : "[FAIL]\n");
    return rtn;
  }

  private static final String QUERY_DB_LOCATION =
      "  select dbt.<q>DB_ID<q>, " +
      "         dbt.<q>NAME<q>, " +
      "         dbt.<q>DB_LOCATION_URI<q> " +
      "    from <q>DBS<q> dbt " +
      "order by dbt.<q>DB_ID<q> ";

  private boolean checkMetaStoreDBLocation(Connection conn, URI[] defaultServers, NestedScriptParser parser) throws HiveMetaException {
    String dbLocQuery = quote(QUERY_DB_LOCATION, parser);

    int numOfInvalid = 0;
    try (Statement stmt = conn.createStatement();
         ResultSet res = stmt.executeQuery(dbLocQuery)) {
      while (res.next()) {
        String locValue = res.getString(3);
        String dbName = getNameOrID(res, 2, 1);
        if (!checkLocation("Database " + dbName, locValue, defaultServers)) {
          numOfInvalid++;
        }
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get DB Location Info.", e);
    }
    return numOfInvalid == 0;
  }

  private static final String TAB_ID_RANGE_QUERY =
      "select max(<q>TBL_ID<q>), " +
      "       min(<q>TBL_ID<q>) " +
      "  from <q>TBLS<q> ";

  private static final String TAB_LOC_QUERY =
      "    select tbl.<q>TBL_ID<q>, " +
      "           tbl.<q>TBL_NAME<q>, " +
      "           sd.<q>LOCATION<q>, " +
      "           dbt.<q>DB_ID<q>, " +
      "           dbt.<q>NAME<q> " +
      "      from <q>TBLS<q> tbl " +
      "inner join <q>SDS<q> sd on sd.<q>SD_ID<q> = tbl.<q>SD_ID<q> " +
      "inner join <q>DBS<q> dbt on tbl.<q>DB_ID<q> = dbt.<q>DB_ID<q> " +
      "     where tbl.<q>TBL_TYPE<q> != '%s' " +
      "       and tbl.<q>TBL_ID<q> >= ? " +
      "       and tbl.<q>TBL_ID<q> <= ? " +
      "  order by tbl.<q>TBL_ID<q> ";

  private static final int TAB_LOC_CHECK_SIZE = 2000;

  private boolean checkMetaStoreTableLocation(Connection conn, URI[] defaultServers, NestedScriptParser parser)
      throws HiveMetaException {
    String tabIDRangeQuery = quote(TAB_ID_RANGE_QUERY, parser);
    String tabLocQuery = String.format(quote(TAB_LOC_QUERY, parser), TableType.VIRTUAL_VIEW);

    try {
      long maxID = 0, minID = 0;
      try (Statement stmt = conn.createStatement();
           ResultSet res = stmt.executeQuery(tabIDRangeQuery)) {
        if (res.next()) {
          maxID = res.getLong(1);
          minID = res.getLong(2);
        }
      }

      int numOfInvalid = 0;
      try (PreparedStatement pStmt = conn.prepareStatement(tabLocQuery)) {
        while (minID <= maxID) {
          pStmt.setLong(1, minID);
          pStmt.setLong(2, minID + TAB_LOC_CHECK_SIZE);
          try (ResultSet res = pStmt.executeQuery()) {
            while (res.next()) {
              String locValue = res.getString(3);
              String entity = "Database " + getNameOrID(res, 5, 4) + ", Table "  + getNameOrID(res, 2, 1);
              if (!checkLocation(entity, locValue, defaultServers)) {
                numOfInvalid++;
              }
            }
          }
          minID += TAB_LOC_CHECK_SIZE + 1;
        }
      }

      return numOfInvalid == 0;
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get Table Location Info.", e);
    }
  }

  private static final String QUERY_PART_ID_RANGE =
      "select max(<q>PART_ID<q>)," +
      "       min(<q>PART_ID<q>)" +
      "  from <q>PARTITIONS<q> ";

  private static final String QUERY_PART_LOC =
      "    select pt.<q>PART_ID<q>, " +
      "           pt.<q>PART_NAME<q>, " +
      "           sd.<q>LOCATION<q>, " +
      "           tbl.<q>TBL_ID<q>, " +
      "           tbl.<q>TBL_NAME<q>, " +
      "           dbt.<q>DB_ID<q>, " +
      "           dbt.<q>NAME<q> " +
      "      from <q>PARTITIONS<q> pt " +
      "inner join <q>SDS<q> sd on sd.<q>SD_ID<q> = pt.<q>SD_ID<q> " +
      "inner join <q>TBLS<q> tbl on tbl.<q>TBL_ID<q> = pt.<q>TBL_ID<q> " +
      "inner join <q>DBS<q> dbt on dbt.<q>DB_ID<q> = tbl.<q>DB_ID<q> " +
      "     where pt.<q>PART_ID<q> >= ? " +
      "       and pt.<q>PART_ID<q> <= ? " +
      "  order by tbl.<q>TBL_ID<q> ";

  private static final int PART_LOC_CHECK_SIZE = 2000;

  private boolean checkMetaStorePartitionLocation(Connection conn, URI[] defaultServers, NestedScriptParser parser)
      throws HiveMetaException {
    String queryPartIDRange = quote(QUERY_PART_ID_RANGE, parser);
    String queryPartLoc = quote(QUERY_PART_LOC, parser);

    try {
      long maxID = 0, minID = 0;
      try (Statement stmt = conn.createStatement();
           ResultSet res = stmt.executeQuery(queryPartIDRange)) {
        if (res.next()) {
          maxID = res.getLong(1);
          minID = res.getLong(2);
        }
      }

      int numOfInvalid = 0;
      try (PreparedStatement pStmt = conn.prepareStatement(queryPartLoc)) {
        while (minID <= maxID) {
          pStmt.setLong(1, minID);
          pStmt.setLong(2, minID + PART_LOC_CHECK_SIZE);
          try (ResultSet res = pStmt.executeQuery()) {
            while (res.next()) {
              String locValue = res.getString(3);
              String entity = "Database " + getNameOrID(res, 7, 6) + ", Table "  + getNameOrID(res, 5, 4) +
                  ", Partition " + getNameOrID(res, 2, 1);
              if (!checkLocation(entity, locValue, defaultServers)) {
                numOfInvalid++;
              }
            }
          }
          minID += PART_LOC_CHECK_SIZE + 1;
        }
      }

      return numOfInvalid == 0;
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get Partition Location Info.", e);
    }
  }

  private static final String QUERY_SKEWED_COL_ID_RANGE =
      "select max(<q>STRING_LIST_ID_KID<q>), " +
      "       min(<q>STRING_LIST_ID_KID<q>) " +
      "  from <q>SKEWED_COL_VALUE_LOC_MAP<q> ";

  private static final String QUERY_SKEWED_COL_LOC =
      "  select t.<q>TBL_NAME<q>, " +
      "         t.<q>TBL_ID<q>, " +
      "         sk.<q>STRING_LIST_ID_KID<q>, " +
      "         sk.<q>LOCATION<q>, " +
      "         db.<q>NAME<q>, " +
      "         db.<q>DB_ID<q> " +
      "    from <q>TBLS<q> t " +
      "    join <q>SDS<q> s on s.<q>SD_ID<q> = t.<q>SD_ID<q> " +
      "    join <q>DBS<q> db on db.<q>DB_ID<q> = t.<q>DB_ID<q> " +
      "    join <q>SKEWED_COL_VALUE_LOC_MAP<q> sk on sk.<q>SD_ID<q> = s.<q>SD_ID<q> " +
      "   where sk.<q>STRING_LIST_ID_KID<q> >= ? " +
      "     and sk.<q>STRING_LIST_ID_KID<q> <= ? " +
      "order by t.<q>TBL_ID<q> ";

  private static final int SKEWED_COL_LOC_CHECK_SIZE = 2000;

  private boolean checkMetaStoreSkewedColumnsLocation(Connection conn, URI[] defaultServers, NestedScriptParser parser)
      throws HiveMetaException {
    String querySkewedColIDRange = quote(QUERY_SKEWED_COL_ID_RANGE, parser);
    String querySkewedColLoc = quote(QUERY_SKEWED_COL_LOC, parser);

    try {
      long maxID = 0, minID = 0;
      try (Statement stmt = conn.createStatement();
           ResultSet res = stmt.executeQuery(querySkewedColIDRange)) {
        if (res.next()) {
          maxID = res.getLong(1);
          minID = res.getLong(2);
        }
      }

      int numOfInvalid = 0;
      try (PreparedStatement pStmt = conn.prepareStatement(querySkewedColLoc)) {
        while (minID <= maxID) {
          pStmt.setLong(1, minID);
          pStmt.setLong(2, minID + SKEWED_COL_LOC_CHECK_SIZE);
          try (ResultSet res = pStmt.executeQuery()) {
            while (res.next()) {
              String locValue = res.getString(4);
              String entity = "Database " + getNameOrID(res, 5, 6) + ", Table " + getNameOrID(res, 1, 2) +
                  ", String list " + res.getString(3);
              if (!checkLocation(entity, locValue, defaultServers)) {
                numOfInvalid++;
              }
            }
          }
          minID += SKEWED_COL_LOC_CHECK_SIZE + 1;
        }
      }

      return numOfInvalid == 0;
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get skewed columns location info.", e);
    }
  }

  /**
   * Check if the location is valid for the given entity.
   * @param entity          the entity to represent a database, partition or table
   * @param entityLocation  the location
   * @param defaultServers  a list of the servers that the location needs to match.
   *                        The location host needs to match one of the given servers.
   *                        If empty, then no check against such list.
   * @return true if the location is valid
   */
  private boolean checkLocation(String entity, String entityLocation, URI[] defaultServers) {
    boolean isValid = true;

    if (entityLocation == null) {
      System.err.println(entity + ", Error: empty location");
      isValid = false;
    } else {
      try {
        URI currentUri = new Path(entityLocation).toUri();
        String scheme = currentUri.getScheme();
        String path   = currentUri.getPath();
        if (StringUtils.isEmpty(scheme)) {
          System.err.println(entity + ", Location: "+ entityLocation + ", Error: missing location scheme.");
          isValid = false;
        } else if (StringUtils.isEmpty(path)) {
          System.err.println(entity + ", Location: "+ entityLocation + ", Error: missing location path.");
          isValid = false;
        } else if (ArrayUtils.isNotEmpty(defaultServers) && currentUri.getAuthority() != null) {
          String authority = currentUri.getAuthority();
          boolean matchServer = false;
          for(URI server : defaultServers) {
            if (StringUtils.equalsIgnoreCase(server.getScheme(), scheme) &&
                StringUtils.equalsIgnoreCase(server.getAuthority(), authority)) {
              matchServer = true;
              break;
            }
          }
          if (!matchServer) {
            System.err.println(entity + ", Location: " + entityLocation + ", Error: mismatched server.");
            isValid = false;
          }
        }

        // if there is no path element other than "/", report it but not fail
        if (isValid && StringUtils.containsOnly(path, "/")) {
          System.err.println(entity + ", Location: "+ entityLocation + ", Warn: location set to root, " +
              "not a recommended config.");
        }
      } catch (Exception pe) {
        System.err.println(entity + ", Error: invalid location - " + pe.getMessage());
        isValid =false;
      }
    }

    return isValid;
  }

  private String getNameOrID(ResultSet res, int nameInx, int idInx) throws SQLException {
    String itemName = res.getString(nameInx);
    return  (itemName == null || itemName.isEmpty()) ? "ID: " + res.getString(idInx) : "Name: " + itemName;
  }

  private static final String QUERY_COLUMN_NULL_VALUES =
      "  select t.*" +
      "    from <q>TBLS<q> t" +
      "   where t.<q>SD_ID<q> IS NULL" +
      "     and (t.<q>TBL_TYPE<q> = '" + TableType.EXTERNAL_TABLE + "' or" +
      "          t.<q>TBL_TYPE<q> = '" + TableType.MANAGED_TABLE + "') " +
      "order by t.<q>TBL_ID<q> ";

  @VisibleForTesting
  boolean validateColumnNullValues(Connection conn, NestedScriptParser parser) throws HiveMetaException {
    LOG.info("Validating columns for incorrect NULL values.");

    boolean isValid = true;
    String queryColumnNullValues = quote(QUERY_COLUMN_NULL_VALUES, parser);

    try (Statement stmt = conn.createStatement();
         ResultSet res = stmt.executeQuery(queryColumnNullValues)) {
      while (res.next()) {
         long tableId = res.getLong("TBL_ID");
         String tableName = res.getString("TBL_NAME");
         String tableType = res.getString("TBL_TYPE");
         isValid = false;
         System.err.println("SD_ID in TBLS should not be NULL for Table Name=" + tableName + ", Table ID=" + tableId + ", Table Type=" + tableType);
      }

      LOG.info(isValid ? "[SUCCESS]\n" : "[FAIL]\n");
      return isValid;
    } catch(SQLException e) {
        throw new HiveMetaException("Failed to validate columns for incorrect NULL values", e);
    }
  }

  public MetastoreValidationTask(ScriptScannerFactory scriptScannerFactory) {
    this.scriptScannerFactory = scriptScannerFactory;
  }
}
