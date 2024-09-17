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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.MetaStoreConnectionInfo;
import org.apache.hadoop.hive.metastore.tools.schematool.HiveSchemaHelper.NestedScriptParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;

/**
 * Print Hive version and schema version.
 */
class SchemaToolTaskValidate extends SchemaToolTask {
  private static final Logger LOG = LoggerFactory.getLogger(SchemaToolTaskValidate.class.getName());

  @Override
  void setCommandLineArguments(SchemaToolCommandLine cl) {
    // do nothing
  }

  @Override
  void execute() throws HiveMetaException {
    System.out.println("Starting metastore validation\n");
    Connection conn = schemaTool.getConnectionToMetastore(false);
    boolean success = true;
    try {
      success &= validateSchemaVersions();
      success &= validateSequences(conn);
      success &= validateSchemaTables(conn);
      success &= validateLocations(conn, schemaTool.getValidationServers());
      success &= validateColumnNullValues(conn);
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          throw new HiveMetaException("Failed to close metastore connection", e);
        }
      }
    }

    System.out.print("Done with metastore validation: ");
    if (!success) {
      System.out.println("[FAIL]");
      throw new HiveMetaException("Validation failed");
    } else {
      System.out.println("[SUCCESS]");
    }
  }

  boolean validateSchemaVersions() throws HiveMetaException {
    System.out.println("Validating schema version");
    try {
      String hiveSchemaVersion = schemaTool.getMetaStoreSchemaInfo().getHiveSchemaVersion();
      MetaStoreConnectionInfo connectionInfo = schemaTool.getConnectionInfo(false);
      String newSchemaVersion = schemaTool.getMetaStoreSchemaInfo().getMetaStoreSchemaVersion(connectionInfo);
      schemaTool.assertCompatibleVersion(hiveSchemaVersion, newSchemaVersion);
    } catch (HiveMetaException hme) {
      if (hme.getMessage().contains("Metastore schema version is not compatible") ||
          hme.getMessage().contains("Multiple versions were found in metastore") ||
          hme.getMessage().contains("Could not find version info in metastore VERSION table")) {
        System.err.println(hme.getMessage());
        System.out.println("[FAIL]\n");
        return false;
      } else {
        throw hme;
      }
    }
    System.out.println("[SUCCESS]\n");
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
  boolean validateSequences(Connection conn) throws HiveMetaException {
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

    System.out.println("Validating sequence number for SEQUENCE_TABLE");

    boolean isValid = true;
    try {
      Statement stmt = conn.createStatement();
      for (Map.Entry<String, Pair<String, String>> e : seqNameToTable.entrySet()) {
        String tableName = e.getValue().getLeft();
        String tableKey = e.getValue().getRight();
        String fullSequenceName = "org.apache.hadoop.hive.metastore.model." + e.getKey();
        String seqQuery = schemaTool.quote(QUERY_SEQ);
        String maxIdQuery = String.format(schemaTool.quote(QUERY_MAX_ID), tableKey, tableName);

        ResultSet res = stmt.executeQuery(maxIdQuery);
        if (res.next()) {
          long maxId = res.getLong(1);
          if (maxId > 0) {
            PreparedStatement stmtSeq = conn.prepareStatement(seqQuery);
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

      System.out.println(isValid ? "[SUCCESS]\n" :"[FAIL]\n");
      return isValid;
    } catch (SQLException e) {
        throw new HiveMetaException("Failed to validate sequence number for SEQUENCE_TABLE", e);
    }
  }

  @VisibleForTesting
  boolean validateSchemaTables(Connection conn) throws HiveMetaException {
    System.out.println("Validating metastore schema tables");
    String version = null;
    try {
      MetaStoreConnectionInfo connectionInfo = schemaTool.getConnectionInfo(false);
      version = schemaTool.getMetaStoreSchemaInfo().getMetaStoreSchemaVersion(connectionInfo);
    } catch (HiveMetaException he) {
      System.err.println("Failed to determine schema version from Hive Metastore DB. " + he.getMessage());
      System.out.println("Failed in schema table validation.");
      LOG.debug("Failed to determine schema version from Hive Metastore DB," + he.getMessage(), he);
      return false;
    }

    Connection hmsConn = schemaTool.getConnectionToMetastore(false);

    LOG.debug("Validating tables in the schema for version " + version);
    List<String> dbTables = new ArrayList<>();
    ResultSet rs = null;
    try {
      String schema = null;
      try {
        schema = hmsConn.getSchema();
      } catch (SQLFeatureNotSupportedException e) {
        LOG.debug("schema is not supported");
      }

      DatabaseMetaData metadata = conn.getMetaData();
      rs = metadata.getTables(null, schema, "%", new String[] {"TABLE"});

      while (rs.next()) {
        String table = rs.getString("TABLE_NAME");
        dbTables.add(table.toLowerCase());
        LOG.debug("Found table " + table + " in HMS dbstore");
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to retrieve schema tables from Hive Metastore DB," +
          e.getMessage(), e);
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          throw new HiveMetaException("Failed to close resultset", e);
        }
      }
    }

    // parse the schema file to determine the tables that are expected to exist
    // we are using oracle schema because it is simpler to parse, no quotes or backticks etc
    List<String> schemaTables = new ArrayList<>();
    List<String> subScripts   = new ArrayList<>();

    String baseDir    = new File(schemaTool.getMetaStoreSchemaInfo().getMetaStoreScriptDir()).getParent();
    String schemaFile = new File(schemaTool.getMetaStoreSchemaInfo().getMetaStoreScriptDir(),
        schemaTool.getMetaStoreSchemaInfo().generateInitFileName(version)).getPath();
    try {
      LOG.debug("Parsing schema script " + schemaFile);
      subScripts.addAll(findCreateTable(schemaFile, schemaTables));
      while (subScripts.size() > 0) {
        schemaFile = baseDir + "/" + schemaTool.getDbType() + "/" + subScripts.remove(0);
        LOG.debug("Parsing subscript " + schemaFile);
        subScripts.addAll(findCreateTable(schemaFile, schemaTables));
      }
    } catch (Exception e) {
      System.err.println("Exception in parsing schema file. Cause:" + e.getMessage());
      System.out.println("Failed in schema table validation.");
      return false;
    }

    LOG.debug("Schema tables:[ " + Arrays.toString(schemaTables.toArray()) + " ]");
    LOG.debug("DB tables:[ " + Arrays.toString(dbTables.toArray()) + " ]");

    // now diff the lists
    schemaTables.removeAll(dbTables);
    if (schemaTables.size() > 0) {
      Collections.sort(schemaTables);
      System.err.println("Table(s) [ " + Arrays.toString(schemaTables.toArray()) + " ] " +
          "are missing from the metastore database schema.");
      System.out.println("[FAIL]\n");
      return false;
    } else {
      System.out.println("[SUCCESS]\n");
      return true;
    }
  }

  @VisibleForTesting
  List<String> findCreateTable(String path, List<String> tableList) throws Exception {
    if (!(new File(path)).exists()) {
      throw new Exception(path + " does not exist. Potentially incorrect version in the metastore VERSION table");
    }

    List<String> subs = new ArrayList<>();
    NestedScriptParser sp = HiveSchemaHelper.getDbCommandParser(schemaTool.getDbType(), false);
    Pattern regexp = Pattern.compile("CREATE TABLE(\\s+IF NOT EXISTS)?\\s+(\\S+).*");

    try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
      String line = null;
      while ((line = reader.readLine()) != null) {
        if (sp.isNestedScript(line)) {
          String subScript = sp.getScriptName(line);
          LOG.debug("Schema subscript " + subScript + " found");
          subs.add(subScript);
          continue;
        }
        line = line.replaceAll("( )+", " "); //suppress multi-spaces
        line = line.replaceAll("\\(", " ");
        line = line.replaceAll("IF NOT EXISTS ", "");
        line = line.replaceAll("`", "");
        line = line.replaceAll("'", "");
        line = line.replaceAll("\"", "");
        Matcher matcher = regexp.matcher(line);

        if (matcher.find()) {
          String table = matcher.group(2);
          if (schemaTool.getDbType().equals("derby")) {
            table = table.replaceAll("APP\\.", "");
          }
          tableList.add(table.toLowerCase());
          LOG.debug("Found table " + table + " in the schema");
        }
      }
    } catch (IOException ex){
      throw new Exception(ex.getMessage());
    }

    return subs;
  }

  @VisibleForTesting
  boolean validateLocations(Connection conn, URI[] defaultServers) throws HiveMetaException {
    System.out.println("Validating DFS locations");
    boolean rtn = true;
    rtn &= checkMetaStoreDBLocation(conn, defaultServers);
    rtn &= checkMetaStoreTableLocation(conn, defaultServers);
    rtn &= checkMetaStorePartitionLocation(conn, defaultServers);
    rtn &= checkMetaStoreSkewedColumnsLocation(conn, defaultServers);
    System.out.println(rtn ? "[SUCCESS]\n" : "[FAIL]\n");
    return rtn;
  }

  private static final String QUERY_DB_LOCATION =
      "  select dbt.<q>DB_ID<q>, " +
      "         dbt.<q>NAME<q>, " +
      "         dbt.<q>DB_LOCATION_URI<q> " +
      "    from <q>DBS<q> dbt " +
      "order by dbt.<q>DB_ID<q> ";

  private boolean checkMetaStoreDBLocation(Connection conn, URI[] defaultServers) throws HiveMetaException {
    String dbLocQuery = schemaTool.quote(QUERY_DB_LOCATION);

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

  private boolean checkMetaStoreTableLocation(Connection conn, URI[] defaultServers)
      throws HiveMetaException {
    String tabIDRangeQuery = schemaTool.quote(TAB_ID_RANGE_QUERY);
    String tabLocQuery = String.format(schemaTool.quote(TAB_LOC_QUERY), TableType.VIRTUAL_VIEW);

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

  private boolean checkMetaStorePartitionLocation(Connection conn, URI[] defaultServers)
      throws HiveMetaException {
    String queryPartIDRange = schemaTool.quote(QUERY_PART_ID_RANGE);
    String queryPartLoc = schemaTool.quote(QUERY_PART_LOC);

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

  private boolean checkMetaStoreSkewedColumnsLocation(Connection conn, URI[] defaultServers)
      throws HiveMetaException {
    String querySkewedColIDRange = schemaTool.quote(QUERY_SKEWED_COL_ID_RANGE);
    String querySkewedColLoc = schemaTool.quote(QUERY_SKEWED_COL_LOC);

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
  boolean validateColumnNullValues(Connection conn) throws HiveMetaException {
    System.out.println("Validating columns for incorrect NULL values.");

    boolean isValid = true;
    String queryColumnNullValues = schemaTool.quote(QUERY_COLUMN_NULL_VALUES);

    try (Statement stmt = conn.createStatement();
         ResultSet res = stmt.executeQuery(queryColumnNullValues)) {
      while (res.next()) {
         long tableId = res.getLong("TBL_ID");
         String tableName = res.getString("TBL_NAME");
         String tableType = res.getString("TBL_TYPE");
         isValid = false;
         System.err.println("SD_ID in TBLS should not be NULL for Table Name=" + tableName + ", Table ID=" + tableId + ", Table Type=" + tableType);
      }

      System.out.println(isValid ? "[SUCCESS]\n" : "[FAIL]\n");
      return isValid;
    } catch(SQLException e) {
        throw new HiveMetaException("Failed to validate columns for incorrect NULL values", e);
    }
  }
}
