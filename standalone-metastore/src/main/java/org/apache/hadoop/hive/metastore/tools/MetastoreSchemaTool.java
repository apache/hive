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
package org.apache.hadoop.hive.metastore.tools;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.IMetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper.MetaStoreConnectionInfo;
import org.apache.hadoop.hive.metastore.tools.HiveSchemaHelper.NestedScriptParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import sqlline.SqlLine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MetastoreSchemaTool {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreSchemaTool.class);
  private static final String PASSWD_MASK = "[passwd stripped]";

  @VisibleForTesting
  public static String homeDir;

  private String userName = null;
  private String passWord = null;
  private boolean dryRun = false;
  private boolean verbose = false;
  private String dbOpts = null;
  private String url = null;
  private String driver = null;
  private URI[] validationServers = null; // The list of servers the database/partition/table can locate on
  private String hiveUser; // Hive username, for use when creating the user, not for connecting
  private String hivePasswd; // Hive password, for use when creating the user, not for connecting
  private String hiveDb; // Hive database, for use when creating the user, not for connecting
  private final Configuration conf;
  private final String dbType;
  private final IMetaStoreSchemaInfo metaStoreSchemaInfo;
  private boolean needsQuotedIdentifier;

  private static String findHomeDir() {
    // If METASTORE_HOME is set, use it, else use HIVE_HOME for backwards compatibility.
    homeDir = homeDir == null ? System.getenv("METASTORE_HOME") : homeDir;
    return homeDir == null ? System.getenv("HIVE_HOME") : homeDir;
  }

  private MetastoreSchemaTool(String dbType) throws HiveMetaException {
    this(findHomeDir(), MetastoreConf.newMetastoreConf(), dbType);
  }

  MetastoreSchemaTool(String metastoreHome, Configuration conf, String dbType)
      throws HiveMetaException {
    if (metastoreHome == null || metastoreHome.isEmpty()) {
      throw new HiveMetaException("No Metastore home directory provided");
    }
    this.conf = conf;
    this.dbType = dbType;
    this.needsQuotedIdentifier = getDbCommandParser(dbType).needsQuotedIdentifier();
    this.metaStoreSchemaInfo = MetaStoreSchemaInfoFactory.get(conf, metastoreHome, dbType);
  }

  Configuration getConf() {
    return conf;
  }

  void setUrl(String url) {
    this.url = url;
  }

  void setDriver(String driver) {
    this.driver = driver;
  }

  void setUserName(String userName) {
    this.userName = userName;
  }

  void setPassWord(String passWord) {
    this.passWord = passWord;
  }

  void setDryRun(boolean dryRun) {
    this.dryRun = dryRun;
  }

  void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  private void setDbOpts(String dbOpts) {
    this.dbOpts = dbOpts;
  }

  private void setValidationServers(String servers) {
    if(StringUtils.isNotEmpty(servers)) {
      String[] strServers = servers.split(",");
      this.validationServers = new URI[strServers.length];
      for (int i = 0; i < validationServers.length; i++) {
        validationServers[i] = new Path(strServers[i]).toUri();
      }
    }
  }

  private void setHiveUser(String hiveUser) {
    this.hiveUser = hiveUser;
  }

  private void setHivePasswd(String hivePasswd) {
    this.hivePasswd = hivePasswd;
  }

  private void setHiveDb(String hiveDb) {
    this.hiveDb = hiveDb;
  }

  private static int usage(Options cmdLineOptions) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("schemaTool", cmdLineOptions);
    return 1;
  }

  Connection getConnectionToMetastore(boolean printInfo)
      throws HiveMetaException {
    return HiveSchemaHelper.getConnectionToMetastore(userName,
        passWord, url, driver, printInfo, conf, null);
  }

  private NestedScriptParser getDbCommandParser(String dbType) {
    return HiveSchemaHelper.getDbCommandParser(dbType, dbOpts, userName,
	passWord, conf, null, true);
  }

  /***
   * Print Hive version and schema version
   */
  private void showInfo() throws HiveMetaException {
    String hiveVersion = metaStoreSchemaInfo.getHiveSchemaVersion();
    String dbVersion = metaStoreSchemaInfo.getMetaStoreSchemaVersion(getConnectionInfo(true));
    System.out.println("Hive distribution version:\t " + hiveVersion);
    System.out.println("Metastore schema version:\t " + dbVersion);
    assertCompatibleVersion(hiveVersion, dbVersion);
  }

  boolean validateLocations(Connection conn, URI[] defaultServers) throws HiveMetaException {
    System.out.println("Validating DFS locations");
    boolean rtn;
    rtn = checkMetaStoreDBLocation(conn, defaultServers);
    rtn = checkMetaStoreTableLocation(conn, defaultServers) && rtn;
    rtn = checkMetaStorePartitionLocation(conn, defaultServers) && rtn;
    rtn = checkMetaStoreSkewedColumnsLocation(conn, defaultServers) && rtn;
    System.out.println((rtn ? "Succeeded" : "Failed") + " in DFS location validation.");
    return rtn;
  }

  private String getNameOrID(ResultSet res, int nameInx, int idInx) throws SQLException {
    String itemName = res.getString(nameInx);
    return  (itemName == null || itemName.isEmpty()) ? "ID: " + res.getString(idInx) : "Name: " + itemName;
  }

  private boolean checkMetaStoreDBLocation(Connection conn, URI[] defaultServers)
      throws HiveMetaException {
    String dbLoc;
    boolean isValid = true;
    int numOfInvalid = 0;
    if (needsQuotedIdentifier) {
      dbLoc = "select dbt.\"DB_ID\", dbt.\"NAME\", dbt.\"DB_LOCATION_URI\" from \"DBS\" dbt order by dbt.\"DB_ID\" ";
    } else {
      dbLoc = "select dbt.DB_ID, dbt.NAME, dbt.DB_LOCATION_URI from DBS dbt order by dbt.DB_ID";
    }

    try(Statement stmt = conn.createStatement();
        ResultSet res = stmt.executeQuery(dbLoc)) {
      while (res.next()) {
        String locValue = res.getString(3);
        String dbName = getNameOrID(res,2,1);
        if (!checkLocation("Database " + dbName, locValue, defaultServers)) {
          numOfInvalid++;
        }
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get DB Location Info.", e);
    }
    if (numOfInvalid > 0) {
      isValid = false;
    }
    return isValid;
  }

  private boolean checkMetaStoreTableLocation(Connection conn, URI[] defaultServers)
      throws HiveMetaException {
    String tabLoc, tabIDRange;
    boolean isValid = true;
    int numOfInvalid = 0;
    if (needsQuotedIdentifier) {
      tabIDRange = "select max(\"TBL_ID\"), min(\"TBL_ID\") from \"TBLS\" ";
    } else {
      tabIDRange = "select max(TBL_ID), min(TBL_ID) from TBLS";
    }

    if (needsQuotedIdentifier) {
      tabLoc = "select tbl.\"TBL_ID\", tbl.\"TBL_NAME\", sd.\"LOCATION\", dbt.\"DB_ID\", dbt.\"NAME\" from \"TBLS\" tbl inner join " +
    "\"SDS\" sd on tbl.\"SD_ID\" = sd.\"SD_ID\" and tbl.\"TBL_TYPE\" != '" + TableType.VIRTUAL_VIEW +
    "' and tbl.\"TBL_ID\" >= ? and tbl.\"TBL_ID\"<= ? " + "inner join \"DBS\" dbt on tbl.\"DB_ID\" = dbt.\"DB_ID\" order by tbl.\"TBL_ID\" ";
    } else {
      tabLoc = "select tbl.TBL_ID, tbl.TBL_NAME, sd.LOCATION, dbt.DB_ID, dbt.NAME from TBLS tbl join SDS sd on tbl.SD_ID = sd.SD_ID and tbl.TBL_TYPE !='"
      + TableType.VIRTUAL_VIEW + "' and tbl.TBL_ID >= ? and tbl.TBL_ID <= ?  inner join DBS dbt on tbl.DB_ID = dbt.DB_ID order by tbl.TBL_ID";
    }

    long maxID = 0, minID = 0;
    long rtnSize = 2000;

    try {
      Statement stmt = conn.createStatement();
      ResultSet res = stmt.executeQuery(tabIDRange);
      if (res.next()) {
        maxID = res.getLong(1);
        minID = res.getLong(2);
      }
      res.close();
      stmt.close();
      PreparedStatement pStmt = conn.prepareStatement(tabLoc);
      while (minID <= maxID) {
        pStmt.setLong(1, minID);
        pStmt.setLong(2, minID + rtnSize);
        res = pStmt.executeQuery();
        while (res.next()) {
          String locValue = res.getString(3);
          String entity = "Database " + getNameOrID(res, 5, 4) +
              ", Table "  + getNameOrID(res,2,1);
          if (!checkLocation(entity, locValue, defaultServers)) {
            numOfInvalid++;
          }
        }
        res.close();
        minID += rtnSize + 1;

      }
      pStmt.close();

    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get Table Location Info.", e);
    }
    if (numOfInvalid > 0) {
      isValid = false;
    }
    return isValid;
  }

  private boolean checkMetaStorePartitionLocation(Connection conn, URI[] defaultServers)
      throws HiveMetaException {
    String partLoc, partIDRange;
    boolean isValid = true;
    int numOfInvalid = 0;
    if (needsQuotedIdentifier) {
      partIDRange = "select max(\"PART_ID\"), min(\"PART_ID\") from \"PARTITIONS\" ";
    } else {
      partIDRange = "select max(PART_ID), min(PART_ID) from PARTITIONS";
    }

    if (needsQuotedIdentifier) {
      partLoc = "select pt.\"PART_ID\", pt.\"PART_NAME\", sd.\"LOCATION\", tbl.\"TBL_ID\", tbl.\"TBL_NAME\",dbt.\"DB_ID\", dbt.\"NAME\" from \"PARTITIONS\" pt "
           + "inner join \"SDS\" sd on pt.\"SD_ID\" = sd.\"SD_ID\" and pt.\"PART_ID\" >= ? and pt.\"PART_ID\"<= ? "
           + " inner join \"TBLS\" tbl on pt.\"TBL_ID\" = tbl.\"TBL_ID\" inner join "
           + "\"DBS\" dbt on tbl.\"DB_ID\" = dbt.\"DB_ID\" order by tbl.\"TBL_ID\" ";
    } else {
      partLoc = "select pt.PART_ID, pt.PART_NAME, sd.LOCATION, tbl.TBL_ID, tbl.TBL_NAME, dbt.DB_ID, dbt.NAME from PARTITIONS pt "
          + "inner join SDS sd on pt.SD_ID = sd.SD_ID and pt.PART_ID >= ? and pt.PART_ID <= ?  "
          + "inner join TBLS tbl on tbl.TBL_ID = pt.TBL_ID inner join DBS dbt on tbl.DB_ID = dbt.DB_ID order by tbl.TBL_ID ";
    }

    long maxID = 0, minID = 0;
    long rtnSize = 2000;

    try {
      Statement stmt = conn.createStatement();
      ResultSet res = stmt.executeQuery(partIDRange);
      if (res.next()) {
        maxID = res.getLong(1);
        minID = res.getLong(2);
      }
      res.close();
      stmt.close();
      PreparedStatement pStmt = conn.prepareStatement(partLoc);
      while (minID <= maxID) {
        pStmt.setLong(1, minID);
        pStmt.setLong(2, minID + rtnSize);
        res = pStmt.executeQuery();
        while (res.next()) {
          String locValue = res.getString(3);
          String entity = "Database " + getNameOrID(res,7,6) +
              ", Table "  + getNameOrID(res,5,4) +
              ", Partition " + getNameOrID(res,2,1);
          if (!checkLocation(entity, locValue, defaultServers)) {
            numOfInvalid++;
          }
        }
        res.close();
        minID += rtnSize + 1;
      }
      pStmt.close();
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get Partiton Location Info.", e);
    }
    if (numOfInvalid > 0) {
      isValid = false;
    }
    return isValid;
  }

  private boolean checkMetaStoreSkewedColumnsLocation(Connection conn, URI[] defaultServers)
      throws HiveMetaException {
    String skewedColLoc, skewedColIDRange;
    boolean isValid = true;
    int numOfInvalid = 0;
    if (needsQuotedIdentifier) {
      skewedColIDRange = "select max(\"STRING_LIST_ID_KID\"), min(\"STRING_LIST_ID_KID\") from \"SKEWED_COL_VALUE_LOC_MAP\" ";
    } else {
      skewedColIDRange = "select max(STRING_LIST_ID_KID), min(STRING_LIST_ID_KID) from SKEWED_COL_VALUE_LOC_MAP";
    }

    if (needsQuotedIdentifier) {
      skewedColLoc = "select t.\"TBL_NAME\", t.\"TBL_ID\", sk.\"STRING_LIST_ID_KID\", sk.\"LOCATION\", db.\"NAME\", db.\"DB_ID\" "
           + " from \"TBLS\" t, \"SDS\" s, \"DBS\" db, \"SKEWED_COL_VALUE_LOC_MAP\" sk "
           + "where sk.\"SD_ID\" = s.\"SD_ID\" and s.\"SD_ID\" = t.\"SD_ID\" and t.\"DB_ID\" = db.\"DB_ID\" and "
           + "sk.\"STRING_LIST_ID_KID\" >= ? and sk.\"STRING_LIST_ID_KID\" <= ? order by t.\"TBL_ID\" ";
    } else {
      skewedColLoc = "select t.TBL_NAME, t.TBL_ID, sk.STRING_LIST_ID_KID, sk.LOCATION, db.NAME, db.DB_ID from TBLS t, SDS s, DBS db, SKEWED_COL_VALUE_LOC_MAP sk "
           + "where sk.SD_ID = s.SD_ID and s.SD_ID = t.SD_ID and t.DB_ID = db.DB_ID and sk.STRING_LIST_ID_KID >= ? and sk.STRING_LIST_ID_KID <= ? order by t.TBL_ID ";
    }

    long maxID = 0, minID = 0;
    long rtnSize = 2000;

    try {
      Statement stmt = conn.createStatement();
      ResultSet res = stmt.executeQuery(skewedColIDRange);
      if (res.next()) {
        maxID = res.getLong(1);
        minID = res.getLong(2);
      }
      res.close();
      stmt.close();
      PreparedStatement pStmt = conn.prepareStatement(skewedColLoc);
      while (minID <= maxID) {
        pStmt.setLong(1, minID);
        pStmt.setLong(2, minID + rtnSize);
        res = pStmt.executeQuery();
        while (res.next()) {
          String locValue = res.getString(4);
          String entity = "Database " + getNameOrID(res,5,6) +
              ", Table " + getNameOrID(res,1,2) +
              ", String list " + res.getString(3);
          if (!checkLocation(entity, locValue, defaultServers)) {
            numOfInvalid++;
          }
        }
        res.close();
        minID += rtnSize + 1;
      }
      pStmt.close();
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to get skewed columns location info.", e);
    }
    if (numOfInvalid > 0) {
      isValid = false;
    }
    return isValid;
  }

  /**
   * Check if the location is valid for the given entity
   * @param entity          the entity to represent a database, partition or table
   * @param entityLocation  the location
   * @param defaultServers  a list of the servers that the location needs to match.
   *                        The location host needs to match one of the given servers.
   *                        If empty, then no check against such list.
   * @return true if the location is valid
   */
  private boolean checkLocation(
      String entity,
      String entityLocation,
      URI[] defaultServers) {
    boolean isValid = true;
    if (entityLocation == null) {
      logAndPrintToError(entity + ", Error: empty location");
      isValid = false;
    } else {
      try {
        URI currentUri = new Path(entityLocation).toUri();
        String scheme = currentUri.getScheme();
        String path   = currentUri.getPath();
        if (StringUtils.isEmpty(scheme)) {
          logAndPrintToError(entity + ", Location: "+ entityLocation + ", Error: missing location scheme.");
          isValid = false;
        } else if (StringUtils.isEmpty(path)) {
          logAndPrintToError(entity + ", Location: "+ entityLocation + ", Error: missing location path.");
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
            logAndPrintToError(entity + ", Location: " + entityLocation + ", Error: mismatched server.");
            isValid = false;
          }
        }

        // if there is no path element other than "/", report it but not fail
        if (isValid && StringUtils.containsOnly(path, "/")) {
          logAndPrintToError(entity + ", Location: "+ entityLocation + ", Warn: location set to root, not a recommended config.");
        }
      } catch (Exception pe) {
        logAndPrintToError(entity + ", Error: invalid location - " + pe.getMessage());
        isValid =false;
      }
    }

    return isValid;
  }

  // test the connection metastore using the config property
  private void testConnectionToMetastore() throws HiveMetaException {
    Connection conn = getConnectionToMetastore(true);
    try {
      conn.close();
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to close metastore connection", e);
    }
  }


  /**
   * check if the current schema version in metastore matches the Hive version
   */
  @VisibleForTesting
  void verifySchemaVersion() throws HiveMetaException {
    // don't check version if its a dry run
    if (dryRun) {
      return;
    }
    String newSchemaVersion = metaStoreSchemaInfo.getMetaStoreSchemaVersion(getConnectionInfo(false));
    // verify that the new version is added to schema
    assertCompatibleVersion(metaStoreSchemaInfo.getHiveSchemaVersion(), newSchemaVersion);
  }

  private void assertCompatibleVersion(String hiveSchemaVersion, String dbSchemaVersion)
      throws HiveMetaException {
    if (!metaStoreSchemaInfo.isVersionCompatible(hiveSchemaVersion, dbSchemaVersion)) {
      throw new HiveMetaException("Metastore schema version is not compatible. Hive Version: "
          + hiveSchemaVersion + ", Database Schema Version: " + dbSchemaVersion);
    }
  }

  /**
   * Perform metastore schema upgrade. extract the current schema version from metastore
   */
  void doUpgrade() throws HiveMetaException {
    String fromVersion =
      metaStoreSchemaInfo.getMetaStoreSchemaVersion(getConnectionInfo(false));
    if (fromVersion == null || fromVersion.isEmpty()) {
      throw new HiveMetaException("Schema version not stored in the metastore. " +
          "Metastore schema is too old or corrupt. Try specifying the version manually");
    }
    doUpgrade(fromVersion);
  }

  private MetaStoreConnectionInfo getConnectionInfo(boolean printInfo) {
    return new MetaStoreConnectionInfo(userName, passWord, url, driver, printInfo, conf,
        dbType, hiveDb);
  }
  /**
   * Perform metastore schema upgrade
   *
   * @param fromSchemaVer
   *          Existing version of the metastore. If null, then read from the metastore
   */
  void doUpgrade(String fromSchemaVer) throws HiveMetaException {
    if (metaStoreSchemaInfo.getHiveSchemaVersion().equals(fromSchemaVer)) {
      System.out.println("No schema upgrade required from version " + fromSchemaVer);
      return;
    }
    // Find the list of scripts to execute for this upgrade
    List<String> upgradeScripts =
        metaStoreSchemaInfo.getUpgradeScripts(fromSchemaVer);
    testConnectionToMetastore();
    System.out.println("Starting upgrade metastore schema from version " +
        fromSchemaVer + " to " + metaStoreSchemaInfo.getHiveSchemaVersion());
    String scriptDir = metaStoreSchemaInfo.getMetaStoreScriptDir();
    try {
      for (String scriptFile : upgradeScripts) {
        System.out.println("Upgrade script " + scriptFile);
        if (!dryRun) {
          runPreUpgrade(scriptDir, scriptFile);
          runSqlLine(scriptDir, scriptFile);
          System.out.println("Completed " + scriptFile);
        }
      }
    } catch (IOException eIO) {
      throw new HiveMetaException(
          "Upgrade FAILED! Metastore state would be inconsistent !!", eIO);
    }

    // Revalidated the new version after upgrade
    verifySchemaVersion();
  }

  /**
   * Initialize the metastore schema to current version
   *
   */
  void doInit() throws HiveMetaException {
    doInit(metaStoreSchemaInfo.getHiveSchemaVersion());

    // Revalidated the new version after upgrade
    verifySchemaVersion();
  }

  /**
   * Initialize the metastore schema
   *
   * @param toVersion
   *          If null then current hive version is used
   */
  void doInit(String toVersion) throws HiveMetaException {
    testConnectionToMetastore();
    System.out.println("Starting metastore schema initialization to " + toVersion);

    String initScriptDir = metaStoreSchemaInfo.getMetaStoreScriptDir();
    String initScriptFile = metaStoreSchemaInfo.generateInitFileName(toVersion);

    try {
      System.out.println("Initialization script " + initScriptFile);
      if (!dryRun) {
        runSqlLine(initScriptDir, initScriptFile);
        System.out.println("Initialization script completed");
      }
    } catch (IOException e) {
      throw new HiveMetaException("Schema initialization FAILED!" +
          " Metastore state would be inconsistent !!", e);
    }
  }

  private void doCreateUser() throws HiveMetaException {
    testConnectionToMetastore();
    System.out.println("Starting user creation");

    String scriptDir = metaStoreSchemaInfo.getMetaStoreScriptDir();
    String protoCreateFile = metaStoreSchemaInfo.getCreateUserScript();

    try {
      File createFile = subUserAndPassword(scriptDir, protoCreateFile);
      System.out.println("Creation script " + createFile.getAbsolutePath());
      if (!dryRun) {
        if ("oracle".equals(dbType)) oracleCreateUserHack(createFile);
        else runSqlLine(createFile.getParent(), createFile.getName());
        System.out.println("User creation completed");
      }
    } catch (IOException e) {
      throw new HiveMetaException("User creation FAILED!" +
          " Metastore unusable !!", e);
    }
  }

  private File subUserAndPassword(String parent, String filename) throws IOException {
    File createFile = File.createTempFile("create-hive-user-" + dbType, ".sql");
    BufferedWriter writer = new BufferedWriter(new FileWriter(createFile));
    File proto = new File(parent, filename);
    BufferedReader reader = new BufferedReader(new FileReader(proto));
    reader.lines()
        .map(s -> s.replace("_REPLACE_WITH_USER_", hiveUser)
            .replace("_REPLACE_WITH_PASSWD_", hivePasswd)
            .replace("_REPLACE_WITH_DB_", hiveDb))
        .forEach(s -> {
            try {
              writer.write(s);
              writer.newLine();
            } catch (IOException e) {
              throw new RuntimeException("Unable to write to tmp file ", e);
            }
          });
    reader.close();
    writer.close();
    return createFile;
  }

  private void oracleCreateUserHack(File createFile) throws HiveMetaException {
    LOG.debug("Found oracle, hacking our way through it rather than using SqlLine");
    try (BufferedReader reader = new BufferedReader(new FileReader(createFile))) {
      try (Connection conn = getConnectionToMetastore(false)) {
        try (Statement stmt = conn.createStatement()) {
          reader.lines()
              .forEach(s -> {
                assert s.charAt(s.length() - 1) == ';';
                try {
                  stmt.execute(s.substring(0, s.length() - 1));
                } catch (SQLException e) {
                  LOG.error("statement <" + s.substring(0, s.length() - 2) + "> failed", e);
                  throw new RuntimeException(e);
                }
              });
        }
      }
    } catch (IOException e) {
      LOG.error("Caught IOException trying to read modified create user script " +
          createFile.getAbsolutePath(), e);
      throw new HiveMetaException(e);
    } catch (HiveMetaException e) {
      LOG.error("Failed to connect to RDBMS", e);
      throw e;
    } catch (SQLException e) {
      LOG.error("Got SQLException", e);
    }
  }

  private int doValidate() throws HiveMetaException {
    System.out.println("Starting metastore validation\n");
    Connection conn = getConnectionToMetastore(false);
    boolean success = true;
    try {
      if (validateSchemaVersions()) {
        System.out.println("[SUCCESS]\n");
      } else {
        success = false;
        System.out.println("[FAIL]\n");
      }
      if (validateSequences(conn)) {
        System.out.println("[SUCCESS]\n");
      } else {
        success = false;
        System.out.println("[FAIL]\n");
      }
      if (validateSchemaTables(conn)) {
        System.out.println("[SUCCESS]\n");
      } else {
        success = false;
        System.out.println("[FAIL]\n");
      }
      if (validateLocations(conn, this.validationServers)) {
        System.out.println("[SUCCESS]\n");
      } else {
        System.out.println("[WARN]\n");
      }
      if (validateColumnNullValues(conn)) {
        System.out.println("[SUCCESS]\n");
      } else {
        System.out.println("[WARN]\n");
      }
    } finally {
      if (conn != null) {
        try {
          conn.close();
        } catch (SQLException e) {
          // Not a lot you can do here.
        }
      }
    }

    System.out.print("Done with metastore validation: ");
    if (!success) {
      System.out.println("[FAIL]");
      return 1;
    } else {
      System.out.println("[SUCCESS]");
      return 0;
    }
  }

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
        .put("MIndex", Pair.of("IDXS", "INDEX_ID"))
        .put("MStringList", Pair.of("SKEWED_STRING_LIST", "STRING_LIST_ID"))
        .build();

    System.out.println("Validating sequence number for SEQUENCE_TABLE");

    boolean isValid = true;
    try {
      Statement stmt = conn.createStatement();
      for (String seqName : seqNameToTable.keySet()) {
        String tableName = seqNameToTable.get(seqName).getLeft();
        String tableKey = seqNameToTable.get(seqName).getRight();
        String fullSequenceName = "org.apache.hadoop.hive.metastore.model." + seqName;
        String seqQuery = needsQuotedIdentifier ?
            ("select t.\"NEXT_VAL\" from \"SEQUENCE_TABLE\" t WHERE t.\"SEQUENCE_NAME\"=? order by t.\"SEQUENCE_NAME\" ")
            : ("select t.NEXT_VAL from SEQUENCE_TABLE t WHERE t.SEQUENCE_NAME=? order by t.SEQUENCE_NAME ");
        String maxIdQuery = needsQuotedIdentifier ?
            ("select max(\"" + tableKey + "\") from \"" + tableName + "\"")
            : ("select max(" + tableKey + ") from " + tableName);

        ResultSet res = stmt.executeQuery(maxIdQuery);
        if (res.next()) {
          long maxId = res.getLong(1);
          if (maxId > 0) {
            PreparedStatement pStmt = conn.prepareStatement(seqQuery);
            pStmt.setString(1, fullSequenceName);
            ResultSet resSeq = pStmt.executeQuery();
            if (!resSeq.next()) {
              isValid = false;
              logAndPrintToError("Missing SEQUENCE_NAME " + seqName + " from SEQUENCE_TABLE");
            } else if (resSeq.getLong(1) < maxId) {
              isValid = false;
              logAndPrintToError("NEXT_VAL for " + seqName + " in SEQUENCE_TABLE < max(" +
                  tableKey + ") in " + tableName);
            }
          }
        }
      }

      System.out.println((isValid ? "Succeeded" :"Failed") + " in sequence number validation for SEQUENCE_TABLE.");
      return isValid;
    } catch(SQLException e) {
        throw new HiveMetaException("Failed to validate sequence number for SEQUENCE_TABLE", e);
    }
  }

  boolean validateSchemaVersions() throws HiveMetaException {
    System.out.println("Validating schema version");
    try {
      String newSchemaVersion = metaStoreSchemaInfo.getMetaStoreSchemaVersion(getConnectionInfo(false));
      assertCompatibleVersion(metaStoreSchemaInfo.getHiveSchemaVersion(), newSchemaVersion);
    } catch (HiveMetaException hme) {
      if (hme.getMessage().contains("Metastore schema version is not compatible")
        || hme.getMessage().contains("Multiple versions were found in metastore")
        || hme.getMessage().contains("Could not find version info in metastore VERSION table")) {
        logAndPrintToError(hme.getMessage());
        System.out.println("Failed in schema version validation.");
        return false;
      } else {
        throw hme;
      }
    }
    System.out.println("Succeeded in schema version validation.");
    return true;
  }

  boolean validateSchemaTables(Connection conn) throws HiveMetaException {
    String version;
    ResultSet rs              = null;
    DatabaseMetaData metadata;
    List<String> dbTables     = new ArrayList<>();
    List<String> schemaTables = new ArrayList<>();
    List<String> subScripts   = new ArrayList<>();
    Connection hmsConn;

    System.out.println("Validating metastore schema tables");
    try {
      version = metaStoreSchemaInfo.getMetaStoreSchemaVersion(getConnectionInfo(false));
    } catch (HiveMetaException he) {
      logAndPrintToError("Failed to determine schema version from Hive Metastore DB. " + he.getMessage());
      System.out.println("Failed in schema table validation.");
      LOG.debug("Failed to determine schema version from Hive Metastore DB," + he.getMessage());
      return false;
    }

    // re-open the hms connection
    hmsConn = getConnectionToMetastore(false);

    LOG.debug("Validating tables in the schema for version " + version);
    try {
      metadata       = conn.getMetaData();
      String[] types = {"TABLE"};
      rs             = metadata.getTables(null, hmsConn.getSchema(), "%", types);
      String table;

      while (rs.next()) {
        table = rs.getString("TABLE_NAME");
        dbTables.add(table.toLowerCase());
        LOG.debug("Found table " + table + " in HMS dbstore");
      }
    } catch (SQLException e) {
      throw new HiveMetaException("Failed to retrieve schema tables from Hive Metastore DB," + e.getMessage());
    } finally {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e) {
          // Not a lot you can do here.
        }
      }
    }

    // parse the schema file to determine the tables that are expected to exist
    // we are using oracle schema because it is simpler to parse, no quotes or backticks etc
    String baseDir    = new File(metaStoreSchemaInfo.getMetaStoreScriptDir()).getParent();
    String schemaFile = new File(metaStoreSchemaInfo.getMetaStoreScriptDir(),
        metaStoreSchemaInfo.generateInitFileName(version)).getPath();
    try {
      LOG.debug("Parsing schema script " + schemaFile);
      subScripts.addAll(findCreateTable(schemaFile, schemaTables));
      while (subScripts.size() > 0) {
        schemaFile = baseDir + "/" + dbType + "/" + subScripts.remove(0);
        LOG.debug("Parsing subscript " + schemaFile);
        subScripts.addAll(findCreateTable(schemaFile, schemaTables));
      }
    } catch (Exception e) {
      logAndPrintToError("Exception in parsing schema file. Cause:" + e.getMessage());
      System.out.println("Failed in schema table validation.");
      return false;
    }

    LOG.debug("Schema tables:[ " + Arrays.toString(schemaTables.toArray()) + " ]");
    LOG.debug("DB tables:[ " + Arrays.toString(dbTables.toArray()) + " ]");
    // now diff the lists
    schemaTables.removeAll(dbTables);
    if (schemaTables.size() > 0) {
      Collections.sort(schemaTables);
      logAndPrintToError("Table(s) [ " + Arrays.toString(schemaTables.toArray())
          + " ] are missing from the metastore database schema.");
      System.out.println("Failed in schema table validation.");
      return false;
    } else {
      System.out.println("Succeeded in schema table validation.");
      return true;
    }
  }

  private List<String> findCreateTable(String path, List<String> tableList)
      throws Exception {
    NestedScriptParser sp           = HiveSchemaHelper.getDbCommandParser(dbType, true);
    Matcher matcher;
    Pattern regexp;
    List<String> subs               = new ArrayList<>();
    int groupNo                     = 2;

    regexp = Pattern.compile("CREATE TABLE(\\s+IF NOT EXISTS)?\\s+(\\S+).*");

    if (!(new File(path)).exists()) {
      throw new Exception(path + " does not exist. Potentially incorrect version in the metastore VERSION table");
    }

    try (
      BufferedReader reader = new BufferedReader(new FileReader(path))
    ){
      String line;
      while ((line = reader.readLine()) != null) {
        if (sp.isNestedScript(line)) {
          String subScript;
          subScript = sp.getScriptName(line);
          LOG.debug("Schema subscript " + subScript + " found");
          subs.add(subScript);
          continue;
        }
        line    = line.replaceAll("( )+", " "); //suppress multi-spaces
        line    = line.replaceAll("\\(", " ");
        line    = line.replaceAll("IF NOT EXISTS ", "");
        line    = line.replaceAll("`","");
        line    = line.replaceAll("'","");
        line    = line.replaceAll("\"","");
        matcher = regexp.matcher(line);

        if (matcher.find()) {
          String table = matcher.group(groupNo);
          if (dbType.equals("derby"))
            table  = table.replaceAll("APP\\.","");
          tableList.add(table.toLowerCase());
          LOG.debug("Found table " + table + " in the schema");
        }
      }
    } catch (IOException ex){
      throw new Exception(ex.getMessage());
    }

    return subs;
  }

  boolean validateColumnNullValues(Connection conn) throws HiveMetaException {
    System.out.println("Validating columns for incorrect NULL values.");
    boolean isValid = true;
    try {
      Statement stmt = conn.createStatement();
      String tblQuery = needsQuotedIdentifier ?
          ("select t.* from \"TBLS\" t WHERE t.\"SD_ID\" IS NULL and (t.\"TBL_TYPE\"='" + TableType.EXTERNAL_TABLE + "' or t.\"TBL_TYPE\"='" + TableType.MANAGED_TABLE + "') order by t.\"TBL_ID\" ")
          : ("select t.* from TBLS t WHERE t.SD_ID IS NULL and (t.TBL_TYPE='" + TableType.EXTERNAL_TABLE + "' or t.TBL_TYPE='" + TableType.MANAGED_TABLE + "') order by t.TBL_ID ");

      ResultSet res = stmt.executeQuery(tblQuery);
      while (res.next()) {
         long tableId = res.getLong("TBL_ID");
         String tableName = res.getString("TBL_NAME");
         String tableType = res.getString("TBL_TYPE");
         isValid = false;
         logAndPrintToError("SD_ID in TBLS should not be NULL for Table Name=" + tableName + ", Table ID=" + tableId + ", Table Type=" + tableType);
      }

      System.out.println((isValid ? "Succeeded" : "Failed") + " in column validation for incorrect NULL values.");
      return isValid;
    } catch(SQLException e) {
        throw new HiveMetaException("Failed to validate columns for incorrect NULL values", e);
    }
  }

  /**
   *  Run pre-upgrade scripts corresponding to a given upgrade script,
   *  if any exist. The errors from pre-upgrade are ignored.
   *  Pre-upgrade scripts typically contain setup statements which
   *  may fail on some database versions and failure is ignorable.
   *
   *  @param scriptDir upgrade script directory name
   *  @param scriptFile upgrade script file name
   */
  private void runPreUpgrade(String scriptDir, String scriptFile) {
    for (int i = 0;; i++) {
      String preUpgradeScript =
          metaStoreSchemaInfo.getPreUpgradeScriptName(i, scriptFile);
      File preUpgradeScriptFile = new File(scriptDir, preUpgradeScript);
      if (!preUpgradeScriptFile.isFile()) {
        break;
      }

      try {
        runSqlLine(scriptDir, preUpgradeScript);
        System.out.println("Completed " + preUpgradeScript);
      } catch (Exception e) {
        // Ignore the pre-upgrade script errors
        logAndPrintToError("Warning in pre-upgrade script " + preUpgradeScript + ": "
            + e.getMessage());
        if (verbose) {
          e.printStackTrace();
        }
      }
    }
  }

  /***
   * Run beeline with the given metastore script. Flatten the nested scripts
   * into single file.
   */
  private void runSqlLine(String scriptDir, String scriptFile)
      throws IOException, HiveMetaException {

    // This no longer does expansions of run commands in the files as it used to.  Instead it
    // depends on the developers to have already unrolled those in the files.
    runSqlLine(scriptDir + File.separatorChar + scriptFile);
  }

  // Generate the beeline args per hive conf and execute the given script
  void runSqlLine(String sqlScriptFile) throws IOException {
    CommandBuilder builder = new CommandBuilder(conf, url, driver,
        userName, passWord, sqlScriptFile);

    // run the script using SqlLine
    SqlLine sqlLine = new SqlLine();
    ByteArrayOutputStream outputForLog = null;
    if (!verbose) {
      OutputStream out;
      if (LOG.isDebugEnabled()) {
        out = outputForLog = new ByteArrayOutputStream();
      } else {
        out = new NullOutputStream();
      }
      sqlLine.setOutputStream(new PrintStream(out));
      System.setProperty("sqlline.silent", "true");
    }
    //sqlLine.getOpts().setAllowMultiLineCommand(false);
    //System.setProperty("sqlline.isolation","TRANSACTION_READ_COMMITTED");
    // We can be pretty sure that an entire line can be processed as a single command since
    // we always add a line separator at the end while calling dbCommandParser.buildCommand.
    //sqlLine.getOpts().setEntireLineAsCommand(true);
    LOG.info("Going to run command <" + builder.buildToLog() + ">");
    SqlLine.Status status = sqlLine.begin(builder.buildToRun(), null, false);
    if (LOG.isDebugEnabled() && outputForLog != null) {
      LOG.debug("Received following output from Sqlline:");
      LOG.debug(outputForLog.toString("UTF-8"));
    }
    if (status != SqlLine.Status.OK) {
      throw new IOException("Schema script failed, errorcode " + status);
    }
  }

  static class CommandBuilder {
    private final Configuration conf;
    private final String userName;
    private final String password;
    private final String sqlScriptFile;
    private final String driver;
    private final String url;

    CommandBuilder(Configuration conf, String url, String driver,
                   String userName, String password, String sqlScriptFile) {
      this.conf = conf;
      this.userName = userName;
      this.password = password;
      this.url = url;
      this.driver = driver;
      this.sqlScriptFile = sqlScriptFile;
    }

    String[] buildToRun() throws IOException {
      return argsWith(password);
    }

    String buildToLog() throws IOException {
      logScript();
      return StringUtils.join(argsWith(PASSWD_MASK), " ");
    }

    private String[] argsWith(String password) throws IOException {
      return new String[]
        {
          "-u", url == null ? MetastoreConf.getVar(conf, ConfVars.CONNECTURLKEY) : url,
          "-d", driver == null ? MetastoreConf.getVar(conf, ConfVars.CONNECTION_DRIVER) : driver,
          "-n", userName,
          "-p", password,
          "--isolation=TRANSACTION_READ_COMMITTED",
          "-f", sqlScriptFile
        };
    }

    private void logScript() throws IOException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to invoke file that contains:");
        try (BufferedReader reader = new BufferedReader(new FileReader(sqlScriptFile))) {
          String line;
          while ((line = reader.readLine()) != null) {
            LOG.debug("script: " + line);
          }
        }
      }
    }
  }

  // Create the required command line options
  @SuppressWarnings("static-access")
  private static void initOptions(Options cmdLineOptions) {
    Option help = new Option("help", "print this message");
    Option upgradeOpt = new Option("upgradeSchema", "Schema upgrade");
    Option upgradeFromOpt = OptionBuilder.withArgName("upgradeFrom").hasArg().
                withDescription("Schema upgrade from a version").
                create("upgradeSchemaFrom");
    Option initOpt = new Option("initSchema", "Schema initialization");
    Option initToOpt = OptionBuilder.withArgName("initTo").hasArg().
                withDescription("Schema initialization to a version").
                create("initSchemaTo");
    Option infoOpt = new Option("info", "Show config and schema details");
    Option validateOpt = new Option("validate", "Validate the database");
    Option createUserOpt = new Option("createUser", "Create the Hive user, use admin user and " +
        "password with this");

    OptionGroup optGroup = new OptionGroup();
    optGroup.addOption(upgradeOpt).addOption(initOpt).
                addOption(help).addOption(upgradeFromOpt).addOption(createUserOpt)
                .addOption(initToOpt).addOption(infoOpt).addOption(validateOpt);
    optGroup.setRequired(true);

    Option userNameOpt = OptionBuilder.withArgName("user")
                .hasArgs()
                .withDescription("Override config file user name")
                .create("userName");
    Option passwdOpt = OptionBuilder.withArgName("password")
                .hasArgs()
                 .withDescription("Override config file password")
                 .create("passWord");
    Option hiveUserOpt = OptionBuilder
        .hasArg()
        .withDescription("Hive user (for use with createUser)")
        .create("hiveUser");
    Option hivePasswdOpt = OptionBuilder
        .hasArg()
        .withDescription("Hive password (for use with createUser)")
        .create("hivePassword");
    Option hiveDbOpt = OptionBuilder
        .hasArg()
        .withDescription("Hive database (for use with createUser)")
        .create("hiveDb");
    Option dbTypeOpt = OptionBuilder.withArgName("databaseType")
                .hasArgs().withDescription("Metastore database type")
                .create("dbType");
    Option urlOpt = OptionBuilder.withArgName("url")
                .hasArgs().withDescription("connection url to the database")
                .create("url");
    Option driverOpt = OptionBuilder.withArgName("driver")
                .hasArgs().withDescription("driver name for connection")
                .create("driver");
    Option dbOpts = OptionBuilder.withArgName("databaseOpts")
                .hasArgs().withDescription("Backend DB specific options")
                .create("dbOpts");
    Option dryRunOpt = new Option("dryRun", "list SQL scripts (no execute)");
    Option verboseOpt = new Option("verbose", "only print SQL statements");
    Option serversOpt = OptionBuilder.withArgName("serverList")
        .hasArgs().withDescription("a comma-separated list of servers used in location validation in the format of scheme://authority (e.g. hdfs://localhost:8000)")
        .create("servers");
    cmdLineOptions.addOption(help);
    cmdLineOptions.addOption(dryRunOpt);
    cmdLineOptions.addOption(userNameOpt);
    cmdLineOptions.addOption(passwdOpt);
    cmdLineOptions.addOption(dbTypeOpt);
    cmdLineOptions.addOption(verboseOpt);
    cmdLineOptions.addOption(urlOpt);
    cmdLineOptions.addOption(driverOpt);
    cmdLineOptions.addOption(dbOpts);
    cmdLineOptions.addOption(serversOpt);
    cmdLineOptions.addOption(hiveUserOpt);
    cmdLineOptions.addOption(hivePasswdOpt);
    cmdLineOptions.addOption(hiveDbOpt);
    cmdLineOptions.addOptionGroup(optGroup);
  }

  private static void logAndPrintToError(String errmsg) {
    LOG.error(errmsg);
    System.err.println(errmsg);
  }

  public static void main(String[] args) {
    System.exit(run(args));
  }

  public static int run(String[] args) {
    LOG.debug("Going to run command: " + StringUtils.join(args, " "));
    CommandLineParser parser = new GnuParser();
    CommandLine line;
    String dbType;
    String schemaVer;
    Options cmdLineOptions = new Options();

    // Argument handling
    initOptions(cmdLineOptions);
    try {
      line = parser.parse(cmdLineOptions, args);
    } catch (ParseException e) {
      logAndPrintToError("HiveSchemaTool:Parsing failed.  Reason: " + e.getLocalizedMessage());
      return usage(cmdLineOptions);
    }

    assert line != null;
    if (line.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("schemaTool", cmdLineOptions);
      return 1;
    }

    if (line.hasOption("dbType")) {
      dbType = line.getOptionValue("dbType");
      if ((!dbType.equalsIgnoreCase(HiveSchemaHelper.DB_DERBY) &&
          !dbType.equalsIgnoreCase(HiveSchemaHelper.DB_MSSQL) &&
          !dbType.equalsIgnoreCase(HiveSchemaHelper.DB_MYSQL) &&
          !dbType.equalsIgnoreCase(HiveSchemaHelper.DB_POSTGRACE) && !dbType
          .equalsIgnoreCase(HiveSchemaHelper.DB_ORACLE))) {
        logAndPrintToError("Unsupported dbType " + dbType);
        return usage(cmdLineOptions);
      }
    } else {
      logAndPrintToError("no dbType supplied");
      return usage(cmdLineOptions);
    }

    System.setProperty(ConfVars.SCHEMA_VERIFICATION.toString(), "true");
    try {
      MetastoreSchemaTool schemaTool = new MetastoreSchemaTool(dbType);

      if (line.hasOption("userName")) {
        schemaTool.setUserName(line.getOptionValue("userName"));
      } else {
        schemaTool.setUserName(MetastoreConf.getVar(schemaTool.getConf(), ConfVars.CONNECTION_USER_NAME));
      }
      if (line.hasOption("passWord")) {
        schemaTool.setPassWord(line.getOptionValue("passWord"));
      } else {
        try {
          schemaTool.setPassWord(MetastoreConf.getPassword(schemaTool.getConf(), ConfVars.PWD));
        } catch (IOException err) {
          throw new HiveMetaException("Error getting metastore password", err);
        }
      }
      if (line.hasOption("hiveUser")) {
        schemaTool.setHiveUser(line.getOptionValue("hiveUser"));
      }
      if (line.hasOption("hivePassword")) {
        schemaTool.setHivePasswd(line.getOptionValue("hivePassword"));
      }
      if (line.hasOption("hiveDb")) {
        schemaTool.setHiveDb(line.getOptionValue("hiveDb"));
      }
      if (line.hasOption("url")) {
        schemaTool.setUrl(line.getOptionValue("url"));
      }
      if (line.hasOption("driver")) {
        schemaTool.setDriver(line.getOptionValue("driver"));
      }
      if (line.hasOption("dryRun")) {
        schemaTool.setDryRun(true);
      }
      if (line.hasOption("verbose")) {
        schemaTool.setVerbose(true);
      }
      if (line.hasOption("dbOpts")) {
        schemaTool.setDbOpts(line.getOptionValue("dbOpts"));
      }
      if (line.hasOption("validate") && line.hasOption("servers")) {
        schemaTool.setValidationServers(line.getOptionValue("servers"));
      }
      if (line.hasOption("info")) {
        schemaTool.showInfo();
      } else if (line.hasOption("upgradeSchema")) {
        schemaTool.doUpgrade();
      } else if (line.hasOption("upgradeSchemaFrom")) {
        schemaVer = line.getOptionValue("upgradeSchemaFrom");
        schemaTool.doUpgrade(schemaVer);
      } else if (line.hasOption("initSchema")) {
        schemaTool.doInit();
      } else if (line.hasOption("initSchemaTo")) {
        schemaVer = line.getOptionValue("initSchemaTo");
        schemaTool.doInit(schemaVer);
      } else if (line.hasOption("validate")) {
        return schemaTool.doValidate();
      } else if (line.hasOption("createUser")) {
        schemaTool.doCreateUser();
      } else {
        logAndPrintToError("no valid option supplied");
        return usage(cmdLineOptions);
      }
    } catch (HiveMetaException e) {
      logAndPrintToError(e.getMessage());
      if (e.getCause() != null) {
        Throwable t = e.getCause();
        logAndPrintToError("Underlying cause: "
            + t.getClass().getName() + " : "
            + t.getMessage());
        if (e.getCause() instanceof SQLException) {
          logAndPrintToError("SQL Error code: " + ((SQLException)t).getErrorCode());
        }
      }
      if (line.hasOption("verbose")) {
        e.printStackTrace();
      } else {
        logAndPrintToError("Use --verbose for detailed stacktrace.");
      }
      logAndPrintToError("*** schemaTool failed ***");
      return 1;
    }
    System.out.println("schemaTool completed");
    return 0;

  }
}
