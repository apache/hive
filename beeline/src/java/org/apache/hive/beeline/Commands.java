/**
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

/*
 * This source file is based on code taken from SQLLine 1.0.2
 * See SQLLine notice in LICENSE
 */
package org.apache.hive.beeline;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.common.cli.ShellCmdExecutor;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveVariableSource;
import org.apache.hadoop.hive.conf.SystemVariables;
import org.apache.hadoop.hive.conf.VariableSubstitution;
import org.apache.hadoop.io.IOUtils;
import org.apache.hive.beeline.logs.BeelineInPlaceUpdateStream;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hive.jdbc.logs.InPlaceUpdateStream;

public class Commands {
  private final BeeLine beeLine;
  private static final int DEFAULT_QUERY_PROGRESS_INTERVAL = 1000;
  private static final int DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT = 10 * 1000;

  /**
   * @param beeLine
   */
  Commands(BeeLine beeLine) {
    this.beeLine = beeLine;
  }


  public boolean metadata(String line) {
    beeLine.debug(line);

    String[] parts = beeLine.split(line);
    List<String> params = new LinkedList<String>(Arrays.asList(parts));
    if (parts == null || parts.length == 0) {
      return dbinfo("");
    }

    params.remove(0);
    params.remove(0);
    beeLine.debug(params.toString());
    return metadata(parts[1],
        params.toArray(new String[0]));
  }


  public boolean metadata(String cmd, String[] args) {
    if (!(beeLine.assertConnection())) {
      return false;
    }

    try {
      Method[] m = beeLine.getDatabaseMetaData().getClass().getMethods();
      Set<String> methodNames = new TreeSet<String>();
      Set<String> methodNamesUpper = new TreeSet<String>();
      for (int i = 0; i < m.length; i++) {
        methodNames.add(m[i].getName());
        methodNamesUpper.add(m[i].getName().toUpperCase());
      }

      if (!methodNamesUpper.contains(cmd.toUpperCase())) {
        beeLine.error(beeLine.loc("no-such-method", cmd));
        beeLine.error(beeLine.loc("possible-methods"));
        for (Iterator<String> i = methodNames.iterator(); i.hasNext();) {
          beeLine.error("   " + i.next());
        }
        return false;
      }

      Object res = beeLine.getReflector().invoke(beeLine.getDatabaseMetaData(),
          DatabaseMetaData.class, cmd, Arrays.asList(args));

      if (res instanceof ResultSet) {
        ResultSet rs = (ResultSet) res;
        if (rs != null) {
          try {
            beeLine.print(rs);
          } finally {
            rs.close();
          }
        }
      } else if (res != null) {
        beeLine.output(res.toString());
      }
    } catch (Exception e) {
      return beeLine.error(e);
    }

    return true;
  }

  public boolean addlocaldrivername(String line) {
    String driverName = arg1(line, "driver class name");
    try {
      beeLine.setDrivers(Arrays.asList(beeLine.scanDrivers(false)));
    } catch (IOException e) {
      beeLine.error("Fail to scan drivers due to the exception:" + e);
      beeLine.error(e);
    }
    for (Driver d : beeLine.getDrivers()) {
      if (driverName.equals(d.getClass().getName())) {
        beeLine.addLocalDriverClazz(driverName);
        return true;
      }
    }
    beeLine.error("Fail to find a driver which contains the driver class");
    return false;
  }

  public boolean addlocaldriverjar(String line) {
    // If jar file is in the hdfs, it should be downloaded first.
    String jarPath = arg1(line, "jar path");
    File p = new File(jarPath);
    if (!p.exists()) {
      beeLine.error("The jar file in the path " + jarPath + " can't be found!");
      return false;
    }

    URLClassLoader classLoader = (URLClassLoader) Thread.currentThread().getContextClassLoader();
    try {
      beeLine.debug(jarPath + " is added to the local beeline.");
      URLClassLoader newClassLoader = new URLClassLoader(new URL[]{p.toURL()}, classLoader);

      Thread.currentThread().setContextClassLoader(newClassLoader);
      beeLine.setDrivers(Arrays.asList(beeLine.scanDrivers(false)));
    } catch (Exception e) {
      beeLine.error("Fail to add local jar due to the exception:" + e);
      beeLine.error(e);
    }
    return true;
  }

  public boolean history(String line) {
    Iterator hist = beeLine.getConsoleReader().getHistory().entries();
    String[] tmp;
    while(hist.hasNext()){
      tmp = hist.next().toString().split(":", 2);
      tmp[0] = Integer.toString(Integer.parseInt(tmp[0]) + 1);
      beeLine.output(beeLine.getColorBuffer().pad(tmp[0], 6)
          .append(":" + tmp[1]));
    }
    return true;
  }


  String arg1(String line, String paramname) {
    return arg1(line, paramname, null);
  }


  String arg1(String line, String paramname, String def) {
    String[] ret = beeLine.split(line);

    if (ret == null || ret.length != 2) {
      if (def != null) {
        return def;
      }
      throw new IllegalArgumentException(beeLine.loc("arg-usage",
          new Object[] {ret.length == 0 ? "" : ret[0],
              paramname}));
    }
    return ret[1];
  }


  public boolean indexes(String line) throws Exception {
    return metadata("getIndexInfo", new String[] {
        beeLine.getConnection().getCatalog(), null,
        arg1(line, "table name"),
        false + "",
        true + ""});
  }


  public boolean primarykeys(String line) throws Exception {
    return metadata("getPrimaryKeys", new String[] {
        beeLine.getConnection().getCatalog(), null,
        arg1(line, "table name"),});
  }


  public boolean exportedkeys(String line) throws Exception {
    return metadata("getExportedKeys",
        new String[] { beeLine.getConnection().getCatalog(), null, arg1(line, "table name"), });
  }


  public boolean importedkeys(String line) throws Exception {
    return metadata("getImportedKeys", new String[] {
        beeLine.getConnection().getCatalog(), null,
        arg1(line, "table name"),});
  }


  public boolean procedures(String line) throws Exception {
    return metadata("getProcedures", new String[] {
        beeLine.getConnection().getCatalog(), null,
        arg1(line, "procedure name pattern", "%"),});
  }


  public boolean tables(String line) throws Exception {
    return metadata("getTables", new String[] {
        beeLine.getConnection().getCatalog(), null,
        arg1(line, "table name", "%"), null});
  }


  public boolean typeinfo(String line) throws Exception {
    return metadata("getTypeInfo", new String[0]);
  }


  public boolean nativesql(String sql) throws Exception {
    if (sql.startsWith(BeeLine.COMMAND_PREFIX)) {
      sql = sql.substring(1);
    }
    if (sql.startsWith("native")) {
      sql = sql.substring("native".length() + 1);
    }
    String nat = beeLine.getConnection().nativeSQL(sql);
    beeLine.output(nat);
    return true;
  }


  public boolean columns(String line) throws Exception {
    return metadata("getColumns", new String[] {
        beeLine.getConnection().getCatalog(), null,
        arg1(line, "table name"), "%"});
  }


  public boolean dropall(String line) {
    if (beeLine.getDatabaseConnection() == null || beeLine.getDatabaseConnection().getUrl() == null) {
      return beeLine.error(beeLine.loc("no-current-connection"));
    }
    try {
      if (!(beeLine.getConsoleReader().readLine(beeLine.loc("really-drop-all")).equals("y"))) {
        return beeLine.error("abort-drop-all");
      }

      List<String> cmds = new LinkedList<String>();
      ResultSet rs = beeLine.getTables();
      try {
        while (rs.next()) {
          cmds.add("DROP TABLE "
              + rs.getString("TABLE_NAME") + ";");
        }
      } finally {
        try {
          rs.close();
        } catch (Exception e) {
        }
      }
      // run as a batch
      return beeLine.runCommands(cmds) == cmds.size();
    } catch (Exception e) {
      return beeLine.error(e);
    }
  }


  public boolean reconnect(String line) {
    if (beeLine.getDatabaseConnection() == null || beeLine.getDatabaseConnection().getUrl() == null) {
      // First, let's try connecting using the last successful url - if that fails, then we error out.
      String lastConnectedUrl = beeLine.getOpts().getLastConnectedUrl();
      if (lastConnectedUrl != null){
        Properties props = new Properties();
        props.setProperty("url",lastConnectedUrl);
        try {
          return connect(props);
        } catch (IOException e) {
          return beeLine.error(e);
        }
      } else {
        return beeLine.error(beeLine.loc("no-current-connection"));
      }
    }
    beeLine.info(beeLine.loc("reconnecting", beeLine.getDatabaseConnection().getUrl()));
    try {
      beeLine.getDatabaseConnection().reconnect();
    } catch (Exception e) {
      return beeLine.error(e);
    }
    return true;
  }


  public boolean scan(String line) throws IOException {
    TreeSet<String> names = new TreeSet<String>();

    if (beeLine.getDrivers() == null) {
      beeLine.setDrivers(Arrays.asList(beeLine.scanDrivers(line)));
    }

    beeLine.info(beeLine.loc("drivers-found-count", beeLine.getDrivers().size()));

    // unique the list
    for (Iterator<Driver> i = beeLine.getDrivers().iterator(); i.hasNext();) {
      names.add(i.next().getClass().getName());
    }

    beeLine.output(beeLine.getColorBuffer()
        .bold(beeLine.getColorBuffer().pad(beeLine.loc("compliant"), 10).getMono())
        .bold(beeLine.getColorBuffer().pad(beeLine.loc("jdbc-version"), 8).getMono())
        .bold(beeLine.getColorBuffer(beeLine.loc("driver-class")).getMono()));

    for (Iterator<String> i = names.iterator(); i.hasNext();) {
      String name = i.next().toString();
      try {
        Driver driver = (Driver) Class.forName(name).newInstance();
        ColorBuffer msg = beeLine.getColorBuffer()
            .pad(driver.jdbcCompliant() ? "yes" : "no", 10)
            .pad(driver.getMajorVersion() + "."
                + driver.getMinorVersion(), 8)
            .append(name);
        if (driver.jdbcCompliant()) {
          beeLine.output(msg);
        } else {
          beeLine.output(beeLine.getColorBuffer().red(msg.getMono()));
        }
      } catch (Throwable t) {
        beeLine.output(beeLine.getColorBuffer().red(name)); // error with driver
      }
    }
    return true;
  }


  public boolean save(String line) throws IOException {
    beeLine.info(beeLine.loc("saving-options", beeLine.getOpts().getPropertiesFile()));
    beeLine.getOpts().save();
    return true;
  }


  public boolean load(String line) throws IOException {
    beeLine.getOpts().load();
    beeLine.info(beeLine.loc("loaded-options", beeLine.getOpts().getPropertiesFile()));
    return true;
  }


  public boolean config(String line) {
    try {
      Properties props = beeLine.getOpts().toProperties();
      Set keys = new TreeSet(props.keySet());
      for (Iterator i = keys.iterator(); i.hasNext();) {
        String key = (String) i.next();
        beeLine.output(beeLine.getColorBuffer()
            .green(beeLine.getColorBuffer().pad(key.substring(
                beeLine.getOpts().PROPERTY_PREFIX.length()), 20)
                .getMono())
            .append(props.getProperty(key)));
      }
    } catch (Exception e) {
      return beeLine.error(e);
    }
    return true;
  }


  public boolean set(String line) {
    if (line == null || line.trim().equals("set")
        || line.length() == 0) {
      return config(null);
    }

    String[] parts = beeLine.split(line, 3, "Usage: set <key> <value>");
    if (parts == null) {
      return false;
    }

    String key = parts[1];
    String value = parts[2];
    boolean success = beeLine.getOpts().set(key, value, false);
    // if we autosave, then save
    if (success && beeLine.getOpts().getAutosave()) {
      try {
        beeLine.getOpts().save();
      } catch (Exception saveException) {
      }
    }
    return success;
  }


  public boolean commit(String line) throws SQLException {
    if (!(beeLine.assertConnection())) {
      return false;
    }
    if (!(beeLine.assertAutoCommit())) {
      return false;
    }
    try {
      long start = System.currentTimeMillis();
      beeLine.getDatabaseConnection().getConnection().commit();
      long end = System.currentTimeMillis();
      beeLine.showWarnings();
      beeLine.info(beeLine.loc("commit-complete")
          + " " + beeLine.locElapsedTime(end - start));
      return true;
    } catch (Exception e) {
      return beeLine.error(e);
    }
  }


  public boolean rollback(String line) throws SQLException {
    if (!(beeLine.assertConnection())) {
      return false;
    }
    if (!(beeLine.assertAutoCommit())) {
      return false;
    }
    try {
      long start = System.currentTimeMillis();
      beeLine.getDatabaseConnection().getConnection().rollback();
      long end = System.currentTimeMillis();
      beeLine.showWarnings();
      beeLine.info(beeLine.loc("rollback-complete")
          + " " + beeLine.locElapsedTime(end - start));
      return true;
    } catch (Exception e) {
      return beeLine.error(e);
    }
  }


  public boolean autocommit(String line) throws SQLException {
    if (!(beeLine.assertConnection())) {
      return false;
    }
    if (line.endsWith("on")) {
      beeLine.getDatabaseConnection().getConnection().setAutoCommit(true);
    } else if (line.endsWith("off")) {
      beeLine.getDatabaseConnection().getConnection().setAutoCommit(false);
    }
    beeLine.showWarnings();
    beeLine.autocommitStatus(beeLine.getDatabaseConnection().getConnection());
    return true;
  }


  public boolean dbinfo(String line) {
    if (!(beeLine.assertConnection())) {
      return false;
    }

    beeLine.showWarnings();
    int padlen = 50;

    String[] m = new String[] {
        "allProceduresAreCallable",
        "allTablesAreSelectable",
        "dataDefinitionCausesTransactionCommit",
        "dataDefinitionIgnoredInTransactions",
        "doesMaxRowSizeIncludeBlobs",
        "getCatalogSeparator",
        "getCatalogTerm",
        "getDatabaseProductName",
        "getDatabaseProductVersion",
        "getDefaultTransactionIsolation",
        "getDriverMajorVersion",
        "getDriverMinorVersion",
        "getDriverName",
        "getDriverVersion",
        "getExtraNameCharacters",
        "getIdentifierQuoteString",
        "getMaxBinaryLiteralLength",
        "getMaxCatalogNameLength",
        "getMaxCharLiteralLength",
        "getMaxColumnNameLength",
        "getMaxColumnsInGroupBy",
        "getMaxColumnsInIndex",
        "getMaxColumnsInOrderBy",
        "getMaxColumnsInSelect",
        "getMaxColumnsInTable",
        "getMaxConnections",
        "getMaxCursorNameLength",
        "getMaxIndexLength",
        "getMaxProcedureNameLength",
        "getMaxRowSize",
        "getMaxSchemaNameLength",
        "getMaxStatementLength",
        "getMaxStatements",
        "getMaxTableNameLength",
        "getMaxTablesInSelect",
        "getMaxUserNameLength",
        "getNumericFunctions",
        "getProcedureTerm",
        "getSchemaTerm",
        "getSearchStringEscape",
        "getSQLKeywords",
        "getStringFunctions",
        "getSystemFunctions",
        "getTimeDateFunctions",
        "getURL",
        "getUserName",
        "isCatalogAtStart",
        "isReadOnly",
        "nullPlusNonNullIsNull",
        "nullsAreSortedAtEnd",
        "nullsAreSortedAtStart",
        "nullsAreSortedHigh",
        "nullsAreSortedLow",
        "storesLowerCaseIdentifiers",
        "storesLowerCaseQuotedIdentifiers",
        "storesMixedCaseIdentifiers",
        "storesMixedCaseQuotedIdentifiers",
        "storesUpperCaseIdentifiers",
        "storesUpperCaseQuotedIdentifiers",
        "supportsAlterTableWithAddColumn",
        "supportsAlterTableWithDropColumn",
        "supportsANSI92EntryLevelSQL",
        "supportsANSI92FullSQL",
        "supportsANSI92IntermediateSQL",
        "supportsBatchUpdates",
        "supportsCatalogsInDataManipulation",
        "supportsCatalogsInIndexDefinitions",
        "supportsCatalogsInPrivilegeDefinitions",
        "supportsCatalogsInProcedureCalls",
        "supportsCatalogsInTableDefinitions",
        "supportsColumnAliasing",
        "supportsConvert",
        "supportsCoreSQLGrammar",
        "supportsCorrelatedSubqueries",
        "supportsDataDefinitionAndDataManipulationTransactions",
        "supportsDataManipulationTransactionsOnly",
        "supportsDifferentTableCorrelationNames",
        "supportsExpressionsInOrderBy",
        "supportsExtendedSQLGrammar",
        "supportsFullOuterJoins",
        "supportsGroupBy",
        "supportsGroupByBeyondSelect",
        "supportsGroupByUnrelated",
        "supportsIntegrityEnhancementFacility",
        "supportsLikeEscapeClause",
        "supportsLimitedOuterJoins",
        "supportsMinimumSQLGrammar",
        "supportsMixedCaseIdentifiers",
        "supportsMixedCaseQuotedIdentifiers",
        "supportsMultipleResultSets",
        "supportsMultipleTransactions",
        "supportsNonNullableColumns",
        "supportsOpenCursorsAcrossCommit",
        "supportsOpenCursorsAcrossRollback",
        "supportsOpenStatementsAcrossCommit",
        "supportsOpenStatementsAcrossRollback",
        "supportsOrderByUnrelated",
        "supportsOuterJoins",
        "supportsPositionedDelete",
        "supportsPositionedUpdate",
        "supportsSchemasInDataManipulation",
        "supportsSchemasInIndexDefinitions",
        "supportsSchemasInPrivilegeDefinitions",
        "supportsSchemasInProcedureCalls",
        "supportsSchemasInTableDefinitions",
        "supportsSelectForUpdate",
        "supportsStoredProcedures",
        "supportsSubqueriesInComparisons",
        "supportsSubqueriesInExists",
        "supportsSubqueriesInIns",
        "supportsSubqueriesInQuantifieds",
        "supportsTableCorrelationNames",
        "supportsTransactions",
        "supportsUnion",
        "supportsUnionAll",
        "usesLocalFilePerTable",
        "usesLocalFiles",
    };

    for (int i = 0; i < m.length; i++) {
      try {
        beeLine.output(beeLine.getColorBuffer().pad(m[i], padlen).append(
            "" + beeLine.getReflector().invoke(beeLine.getDatabaseMetaData(),
                m[i], new Object[0])));
      } catch (Exception e) {
        beeLine.output(beeLine.getColorBuffer().pad(m[i], padlen), false);
        beeLine.handleException(e);
      }
    }
    return true;
  }


  public boolean verbose(String line) {
    beeLine.info("verbose: on");
    return set("set verbose true");
  }


  public boolean outputformat(String line) {
    return set("set " + line);
  }


  public boolean brief(String line) {
    beeLine.info("verbose: off");
    return set("set verbose false");
  }


  public boolean isolation(String line) throws SQLException {
    if (!(beeLine.assertConnection())) {
      return false;
    }

    int i;

    if (line.endsWith("TRANSACTION_NONE")) {
      i = Connection.TRANSACTION_NONE;
    } else if (line.endsWith("TRANSACTION_READ_COMMITTED")) {
      i = Connection.TRANSACTION_READ_COMMITTED;
    } else if (line.endsWith("TRANSACTION_READ_UNCOMMITTED")) {
      i = Connection.TRANSACTION_READ_UNCOMMITTED;
    } else if (line.endsWith("TRANSACTION_REPEATABLE_READ")) {
      i = Connection.TRANSACTION_REPEATABLE_READ;
    } else if (line.endsWith("TRANSACTION_SERIALIZABLE")) {
      i = Connection.TRANSACTION_SERIALIZABLE;
    } else {
      return beeLine.error("Usage: isolation <TRANSACTION_NONE "
          + "| TRANSACTION_READ_COMMITTED "
          + "| TRANSACTION_READ_UNCOMMITTED "
          + "| TRANSACTION_REPEATABLE_READ "
          + "| TRANSACTION_SERIALIZABLE>");
    }

    beeLine.getDatabaseConnection().getConnection().setTransactionIsolation(i);

    int isol = beeLine.getDatabaseConnection().getConnection().getTransactionIsolation();
    final String isoldesc;
    switch (i)
    {
    case Connection.TRANSACTION_NONE:
      isoldesc = "TRANSACTION_NONE";
      break;
    case Connection.TRANSACTION_READ_COMMITTED:
      isoldesc = "TRANSACTION_READ_COMMITTED";
      break;
    case Connection.TRANSACTION_READ_UNCOMMITTED:
      isoldesc = "TRANSACTION_READ_UNCOMMITTED";
      break;
    case Connection.TRANSACTION_REPEATABLE_READ:
      isoldesc = "TRANSACTION_REPEATABLE_READ";
      break;
    case Connection.TRANSACTION_SERIALIZABLE:
      isoldesc = "TRANSACTION_SERIALIZABLE";
      break;
    default:
      isoldesc = "UNKNOWN";
    }

    beeLine.info(beeLine.loc("isolation-status", isoldesc));
    return true;
  }


  public boolean batch(String line) {
    if (!(beeLine.assertConnection())) {
      return false;
    }
    if (beeLine.getBatch() == null) {
      beeLine.setBatch(new LinkedList<String>());
      beeLine.info(beeLine.loc("batch-start"));
      return true;
    } else {
      beeLine.info(beeLine.loc("running-batch"));
      try {
        beeLine.runBatch(beeLine.getBatch());
        return true;
      } catch (Exception e) {
        return beeLine.error(e);
      } finally {
        beeLine.setBatch(null);
      }
    }
  }

  public boolean sql(String line) {
    return execute(line, false, false);
  }

  /**
   * This method is used for retrieving the latest configuration from hive server2.
   * It uses the set command processor.
   *
   * @return
   */
  private Map<String, String> getHiveVariables() {
    Map<String, String> result = new HashMap<>();
    BufferedRows rows = getConfInternal(true);
    if (rows != null) {
      while (rows.hasNext()) {
        Rows.Row row = (Rows.Row) rows.next();
        if (!row.isMeta) {
          result.put(row.values[0], row.values[1]);
        }
      }
    }
    return result;
  }

  /**
   * This method should only be used in CLI mode.
   *
   * @return the hive configuration from server side
   */
  public HiveConf getHiveConf(boolean call) {
    HiveConf hiveConf = beeLine.getOpts().getConf();
    if (hiveConf != null && call) {
      return hiveConf;
    } else {
      return getHiveConfHelper(call);
    }
  }

  public HiveConf getHiveConfHelper(boolean call) {
    HiveConf conf = new HiveConf();
    BufferedRows rows = getConfInternal(call);
    while (rows != null && rows.hasNext()) {
      addConf((Rows.Row) rows.next(), conf);
    }
    return conf;
  }

  /**
   * Use call statement to retrieve the configurations for substitution and sql for the substitution.
   *
   * @param call
   * @return
   */
  private BufferedRows getConfInternal(boolean call) {
    Statement stmnt = null;
    BufferedRows rows = null;
    try {
      boolean hasResults = false;
      DatabaseConnection dbconn = beeLine.getDatabaseConnection();
      Connection conn = null;
      if (dbconn != null)
        conn = dbconn.getConnection();
      if (conn != null) {
        if (call) {
          stmnt = conn.prepareCall("set");
          hasResults = ((CallableStatement) stmnt).execute();
        } else {
          stmnt = beeLine.createStatement();
          hasResults = stmnt.execute("set");
        }
      }
      if (hasResults) {
        ResultSet rs = stmnt.getResultSet();
        rows = new BufferedRows(beeLine, rs);
      }
    } catch (SQLException e) {
      beeLine.error(e);
    } finally {
      if (stmnt != null) {
        try {
          stmnt.close();
        } catch (SQLException e1) {
          beeLine.error(e1);
        }
      }
    }
    return rows;
  }

  private void addConf(Rows.Row r, HiveConf hiveConf) {
    if (r.isMeta) {
      return;
    }
    if (r.values == null || r.values[0] == null || r.values[0].isEmpty()) {
      return;
    }
    String val = r.values[0];
    if (r.values[0].startsWith(SystemVariables.SYSTEM_PREFIX) || r.values[0]
        .startsWith(SystemVariables.ENV_PREFIX)) {
      return;
    } else {
      String[] kv = val.split("=", 2);
      if (kv.length == 2)
        hiveConf.set(kv[0], kv[1]);
    }
  }

  /**
   * Extract and clean up the first command in the input.
   */
  private String getFirstCmd(String cmd, int length) {
    return cmd.substring(length).trim();
  }

  private String[] tokenizeCmd(String cmd) {
    return cmd.split("\\s+");
  }

  private boolean isSourceCMD(String cmd) {
    if (cmd == null || cmd.isEmpty())
      return false;
    String[] tokens = tokenizeCmd(cmd);
    return tokens[0].equalsIgnoreCase("source");
  }

  private boolean sourceFile(String cmd) {
    String[] tokens = tokenizeCmd(cmd);
    String cmd_1 = getFirstCmd(cmd, tokens[0].length());

    cmd_1 = substituteVariables(getHiveConf(false), cmd_1);
    File sourceFile = new File(cmd_1);
    if (!sourceFile.isFile()) {
      return false;
    } else {
      boolean ret;
      try {
        ret = sourceFileInternal(sourceFile);
      } catch (IOException e) {
        beeLine.error(e);
        return false;
      }
      return ret;
    }
  }

  private boolean sourceFileInternal(File sourceFile) throws IOException {
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(sourceFile));
      String extra = reader.readLine();
      String lines = null;
      while (extra != null) {
        if (beeLine.isComment(extra)) {
          continue;
        }
        if (lines == null) {
          lines = extra;
        } else {
          lines += "\n" + extra;
        }
        extra = reader.readLine();
      }
      String[] cmds = lines.split(";");
      for (String c : cmds) {
        c = c.trim();
        if (!executeInternal(c, false)) {
          return false;
        }
      }
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
    return true;
  }

  public String cliToBeelineCmd(String cmd) {
    if (cmd == null)
      return null;
    if (cmd.toLowerCase().equals("quit") || cmd.toLowerCase().equals("exit")) {
      return BeeLine.COMMAND_PREFIX + cmd;
    } else if (cmd.startsWith("!")) {
      String shell_cmd = cmd.substring(1);
      return "!sh " + shell_cmd;
    } else { // local mode
      // command like dfs
      return cmd;
    }
  }

  // Return false only occurred error when execution the sql and the sql should follow the rules
  // of beeline.
  private boolean executeInternal(String sql, boolean call) {
    if (!beeLine.isBeeLine()) {
      sql = cliToBeelineCmd(sql);
    }

    if (sql == null || sql.length() == 0) {
      return true;
    }

    if (beeLine.isComment(sql)) {
      //skip this and rest cmds in the line
      return true;
    }

    // is source CMD
    if (isSourceCMD(sql)) {
      return sourceFile(sql);
    }

    if (sql.startsWith(BeeLine.COMMAND_PREFIX)) {
      return beeLine.execCommandWithPrefix(sql);
    }

    String prefix = call ? "call" : "sql";

    if (sql.startsWith(prefix)) {
      sql = sql.substring(prefix.length());
    }

    // batch statements?
    if (beeLine.getBatch() != null) {
      beeLine.getBatch().add(sql);
      return true;
    }

    if (!(beeLine.assertConnection())) {
      return false;
    }

    ClientHook hook = ClientCommandHookFactory.get().getHook(beeLine, sql);

    try {
      Statement stmnt = null;
      boolean hasResults;
      Thread logThread = null;

      try {
        long start = System.currentTimeMillis();

        if (call) {
          stmnt = beeLine.getDatabaseConnection().getConnection().prepareCall(sql);
          hasResults = ((CallableStatement) stmnt).execute();
        } else {
          stmnt = beeLine.createStatement();
          if (beeLine.getOpts().isSilent()) {
            hasResults = stmnt.execute(sql);
          } else {
            InPlaceUpdateStream.EventNotifier eventNotifier =
                new InPlaceUpdateStream.EventNotifier();
            logThread = new Thread(createLogRunnable(stmnt, eventNotifier));
            logThread.setDaemon(true);
            logThread.start();
            if (stmnt instanceof HiveStatement) {
              HiveStatement hiveStatement = (HiveStatement) stmnt;
              hiveStatement.setInPlaceUpdateStream(
                  new BeelineInPlaceUpdateStream(
                      beeLine.getErrorStream(),
                      eventNotifier
                  ));
            }
            hasResults = stmnt.execute(sql);
            logThread.interrupt();
            logThread.join(DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT);
          }
        }

        beeLine.showWarnings();

        if (hasResults) {
          do {
            ResultSet rs = stmnt.getResultSet();
            try {
              int count = beeLine.print(rs);
              long end = System.currentTimeMillis();

              beeLine.info(
                  beeLine.loc("rows-selected", count) + " " + beeLine.locElapsedTime(end - start));
            } finally {
              if (logThread != null) {
                logThread.join(DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT);
                showRemainingLogsIfAny(stmnt);
                logThread = null;
              }
              rs.close();
            }
          } while (BeeLine.getMoreResults(stmnt));
        } else {
          int count = stmnt.getUpdateCount();
          long end = System.currentTimeMillis();
          beeLine.info(
              beeLine.loc("rows-affected", count) + " " + beeLine.locElapsedTime(end - start));
        }
      } finally {
        if (logThread != null) {
          if (!logThread.isInterrupted()) {
            logThread.interrupt();
          }
          logThread.join(DEFAULT_QUERY_PROGRESS_THREAD_TIMEOUT);
          showRemainingLogsIfAny(stmnt);
        }
        if (stmnt != null) {
          stmnt.close();
        }
      }
    } catch (Exception e) {
      return beeLine.error(e);
    }
    beeLine.showWarnings();
    if (hook != null) {
      hook.postHook(beeLine);
    }
    return true;
  }

  //startQuote use array type in order to pass int type as input/output parameter.
  //This method remove comment from current line of a query.
  //It does not remove comment like strings inside quotes.
  @VisibleForTesting
  String removeComments(String line, int[] startQuote) {
    if (line == null || line.isEmpty()) return line;
    if (startQuote[0] == -1 && beeLine.isComment(line)) return "";  //assume # can only be used at the beginning of line.
    StringBuilder builder = new StringBuilder();
    for (int index = 0; index < line.length(); index++) {
      if (startQuote[0] == -1 && index < line.length() - 1 && line.charAt(index) == '-' && line.charAt(index + 1) =='-') {
        return builder.toString().trim();
      }

      char letter = line.charAt(index);
      if (startQuote[0] == letter && (index == 0 || line.charAt(index -1) != '\\') ) {
        startQuote[0] = -1; // Turn escape off.
      } else if (startQuote[0] == -1 && (letter == '\'' || letter == '"') && (index == 0 || line.charAt(index -1) != '\\')) {
        startQuote[0] = letter; // Turn escape on.
      }

      builder.append(letter);
    }

    return builder.toString().trim();
  }

  /*
   * Check if the input line is a multi-line command which needs to read further
   */
  public String handleMultiLineCmd(String line) throws IOException {
    //When using -e, console reader is not initialized and command is always a single line
    int[] startQuote = {-1};
    line = removeComments(line,startQuote);
    while (isMultiLine(line) && beeLine.getOpts().isAllowMultiLineCommand()) {
      StringBuilder prompt = new StringBuilder(beeLine.getPrompt());
      if (!beeLine.getOpts().isSilent()) {
        for (int i = 0; i < prompt.length() - 1; i++) {
          if (prompt.charAt(i) != '>') {
            prompt.setCharAt(i, i % 2 == 0 ? '.' : ' ');
          }
        }
      }
      String extra;
      //avoid NPE below if for some reason -e argument has multi-line command
      if (beeLine.getConsoleReader() == null) {
        throw new RuntimeException("Console reader not initialized. This could happen when there "
            + "is a multi-line command using -e option and which requires further reading from console");
      }
      if (beeLine.getOpts().isSilent() && beeLine.getOpts().getScriptFile() != null) {
        extra = beeLine.getConsoleReader().readLine(null, jline.console.ConsoleReader.NULL_MASK);
      } else {
        extra = beeLine.getConsoleReader().readLine(prompt.toString());
      }

      if (extra == null) { //it happens when using -f and the line of cmds does not end with ;
        break;
      }
      extra = removeComments(extra,startQuote);
      if (extra != null && !extra.isEmpty()) {
        line += "\n" + extra;
      }
    }
    return line;
  }

  //returns true if statement represented by line is
  //not complete and needs additional reading from
  //console. Used in handleMultiLineCmd method
  //assumes line would never be null when this method is called
  private boolean isMultiLine(String line) {
    line = line.trim();
    if (line.endsWith(";") || beeLine.isComment(line)) {
      return false;
    }
    // handles the case like line = show tables; --test comment
    List<String> cmds = getCmdList(line, false);
    if (!cmds.isEmpty() && cmds.get(cmds.size() - 1).trim().startsWith("--")) {
      return false;
    }
    return true;
  }

  public boolean sql(String line, boolean entireLineAsCommand) {
    return execute(line, false, entireLineAsCommand);
  }

  public String substituteVariables(HiveConf conf, String line) {
    if (!beeLine.isBeeLine()) {
      // Substitution is only supported in non-beeline mode.
      return new VariableSubstitution(new HiveVariableSource() {
        @Override
        public Map<String, String> getHiveVariable() {
          return getHiveVariables();
        }
      }).substitute(conf, line);
    }
    return line;
  }

  public boolean sh(String line) {
    if (line == null || line.length() == 0) {
      return false;
    }

    if (!line.startsWith("sh")) {
      return false;
    }

    line = line.substring("sh".length()).trim();
    if (!beeLine.isBeeLine())
      line = substituteVariables(getHiveConf(false), line.trim());

    try {
      ShellCmdExecutor executor = new ShellCmdExecutor(line, beeLine.getOutputStream(),
          beeLine.getErrorStream());
      int ret = executor.execute();
      if (ret != 0) {
        beeLine.output("Command failed with exit code = " + ret);
        return false;
      }
      return true;
    } catch (Exception e) {
      beeLine.error("Exception raised from Shell command " + e);
      return false;
    }
  }

  public boolean call(String line) {
    return execute(line, true, false);
  }

  private boolean execute(String line, boolean call, boolean entireLineAsCommand) {
    if (line == null || line.length() == 0) {
      return false; // ???
    }

    // ### FIXME: doing the multi-line handling down here means
    // higher-level logic never sees the extra lines. So,
    // for example, if a script is being saved, it won't include
    // the continuation lines! This is logged as sf.net
    // bug 879518.

    // use multiple lines for statements not terminated by ";"
    try {
      line = handleMultiLineCmd(line);
    } catch (Exception e) {
      beeLine.handleException(e);
    }

    line = line.trim();
    List<String> cmdList = getCmdList(line, entireLineAsCommand);
    for (int i = 0; i < cmdList.size(); i++) {
      String sql = cmdList.get(i).trim();
      if (sql.length() != 0) {
        if (!executeInternal(sql, call)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Helper method to parse input from Beeline and convert it to a {@link List} of commands that
   * can be executed. This method contains logic for handling semicolons that are placed within
   * quotations. It iterates through each character in the line and checks to see if it is a ;, ',
   * or "
   */
  private List<String> getCmdList(String line, boolean entireLineAsCommand) {
    List<String> cmdList = new ArrayList<String>();
    if (entireLineAsCommand) {
      cmdList.add(line);
    } else {
      StringBuilder command = new StringBuilder();

      // Marker to track if there is starting double quote without an ending double quote
      boolean hasUnterminatedDoubleQuote = false;

      // Marker to track if there is starting single quote without an ending double quote
      boolean hasUnterminatedSingleQuote = false;

      // Index of the last seen semicolon in the given line
      int lastSemiColonIndex = 0;
      char[] lineChars = line.toCharArray();

      // Marker to track if the previous character was an escape character
      boolean wasPrevEscape = false;

      int index = 0;

      // Iterate through the line and invoke the addCmdPart method whenever a semicolon is seen that is not inside a
      // quoted string
      for (; index < lineChars.length; index++) {
        switch (lineChars[index]) {
          case '\'':
            // If a single quote is seen and the index is not inside a double quoted string and the previous character
            // was not an escape, then update the hasUnterminatedSingleQuote flag
            if (!hasUnterminatedDoubleQuote && !wasPrevEscape) {
              hasUnterminatedSingleQuote = !hasUnterminatedSingleQuote;
            }
            wasPrevEscape = false;
            break;
          case '\"':
            // If a double quote is seen and the index is not inside a single quoted string and the previous character
            // was not an escape, then update the hasUnterminatedDoubleQuote flag
            if (!hasUnterminatedSingleQuote && !wasPrevEscape) {
              hasUnterminatedDoubleQuote = !hasUnterminatedDoubleQuote;
            }
            wasPrevEscape = false;
            break;
          case ';':
            // If a semicolon is seen, and the line isn't inside a quoted string, then treat
            // line[lastSemiColonIndex] to line[index] as a single command
            if (!hasUnterminatedDoubleQuote && !hasUnterminatedSingleQuote) {
              addCmdPart(cmdList, command, line.substring(lastSemiColonIndex, index));
              lastSemiColonIndex = index + 1;
            }
            wasPrevEscape = false;
            break;
          case '\\':
            wasPrevEscape = !wasPrevEscape;
            break;
          default:
            wasPrevEscape = false;
            break;
        }
      }
      // If the line doesn't end with a ; or if the line is empty, add the cmd part
      if (lastSemiColonIndex != index || lineChars.length == 0) {
        addCmdPart(cmdList, command, line.substring(lastSemiColonIndex, index));
      }
    }
    return cmdList;
  }

  /**
   * Given a cmdpart (e.g. if a command spans multiple lines), add to the current command, and if
   * applicable add that command to the {@link List} of commands
   */
  private void addCmdPart(List<String> cmdList, StringBuilder command, String cmdpart) {
    if (cmdpart.endsWith("\\")) {
      command.append(cmdpart.substring(0, cmdpart.length() - 1)).append(";");
      return;
    } else {
      command.append(cmdpart);
    }
    cmdList.add(command.toString());
    command.setLength(0);
  }

  private Runnable createLogRunnable(final Statement statement,
      InPlaceUpdateStream.EventNotifier eventNotifier) {
    if (statement instanceof HiveStatement) {
      return new LogRunnable(this, (HiveStatement) statement, DEFAULT_QUERY_PROGRESS_INTERVAL,
          eventNotifier);
    } else {
      beeLine.debug(
          "The statement instance is not HiveStatement type: " + statement
              .getClass());
      return new Runnable() {
        @Override
        public void run() {
          // do nothing.
        }
      };
    }
  }

  private void error(Throwable throwable) {
    beeLine.error(throwable);
  }

  private void debug(String message) {
    beeLine.debug(message);
  }

  static class LogRunnable implements Runnable {
    private final Commands commands;
    private final HiveStatement hiveStatement;
    private final long queryProgressInterval;
    private final InPlaceUpdateStream.EventNotifier notifier;

    LogRunnable(Commands commands, HiveStatement hiveStatement,
        long queryProgressInterval, InPlaceUpdateStream.EventNotifier eventNotifier) {
      this.hiveStatement = hiveStatement;
      this.commands = commands;
      this.queryProgressInterval = queryProgressInterval;
      this.notifier = eventNotifier;
    }

    private void updateQueryLog() {
      try {
        List<String> queryLogs = hiveStatement.getQueryLog();
        for (String log : queryLogs) {
          commands.beeLine.info(log);
        }
        if (!queryLogs.isEmpty()) {
          notifier.operationLogShowedToUser();
        }
      } catch (SQLException e) {
        commands.error(new SQLWarning(e));
      }
    }

    @Override public void run() {
      try {
        while (hiveStatement.hasMoreLogs()) {
          /*
            get the operation logs once and print it, then wait till progress bar update is complete
            before printing the remaining logs.
          */
          if (notifier.canOutputOperationLogs()) {
            commands.debug("going to print operations logs");
            updateQueryLog();
            commands.debug("printed operations logs");
          }
          Thread.sleep(queryProgressInterval);
        }
      } catch (InterruptedException e) {
        commands.debug("Getting log thread is interrupted, since query is done!");
      } finally {
        commands.showRemainingLogsIfAny(hiveStatement);
      }
    }
  }

  private void showRemainingLogsIfAny(Statement statement) {
    if (statement instanceof HiveStatement) {
      HiveStatement hiveStatement = (HiveStatement) statement;
      List<String> logs = null;
      do {
        try {
          logs = hiveStatement.getQueryLog();
        } catch (SQLException e) {
          beeLine.error(new SQLWarning(e));
          return;
        }
        for (String log : logs) {
          beeLine.info(log);
        }
      } while (logs.size() > 0);
    } else {
      beeLine.debug("The statement instance is not HiveStatement type: " + statement.getClass());
    }
  }

  public boolean quit(String line) {
    beeLine.setExit(true);
    close(null);
    return true;
  }

  public boolean exit(String line) {
    return quit(line);
  }

  /**
   * Close all connections.
   */
  public boolean closeall(String line) {
    if (close(null)) {
      while (close(null)) {
        ;
      }
      return true;
    }
    return false;
  }


  /**
   * Close the current connection.
   */
  public boolean close(String line) {
    if (beeLine.getDatabaseConnection() == null) {
      return false;
    }
    try {
      if (beeLine.getDatabaseConnection().getCurrentConnection() != null
          && !(beeLine.getDatabaseConnection().getCurrentConnection().isClosed())) {
        int index = beeLine.getDatabaseConnections().getIndex();
        beeLine.info(beeLine.loc("closing", index, beeLine.getDatabaseConnection()));
        beeLine.getDatabaseConnection().getCurrentConnection().close();
      } else {
        beeLine.info(beeLine.loc("already-closed"));
      }
    } catch (Exception e) {
      return beeLine.error(e);
    }
    beeLine.getDatabaseConnections().remove();
    return true;
  }


  /**
   * Connect to the database defined in the specified properties file.
   */
  public boolean properties(String line) throws Exception {
    String example = "";
    example += "Usage: properties <properties file>" + BeeLine.getSeparator();

    String[] parts = beeLine.split(line);
    if (parts.length < 2) {
      return beeLine.error(example);
    }

    int successes = 0;

    for (int i = 1; i < parts.length; i++) {
      Properties props = new Properties();
      InputStream stream = new FileInputStream(parts[i]);
      try {
        props.load(stream);
      } finally {
        IOUtils.closeStream(stream);
      }
      if (connect(props)) {
        successes++;
      }
    }

    if (successes != (parts.length - 1)) {
      return false;
    } else {
      return true;
    }
  }


  public boolean connect(String line) throws Exception {
    String example = "Usage: connect <url> <username> <password> [driver]"
        + BeeLine.getSeparator();

    String[] parts = beeLine.split(line);
    if (parts == null) {
      return false;
    }

    if (parts.length < 2) {
      return beeLine.error(example);
    }

    String url = parts.length < 2 ? null : parts[1];
    String user = parts.length < 3 ? null : parts[2];
    String pass = parts.length < 4 ? null : parts[3];
    String driver = parts.length < 5 ? null : parts[4];

    Properties props = new Properties();
    if (url != null) {
      String saveUrl = getUrlToUse(url);
      props.setProperty(JdbcConnectionParams.PROPERTY_URL, saveUrl);
    }

    String value = null;
    if (driver != null) {
      props.setProperty(JdbcConnectionParams.PROPERTY_DRIVER, driver);
    } else {
      value = Utils.parsePropertyFromUrl(url, JdbcConnectionParams.PROPERTY_DRIVER);
      if (value != null) {
        props.setProperty(JdbcConnectionParams.PROPERTY_DRIVER, value);
      }
    }

    if (user != null) {
      props.setProperty(JdbcConnectionParams.AUTH_USER, user);
    } else {
      value = Utils.parsePropertyFromUrl(url, JdbcConnectionParams.AUTH_USER);
      if (value != null) {
        props.setProperty(JdbcConnectionParams.AUTH_USER, value);
      }
    }

    if (pass != null) {
      props.setProperty(JdbcConnectionParams.AUTH_PASSWD, pass);
    } else {
      value = Utils.parsePropertyFromUrl(url, JdbcConnectionParams.AUTH_PASSWD);
      if (value != null) {
        props.setProperty(JdbcConnectionParams.AUTH_PASSWD, value);
      }
    }

    value = Utils.parsePropertyFromUrl(url, JdbcConnectionParams.AUTH_TYPE);
    if (value != null) {
      props.setProperty(JdbcConnectionParams.AUTH_TYPE, value);
    }
    return connect(props);
  }

  private String getUrlToUse(String urlParam) {
    boolean useIndirectUrl = false;
    // If the url passed to us is a valid url with a protocol, we use it as-is
    // Otherwise, we assume it is a name of parameter that we have to get the url from
    try {
      URI tryParse = new URI(urlParam);
      if (tryParse.getScheme() == null){
        // param had no scheme, so not a URL
        useIndirectUrl = true;
      }
    } catch (URISyntaxException e){
      // param did not parse as a URL, so not a URL
      useIndirectUrl = true;
    }
    if (useIndirectUrl){
      // Use url param indirectly - as the name of an env var that contains the url
      // If the urlParam is "default", we would look for a BEELINE_URL_DEFAULT url
      String envUrl = beeLine.getOpts().getEnv().get(
          BeeLineOpts.URL_ENV_PREFIX + urlParam.toUpperCase());
      if (envUrl != null){
        return envUrl;
      }
    }
    return urlParam; // default return the urlParam passed in as-is.
  }

  private String getProperty(Properties props, String[] keys) {
    for (int i = 0; i < keys.length; i++) {
      String val = props.getProperty(keys[i]);
      if (val != null) {
        return val;
      }
    }

    for (Iterator i = props.keySet().iterator(); i.hasNext();) {
      String key = (String) i.next();
      for (int j = 0; j < keys.length; j++) {
        if (key.endsWith(keys[j])) {
          return props.getProperty(key);
        }
      }
    }

    return null;
  }

  public boolean connect(Properties props) throws IOException {
    String url = getProperty(props, new String[] {
        JdbcConnectionParams.PROPERTY_URL,
        "javax.jdo.option.ConnectionURL",
        "ConnectionURL",
    });
    String driver = getProperty(props, new String[] {
        JdbcConnectionParams.PROPERTY_DRIVER,
        "javax.jdo.option.ConnectionDriverName",
        "ConnectionDriverName",
    });
    String username = getProperty(props, new String[] {
        JdbcConnectionParams.AUTH_USER,
        "javax.jdo.option.ConnectionUserName",
        "ConnectionUserName",
    });
    String password = getProperty(props, new String[] {
        JdbcConnectionParams.AUTH_PASSWD,
        "javax.jdo.option.ConnectionPassword",
        "ConnectionPassword",
    });

    if (url == null || url.length() == 0) {
      return beeLine.error("Property \"url\" is required");
    }
    if (driver == null || driver.length() == 0) {
      if (!beeLine.scanForDriver(url)) {
        return beeLine.error(beeLine.loc("no-driver", url));
      }
    }

    String auth = getProperty(props, new String[] {JdbcConnectionParams.AUTH_TYPE});
    if (auth == null) {
      auth = beeLine.getOpts().getAuthType();
      if (auth != null) {
        props.setProperty(JdbcConnectionParams.AUTH_TYPE, auth);
      }
    }

    beeLine.info("Connecting to " + url);
    if (Utils.parsePropertyFromUrl(url, JdbcConnectionParams.AUTH_PRINCIPAL) == null) {
      String urlForPrompt = url.substring(0, url.contains(";") ? url.indexOf(';') : url.length());
      if (username == null) {
        username = beeLine.getConsoleReader().readLine("Enter username for " + urlForPrompt + ": ");
      }
      props.setProperty(JdbcConnectionParams.AUTH_USER, username);
      if (password == null) {
        password = beeLine.getConsoleReader().readLine("Enter password for " + urlForPrompt + ": ",
          new Character('*'));
      }
      props.setProperty(JdbcConnectionParams.AUTH_PASSWD, password);
    }

    try {
      beeLine.getDatabaseConnections().setConnection(
          new DatabaseConnection(beeLine, driver, url, props));
      beeLine.getDatabaseConnection().getConnection();

      if (!beeLine.isBeeLine()) {
        beeLine.updateOptsForCli();
      }
      beeLine.runInit();

      beeLine.setCompletions();
      beeLine.getOpts().setLastConnectedUrl(url);
      return true;
    } catch (SQLException sqle) {
      beeLine.getDatabaseConnections().remove();
      return beeLine.error(sqle);
    } catch (IOException ioe) {
      return beeLine.error(ioe);
    }
  }

  public boolean rehash(String line) {
    try {
      if (!(beeLine.assertConnection())) {
        return false;
      }
      if (beeLine.getDatabaseConnection() != null) {
        beeLine.getDatabaseConnection().setCompletions(false);
      }
      return true;
    } catch (Exception e) {
      return beeLine.error(e);
    }
  }


  /**
   * List the current connections
   */
  public boolean list(String line) {
    int index = 0;
    beeLine.info(beeLine.loc("active-connections", beeLine.getDatabaseConnections().size()));

    for (Iterator<DatabaseConnection> i = beeLine.getDatabaseConnections().iterator(); i.hasNext(); index++) {
      DatabaseConnection c = i.next();
      boolean closed = false;
      try {
        closed = c.getConnection().isClosed();
      } catch (Exception e) {
        closed = true;
      }

      beeLine.output(beeLine.getColorBuffer().pad(" #" + index + "", 5)
          .pad(closed ? beeLine.loc("closed") : beeLine.loc("open"), 9)
          .append(c.getUrl()));
    }

    return true;
  }

  public boolean all(String line) {
    int index = beeLine.getDatabaseConnections().getIndex();
    boolean success = true;

    for (int i = 0; i < beeLine.getDatabaseConnections().size(); i++) {
      beeLine.getDatabaseConnections().setIndex(i);
      beeLine.output(beeLine.loc("executing-con", beeLine.getDatabaseConnection()));
      // ### FIXME: this is broken for multi-line SQL
      success = sql(line.substring("all ".length())) && success;
    }

    // restore index
    beeLine.getDatabaseConnections().setIndex(index);
    return success;
  }


  public boolean go(String line) {
    String[] parts = beeLine.split(line, 2, "Usage: go <connection index>");
    if (parts == null) {
      return false;
    }
    int index = Integer.parseInt(parts[1]);
    if (!(beeLine.getDatabaseConnections().setIndex(index))) {
      beeLine.error(beeLine.loc("invalid-connection", "" + index));
      list(""); // list the current connections
      return false;
    }
    return true;
  }


  /**
   * Save or stop saving a script to a file
   */
  public boolean script(String line) {
    if (beeLine.getScriptOutputFile() == null) {
      return startScript(line);
    } else {
      return stopScript(line);
    }
  }


  /**
   * Stop writing to the script file and close the script.
   */
  private boolean stopScript(String line) {
    try {
      beeLine.getScriptOutputFile().close();
    } catch (Exception e)
    {
      beeLine.handleException(e);
    }

    beeLine.output(beeLine.loc("script-closed", beeLine.getScriptOutputFile()));
    beeLine.setScriptOutputFile(null);
    return true;
  }


  /**
   * Start writing to the specified script file.
   */
  private boolean startScript(String line) {
    if (beeLine.getScriptOutputFile() != null) {
      return beeLine.error(beeLine.loc("script-already-running", beeLine.getScriptOutputFile()));
    }

    String[] parts = beeLine.split(line, 2, "Usage: script <filename>");
    if (parts == null) {
      return false;
    }

    try {
      beeLine.setScriptOutputFile(new OutputFile(parts[1]));
      beeLine.output(beeLine.loc("script-started", beeLine.getScriptOutputFile()));
      return true;
    } catch (Exception e) {
      return beeLine.error(e);
    }
  }


  /**
   * Run a script from the specified file.
   */
  public boolean run(String line) {
    String[] parts = beeLine.split(line, 2, "Usage: run <scriptfile>");
    if (parts == null) {
      return false;
    }

    List<String> cmds = new LinkedList<String>();

    try {
      BufferedReader reader = new BufferedReader(new FileReader(
          parts[1]));
      try {
        // ### NOTE: fix for sf.net bug 879427
        StringBuilder cmd = null;
        for (;;) {
          String scriptLine = reader.readLine();

          if (scriptLine == null) {
            break;
          }

          String trimmedLine = scriptLine.trim();
          if (beeLine.getOpts().getTrimScripts()) {
            scriptLine = trimmedLine;
          }

          if (cmd != null) {
            // we're continuing an existing command
            cmd.append(" \n");
            cmd.append(scriptLine);
            if (trimmedLine.endsWith(";")) {
              // this command has terminated
              cmds.add(cmd.toString());
              cmd = null;
            }
          } else {
            // we're starting a new command
            if (beeLine.needsContinuation(scriptLine)) {
              // multi-line
              cmd = new StringBuilder(scriptLine);
            } else {
              // single-line
              cmds.add(scriptLine);
            }
          }
        }

        if (cmd != null) {
          // ### REVIEW: oops, somebody left the last command
          // unterminated; should we fix it for them or complain?
          // For now be nice and fix it.
          cmd.append(";");
          cmds.add(cmd.toString());
        }
      } finally {
        reader.close();
      }

      // success only if all the commands were successful
      return beeLine.runCommands(cmds) == cmds.size();
    } catch (Exception e) {
      return beeLine.error(e);
    }
  }


  /**
   * Save or stop saving all output to a file.
   */
  public boolean record(String line) {
    if (beeLine.getRecordOutputFile() == null) {
      return startRecording(line);
    } else {
      return stopRecording(line);
    }
  }


  /**
   * Stop writing output to the record file.
   */
  private boolean stopRecording(String line) {
    try {
      beeLine.getRecordOutputFile().close();
    } catch (Exception e) {
      beeLine.handleException(e);
    }
    beeLine.setRecordOutputFile(null);
    beeLine.output(beeLine.loc("record-closed", beeLine.getRecordOutputFile()));
    return true;
  }


  /**
   * Start writing to the specified record file.
   */
  private boolean startRecording(String line) {
    if (beeLine.getRecordOutputFile() != null) {
      return beeLine.error(beeLine.loc("record-already-running", beeLine.getRecordOutputFile()));
    }

    String[] parts = beeLine.split(line, 2, "Usage: record <filename>");
    if (parts == null) {
      return false;
    }

    try {
      OutputFile recordOutput = new OutputFile(parts[1]);
      beeLine.output(beeLine.loc("record-started", recordOutput));
      beeLine.setRecordOutputFile(recordOutput);
      return true;
    } catch (Exception e) {
      return beeLine.error(e);
    }
  }




  public boolean describe(String line) throws SQLException {
    String[] table = beeLine.split(line, 2, "Usage: describe <table name>");
    if (table == null) {
      return false;
    }

    ResultSet rs;

    if (table[1].equals("tables")) {
      rs = beeLine.getTables();
    } else {
      rs = beeLine.getColumns(table[1]);
    }

    if (rs == null) {
      return false;
    }

    beeLine.print(rs);
    rs.close();
    return true;
  }


  public boolean help(String line) {
    String[] parts = beeLine.split(line);
    String cmd = parts.length > 1 ? parts[1] : "";
    int count = 0;
    TreeSet<ColorBuffer> clist = new TreeSet<ColorBuffer>();

    for (int i = 0; i < beeLine.commandHandlers.length; i++) {
      if (cmd.length() == 0 ||
          Arrays.asList(beeLine.commandHandlers[i].getNames()).contains(cmd)) {
        clist.add(beeLine.getColorBuffer().pad("!" + beeLine.commandHandlers[i].getName(), 20)
            .append(beeLine.wrap(beeLine.commandHandlers[i].getHelpText(), 60, 20)));
      }
    }

    for (Iterator<ColorBuffer> i = clist.iterator(); i.hasNext();) {
      beeLine.output(i.next());
    }

    if (cmd.length() == 0) {
      beeLine.output("");
      beeLine.output(beeLine.loc("comments", beeLine.getApplicationContactInformation()));
    }

    return true;
  }


  public boolean manual(String line) throws IOException {
    InputStream in = BeeLine.class.getResourceAsStream("manual.txt");
    if (in == null) {
      return beeLine.error(beeLine.loc("no-manual"));
    }

    BufferedReader breader = new BufferedReader(
        new InputStreamReader(in));
    String man;
    int index = 0;
    while ((man = breader.readLine()) != null) {
      index++;
      beeLine.output(man);

      // silly little pager
      if (index % (beeLine.getOpts().getMaxHeight() - 1) == 0) {
        String ret = beeLine.getConsoleReader().readLine(beeLine.loc("enter-for-more"));
        if (ret != null && ret.startsWith("q")) {
          break;
        }
      }
    }
    breader.close();
    return true;
  }
}
