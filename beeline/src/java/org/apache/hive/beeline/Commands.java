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
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;


public class Commands {
  private final BeeLine beeLine;

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


  public boolean history(String line) {
    List hist = beeLine.getConsoleReader().getHistory().getHistoryList();
    int index = 1;
    for (Iterator i = hist.iterator(); i.hasNext(); index++) {
      beeLine.output(beeLine.getColorBuffer().pad(index + ".", 6)
          .append(i.next().toString()));
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
    return metadata("getExportedKeys", new String[] {
        beeLine.getConnection().getCatalog(), null,
        arg1(line, "table name"),});
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
      return beeLine.error(beeLine.loc("no-current-connection"));
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
    return execute(line, false);
  }

  public boolean call(String line) {
    return execute(line, true);
  }

  private boolean execute(String line, boolean call) {
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
      //When using -e, console reader is not initialized and command is a single line
      while (beeLine.getConsoleReader() != null && !(line.trim().endsWith(";"))
        && beeLine.getOpts().isAllowMultiLineCommand()) {

        StringBuilder prompt = new StringBuilder(beeLine.getPrompt());
        for (int i = 0; i < prompt.length() - 1; i++) {
          if (prompt.charAt(i) != '>') {
            prompt.setCharAt(i, i % 2 == 0 ? '.' : ' ');
          }
        }

        String extra = beeLine.getConsoleReader().readLine(prompt.toString());
        if (!beeLine.isComment(extra)) {
          line += " " + extra;
        }
      }
    } catch (Exception e) {
      beeLine.handleException(e);
    }


    if (line.endsWith(";")) {
      line = line.substring(0, line.length() - 1);
    }

    if (!(beeLine.assertConnection())) {
      return false;
    }

    String sql = line;

    if (sql.startsWith(BeeLine.COMMAND_PREFIX)) {
      sql = sql.substring(1);
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

    try {
      Statement stmnt = null;
      boolean hasResults;

      try {
        long start = System.currentTimeMillis();

        if (call) {
          stmnt = beeLine.getDatabaseConnection().getConnection().prepareCall(sql);
          hasResults = ((CallableStatement) stmnt).execute();
        } else {
          stmnt = beeLine.createStatement();
          hasResults = stmnt.execute(sql);
        }

        beeLine.showWarnings();

        if (hasResults) {
          do {
            ResultSet rs = stmnt.getResultSet();
            try {
              int count = beeLine.print(rs);
              long end = System.currentTimeMillis();

              beeLine.info(beeLine.loc("rows-selected", count) + " "
                  + beeLine.locElapsedTime(end - start));
            } finally {
              rs.close();
            }
          } while (BeeLine.getMoreResults(stmnt));
        } else {
          int count = stmnt.getUpdateCount();
          long end = System.currentTimeMillis();
          beeLine.info(beeLine.loc("rows-affected", count)
              + " " + beeLine.locElapsedTime(end - start));
        }
      } finally {
        if (stmnt != null) {
          stmnt.close();
        }
      }
    } catch (Exception e) {
      return beeLine.error(e);
    }
    beeLine.showWarnings();
    return true;
  }


  public boolean quit(String line) {
    beeLine.setExit(true);
    close(null);
    return true;
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
      if (beeLine.getDatabaseConnection().getConnection() != null
          && !(beeLine.getDatabaseConnection().getConnection().isClosed())) {
        int index = beeLine.getDatabaseConnections().getIndex();
        beeLine.info(beeLine.loc("closing", index, beeLine.getDatabaseConnection()));
        beeLine.getDatabaseConnection().getConnection().close();
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
      props.load(new FileInputStream(parts[i]));
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
      props.setProperty("url", url);
    }
    if (driver != null) {
      props.setProperty("driver", driver);
    }
    if (user != null) {
      props.setProperty("user", user);
    }
    if (pass != null) {
      props.setProperty("password", pass);
    }

    return connect(props);
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
        "url",
        "javax.jdo.option.ConnectionURL",
        "ConnectionURL",
    });
    String driver = getProperty(props, new String[] {
        "driver",
        "javax.jdo.option.ConnectionDriverName",
        "ConnectionDriverName",
    });
    String username = getProperty(props, new String[] {
        "user",
        "javax.jdo.option.ConnectionUserName",
        "ConnectionUserName",
    });
    String password = getProperty(props, new String[] {
        "password",
        "javax.jdo.option.ConnectionPassword",
        "ConnectionPassword",
    });
    String auth = getProperty(props, new String[] {"auth"});

    if (url == null || url.length() == 0) {
      return beeLine.error("Property \"url\" is required");
    }
    if (driver == null || driver.length() == 0) {
      if (!beeLine.scanForDriver(url)) {
        return beeLine.error(beeLine.loc("no-driver", url));
      }
    }

    beeLine.info("Connecting to " + url);

    if (username == null) {
      username = beeLine.getConsoleReader().readLine("Enter username for " + url + ": ");
    }
    props.setProperty("user", username);
    if (password == null) {
      password = beeLine.getConsoleReader().readLine("Enter password for " + url + ": ",
          new Character('*'));
    }
    props.setProperty("password", password);

    if (auth == null) {
      auth = beeLine.getOpts().getAuthType();
    }
    if (auth != null) {
      props.setProperty("auth", auth);
    }

    try {
      beeLine.getDatabaseConnections().setConnection(
          new DatabaseConnection(beeLine, driver, url, props));
      beeLine.getDatabaseConnection().getConnection();

      beeLine.setCompletions();
      return true;
    } catch (SQLException sqle) {
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
