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

package org.apache.hive.jdbc;

import java.net.URI;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hive.service.cli.thrift.TStatus;
import org.apache.hive.service.cli.thrift.TStatusCode;

public class Utils {
  /**
    * The required prefix for the connection URL.
    */
  public static final String URL_PREFIX = "jdbc:hive2://";

  /**
    * If host is provided, without a port.
    */
  public static final String DEFAULT_PORT = "10000";

  /**
   * Hive's default database name
   */
  public static final String DEFAULT_DATABASE = "default";

  private static final String URI_JDBC_PREFIX = "jdbc:";

  public static class JdbcConnectionParams {
    private String host = null;
    private int port;
    private String dbName = DEFAULT_DATABASE;
    private Map<String,String> hiveConfs = new HashMap<String,String>();
    private Map<String,String> hiveVars = new HashMap<String,String>();
    private Map<String,String> sessionVars = new HashMap<String,String>();
    private boolean isEmbeddedMode = false;

    public JdbcConnectionParams() {
    }

    public String getHost() {
      return host;
    }
    public int getPort() {
      return port;
    }
    public String getDbName() {
      return dbName;
    }
    public Map<String, String> getHiveConfs() {
      return hiveConfs;
    }
    public Map<String,String> getHiveVars() {
      return hiveVars;
    }
    public boolean isEmbeddedMode() {
      return isEmbeddedMode;
    }
    public Map<String, String> getSessionVars() {
      return sessionVars;
    }

    public void setHost(String host) {
      this.host = host;
    }
    public void setPort(int port) {
      this.port = port;
    }
    public void setDbName(String dbName) {
      this.dbName = dbName;
    }
    public void setHiveConfs(Map<String, String> hiveConfs) {
      this.hiveConfs = hiveConfs;
    }
    public void setHiveVars(Map<String,String> hiveVars) {
      this.hiveVars = hiveVars;
    }
    public void setEmbeddedMode(boolean embeddedMode) {
      this.isEmbeddedMode = embeddedMode;
    }
    public void setSessionVars(Map<String, String> sessionVars) {
      this.sessionVars = sessionVars;
    }
  }


  /**
   * Convert hive types to sql types.
   * @param type
   * @return Integer java.sql.Types values
   * @throws SQLException
   */
  public static int hiveTypeToSqlType(String type) throws SQLException {
    if ("string".equalsIgnoreCase(type)) {
      return Types.VARCHAR;
    } else if ("float".equalsIgnoreCase(type)) {
      return Types.FLOAT;
    } else if ("double".equalsIgnoreCase(type)) {
      return Types.DOUBLE;
    } else if ("boolean".equalsIgnoreCase(type)) {
      return Types.BOOLEAN;
    } else if ("tinyint".equalsIgnoreCase(type)) {
      return Types.TINYINT;
    } else if ("smallint".equalsIgnoreCase(type)) {
      return Types.SMALLINT;
    } else if ("int".equalsIgnoreCase(type)) {
      return Types.INTEGER;
    } else if ("bigint".equalsIgnoreCase(type)) {
      return Types.BIGINT;
    } else if ("timestamp".equalsIgnoreCase(type)) {
      return Types.TIMESTAMP;
    } else if ("decimal".equalsIgnoreCase(type)) {
      return Types.DECIMAL;
    } else if ("binary".equalsIgnoreCase(type)) {
      return Types.BINARY;
    } else if (type.startsWith("map<")) {
      return Types.VARCHAR;
    } else if (type.startsWith("array<")) {
      return Types.VARCHAR;
    } else if (type.startsWith("struct<")) {
      return Types.VARCHAR;
    }
    throw new SQLException("Unrecognized column type: " + type);
  }

  // Verify success or success_with_info status, else throw SQLException
  public static void verifySuccessWithInfo(TStatus status) throws SQLException {
    verifySuccess(status, true);
  }

  // Verify success status, else throw SQLException
  public static void verifySuccess(TStatus status) throws SQLException {
    verifySuccess(status, false);
  }

  // Verify success and optionally with_info status, else throw SQLException
  public static void verifySuccess(TStatus status, boolean withInfo) throws SQLException {
    if ((status.getStatusCode() != TStatusCode.SUCCESS_STATUS) &&
        (withInfo && (status.getStatusCode() != TStatusCode.SUCCESS_WITH_INFO_STATUS))) {
      throw new SQLException(status.getErrorMessage(),
           status.getSqlState(), status.getErrorCode());
      }
  }

  /**
   * Parse JDBC connection URL
   * The new format of the URL is jdbc:hive://<host>:<port>/dbName;sess_var_list?hive_conf_list#hive_var_list
   * where the optional sess, conf and var lists are semicolon separated <key>=<val> pairs. As before, if the
   * host/port is not specified, it the driver runs an embedded hive.
   * examples -
   *  jdbc:hive://ubuntu:11000/db2?hive.cli.conf.printheader=true;hive.exec.mode.local.auto.inputbytes.max=9999#stab=salesTable;icol=customerID
   *  jdbc:hive://?hive.cli.conf.printheader=true;hive.exec.mode.local.auto.inputbytes.max=9999#stab=salesTable;icol=customerID
   *  jdbc:hive://ubuntu:11000/db2;user=foo;password=bar
   *
   * Note that currently the session properties are not used.
   *
   * @param uri
   * @return
   */
  public static JdbcConnectionParams parseURL(String uri) throws IllegalArgumentException {
    JdbcConnectionParams connParams = new JdbcConnectionParams();

    if (!uri.startsWith(URL_PREFIX)) {
      throw new IllegalArgumentException("Bad URL format");
    }

    // Don't parse URL with no other configuration.
    if (uri.equalsIgnoreCase(URL_PREFIX)) {
      connParams.setEmbeddedMode(true);
      return connParams;
    }
    URI jdbcURI = URI.create(uri.substring(URI_JDBC_PREFIX.length()));

    connParams.setHost(jdbcURI.getHost());
    if (connParams.getHost() == null) {
      connParams.setEmbeddedMode(true);
    } else {
      int port = jdbcURI.getPort();
      if (port == -1) {
        port = Integer.valueOf(DEFAULT_PORT);
      }
      connParams.setPort(port);
    }

    // key=value pattern
    Pattern pattern = Pattern.compile("([^;]*)=([^;]*)[;]?");

    // dbname and session settings
    String sessVars = jdbcURI.getPath();
    if ((sessVars == null) || sessVars.isEmpty()) {
      connParams.setDbName(DEFAULT_DATABASE);
    } else {
      // removing leading '/' returned by getPath()
      sessVars = sessVars.substring(1);
      if (!sessVars.contains(";")) {
        // only dbname is provided
        connParams.setDbName(sessVars);
      } else {
        // we have dbname followed by session parameters
        connParams.setDbName(sessVars.substring(0, sessVars.indexOf(';')));
        sessVars = sessVars.substring(sessVars.indexOf(';')+1);
        if (sessVars != null) {
          Matcher sessMatcher = pattern.matcher(sessVars);
          while (sessMatcher.find()) {
            connParams.getSessionVars().put(sessMatcher.group(1), sessMatcher.group(2));
          }
        }
      }
    }

    // parse hive conf settings
    String confStr = jdbcURI.getQuery();
    if (confStr != null) {
      Matcher confMatcher = pattern.matcher(confStr);
      while (confMatcher.find()) {
        connParams.getHiveConfs().put(confMatcher.group(1), confMatcher.group(2));
      }
    }

    // parse hive var settings
    String varStr = jdbcURI.getFragment();
    if (varStr != null) {
      Matcher varMatcher = pattern.matcher(varStr);
      while (varMatcher.find()) {
        connParams.getHiveVars().put(varMatcher.group(1), varMatcher.group(2));
      }
    }

    return connParams;
  }


}
