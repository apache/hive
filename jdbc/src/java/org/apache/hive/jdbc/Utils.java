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
   * The new format of the URL is jdbc:hive2://<host>:<port>/dbName;sess_var_list?hive_conf_list#hive_var_list
   * where the optional sess, conf and var lists are semicolon separated <key>=<val> pairs. As before, if the
   * host/port is not specified, it the driver runs an embedded hive.
   * examples -
   *  jdbc:hive2://ubuntu:11000/db2?hive.cli.conf.printheader=true;hive.exec.mode.local.auto.inputbytes.max=9999#stab=salesTable;icol=customerID
   *  jdbc:hive2://?hive.cli.conf.printheader=true;hive.exec.mode.local.auto.inputbytes.max=9999#stab=salesTable;icol=customerID
   *  jdbc:hive2://ubuntu:11000/db2;user=foo;password=bar
   *
   *  Connect to http://server:10001/hs2, with specified basicAuth credentials and initial database:
   *     jdbc:hive2://server:10001/db;user=foo;password=bar?hive.server2.transport.mode=http;hive.server2.thrift.http.path=hs2
   *
   * Note that currently the session properties are not used.
   *
   * @param uri
   * @return
   */
  public static JdbcConnectionParams parseURL(String uri) throws IllegalArgumentException {
    JdbcConnectionParams connParams = new JdbcConnectionParams();

    if (!uri.startsWith(URL_PREFIX)) {
      throw new IllegalArgumentException("Bad URL format: Missing prefix " + URL_PREFIX);
    }

    // For URLs with no other configuration
    // Don't parse them, but set embedded mode as true
    if (uri.equalsIgnoreCase(URL_PREFIX)) {
      connParams.setEmbeddedMode(true);
      return connParams;
    }

    URI jdbcURI = URI.create(uri.substring(URI_JDBC_PREFIX.length()));

    // Check to prevent unintentional use of embedded mode. A missing "/"
    // to separate the 'path' portion of URI can result in this.
    // The missing "/" common typo while using secure mode, eg of such url -
    // jdbc:hive2://localhost:10000;principal=hive/HiveServer2Host@YOUR-REALM.COM
    if((jdbcURI.getAuthority() != null) && (jdbcURI.getHost()==null)) {
       throw new IllegalArgumentException("Bad URL format. Hostname not found "
           + " in authority part of the url: " + jdbcURI.getAuthority()
           + ". Are you missing a '/' after the hostname ?");
    }

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
    if ((sessVars != null) && !sessVars.isEmpty()) {
      String dbName = "";
      // removing leading '/' returned by getPath()
      sessVars = sessVars.substring(1);
      if (!sessVars.contains(";")) {
        // only dbname is provided
        dbName = sessVars;
      } else {
        // we have dbname followed by session parameters
        dbName = sessVars.substring(0, sessVars.indexOf(';'));
        sessVars = sessVars.substring(sessVars.indexOf(';')+1);
        if (sessVars != null) {
          Matcher sessMatcher = pattern.matcher(sessVars);
          while (sessMatcher.find()) {
            if (connParams.getSessionVars().put(sessMatcher.group(1), sessMatcher.group(2)) != null) {
              throw new IllegalArgumentException("Bad URL format: Multiple values for property " + sessMatcher.group(1));
            }
          }
        }
      }
      if (!dbName.isEmpty()) {
        connParams.setDbName(dbName);
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

  /**
   * Takes a version string delmited by '.' and '-' characters
   * and returns a partial version.
   *
   * @param fullVersion
   *          version string.
   * @param tokenPosition
   *          position of version string to get starting at 0. eg, for a X.x.xxx
   *          string, 0 will return the major version, 1 will return minor
   *          version.
   * @return version part, or -1 if version string was malformed.
   */
  static int getVersionPart(String fullVersion, int position) {
    int version = -1;
    try {
      String[] tokens = fullVersion.split("[\\.-]"); //$NON-NLS-1$

      if (tokens != null && tokens.length > 1 && tokens[position] != null) {
        version = Integer.parseInt(tokens[position]);
      }
    } catch (Exception e) {
      version = -1;
    }
    return version;
  }
}
