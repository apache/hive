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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.http.client.CookieStore;
import org.apache.http.cookie.Cookie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  static final Logger LOG = LoggerFactory.getLogger(Utils.class.getName());
  /**
    * The required prefix for the connection URL.
    */
  public static final String URL_PREFIX = "jdbc:hive2://";

  /**
    * If host is provided, without a port.
    */
  static final String DEFAULT_PORT = "10000";

  /**
   * Hive's default database name
   */
  static final String DEFAULT_DATABASE = "default";

  private static final String URI_JDBC_PREFIX = "jdbc:";

  private static final String URI_HIVE_PREFIX = "hive2:";

  // This value is set to true by the setServiceUnavailableRetryStrategy() when the server returns 401
  static final String HIVE_SERVER2_RETRY_KEY = "hive.server2.retryserver";
  static final String HIVE_SERVER2_RETRY_TRUE = "true";
  static final String HIVE_SERVER2_RETRY_FALSE = "false";

  public static class JdbcConnectionParams {
    // Note on client side parameter naming convention:
    // Prefer using a shorter camelCase param name instead of using the same name as the
    // corresponding
    // HiveServer2 config.
    // For a jdbc url: jdbc:hive2://<host>:<port>/dbName;sess_var_list?hive_conf_list#hive_var_list,
    // client side params are specified in sess_var_list

    // Client param names:

    // Retry setting
    static final String RETRIES = "retries";

    public static final String AUTH_TYPE = "auth";
    // We're deprecating this variable's name.
    public static final String AUTH_QOP_DEPRECATED = "sasl.qop";
    public static final String AUTH_QOP = "saslQop";
    public static final String AUTH_SIMPLE = "noSasl";
    public static final String AUTH_TOKEN = "delegationToken";
    public static final String AUTH_USER = "user";
    public static final String AUTH_PRINCIPAL = "principal";
    public static final String AUTH_PASSWD = "password";
    public static final String AUTH_KERBEROS_AUTH_TYPE = "kerberosAuthType";
    public static final String AUTH_KERBEROS_AUTH_TYPE_FROM_SUBJECT = "fromSubject";
    public static final String ANONYMOUS_USER = "anonymous";
    public static final String ANONYMOUS_PASSWD = "anonymous";
    public static final String USE_SSL = "ssl";
    public static final String SSL_TRUST_STORE = "sslTrustStore";
    public static final String SSL_TRUST_STORE_PASSWORD = "trustStorePassword";
    // We're deprecating the name and placement of this in the parsed map (from hive conf vars to
    // hive session vars).
    static final String TRANSPORT_MODE_DEPRECATED = "hive.server2.transport.mode";
    public static final String TRANSPORT_MODE = "transportMode";
    // We're deprecating the name and placement of this in the parsed map (from hive conf vars to
    // hive session vars).
    static final String HTTP_PATH_DEPRECATED = "hive.server2.thrift.http.path";
    public static final String HTTP_PATH = "httpPath";
    public static final String SERVICE_DISCOVERY_MODE = "serviceDiscoveryMode";
    public static final String PROPERTY_DRIVER        = "driver";
    public static final String PROPERTY_URL           = "url";
    // Don't use dynamic service discovery
    static final String SERVICE_DISCOVERY_MODE_NONE = "none";
    // Use ZooKeeper for indirection while using dynamic service discovery
    static final String SERVICE_DISCOVERY_MODE_ZOOKEEPER = "zooKeeper";
    static final String ZOOKEEPER_NAMESPACE = "zooKeeperNamespace";
    // Default namespace value on ZooKeeper.
    // This value is used if the param "zooKeeperNamespace" is not specified in the JDBC Uri.
    static final String ZOOKEEPER_DEFAULT_NAMESPACE = "hiveserver2";
    static final String COOKIE_AUTH = "cookieAuth";
    static final String COOKIE_AUTH_FALSE = "false";
    static final String COOKIE_NAME = "cookieName";
    // The default value of the cookie name when CookieAuth=true
    static final String DEFAULT_COOKIE_NAMES_HS2 = "hive.server2.auth";
    // The http header prefix for additional headers which have to be appended to the request
    static final String HTTP_HEADER_PREFIX = "http.header.";
    // Set the fetchSize
    static final String FETCH_SIZE = "fetchSize";
    static final String INIT_FILE = "initFile";

    // --------------- Begin 2 way ssl options -------------------------
    // Use two way ssl. This param will take effect only when ssl=true
    static final String USE_TWO_WAY_SSL = "twoWay";
    static final String TRUE = "true";
    static final String SSL_KEY_STORE = "sslKeyStore";
    static final String SSL_KEY_STORE_PASSWORD = "keyStorePassword";
    static final String SSL_KEY_STORE_TYPE = "JKS";
    static final String SUNX509_ALGORITHM_STRING = "SunX509";
    static final String SUNJSSE_ALGORITHM_STRING = "SunJSSE";
   // --------------- End 2 way ssl options ----------------------------

    // Non-configurable params:
    // Currently supports JKS keystore format
    static final String SSL_TRUST_STORE_TYPE = "JKS";

    private static final String HIVE_VAR_PREFIX = "hivevar:";
    private static final String HIVE_CONF_PREFIX = "hiveconf:";
    private String host = null;
    private int port = 0;
    private String jdbcUriString;
    private String dbName = DEFAULT_DATABASE;
    private Map<String,String> hiveConfs = new LinkedHashMap<String,String>();
    private Map<String,String> hiveVars = new LinkedHashMap<String,String>();
    private Map<String,String> sessionVars = new LinkedHashMap<String,String>();
    private boolean isEmbeddedMode = false;
    private String[] authorityList;
    private String zooKeeperEnsemble = null;
    private String currentHostZnodePath;
    private final List<String> rejectedHostZnodePaths = new ArrayList<String>();

    public JdbcConnectionParams() {
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    public String getJdbcUriString() {
      return jdbcUriString;
    }

    public String getDbName() {
      return dbName;
    }

    public Map<String, String> getHiveConfs() {
      return hiveConfs;
    }

    public Map<String, String> getHiveVars() {
      return hiveVars;
    }

    public boolean isEmbeddedMode() {
      return isEmbeddedMode;
    }

    public Map<String, String> getSessionVars() {
      return sessionVars;
    }

    public String[] getAuthorityList() {
      return authorityList;
    }

    public String getZooKeeperEnsemble() {
      return zooKeeperEnsemble;
    }

    public List<String> getRejectedHostZnodePaths() {
      return rejectedHostZnodePaths;
    }

    public String getCurrentHostZnodePath() {
      return currentHostZnodePath;
    }

    public void setHost(String host) {
      this.host = host;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public void setJdbcUriString(String jdbcUriString) {
      this.jdbcUriString = jdbcUriString;
    }

    public void setDbName(String dbName) {
      this.dbName = dbName;
    }

    public void setHiveConfs(Map<String, String> hiveConfs) {
      this.hiveConfs = hiveConfs;
    }

    public void setHiveVars(Map<String, String> hiveVars) {
      this.hiveVars = hiveVars;
    }

    public void setEmbeddedMode(boolean embeddedMode) {
      this.isEmbeddedMode = embeddedMode;
    }

    public void setSessionVars(Map<String, String> sessionVars) {
      this.sessionVars = sessionVars;
    }

    public void setSuppliedAuthorityList(String[] authorityList) {
      this.authorityList = authorityList;
    }

    public void setZooKeeperEnsemble(String zooKeeperEnsemble) {
      this.zooKeeperEnsemble = zooKeeperEnsemble;
    }

    public void setCurrentHostZnodePath(String currentHostZnodePath) {
      this.currentHostZnodePath = currentHostZnodePath;
    }
  }

  // Verify success or success_with_info status, else throw SQLException
  static void verifySuccessWithInfo(TStatus status) throws SQLException {
    verifySuccess(status, true);
  }

  // Verify success status, else throw SQLException
  static void verifySuccess(TStatus status) throws SQLException {
    verifySuccess(status, false);
  }

  // Verify success and optionally with_info status, else throw SQLException
  static void verifySuccess(TStatus status, boolean withInfo) throws SQLException {
    if (status.getStatusCode() == TStatusCode.SUCCESS_STATUS ||
        (withInfo && status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS)) {
      return;
    }
    throw new HiveSQLException(status);
  }

  public static JdbcConnectionParams parseURL(String uri) throws JdbcUriParseException,
          SQLException, ZooKeeperHiveClientException {
    return parseURL(uri, new Properties());
  }
  /**
   * Parse JDBC connection URL
   * The new format of the URL is:
   * jdbc:hive2://<host1>:<port1>,<host2>:<port2>/dbName;sess_var_list?hive_conf_list#hive_var_list
   * where the optional sess, conf and var lists are semicolon separated <key>=<val> pairs.
   * For utilizing dynamic service discovery with HiveServer2 multiple comma separated host:port pairs can
   * be specified as shown above.
   * The JDBC driver resolves the list of uris and picks a specific server instance to connect to.
   * Currently, dynamic service discovery using ZooKeeper is supported, in which case the host:port pairs represent a ZooKeeper ensemble.
   *
   * As before, if the host/port is not specified, it the driver runs an embedded hive.
   * examples -
   *  jdbc:hive2://ubuntu:11000/db2?hive.cli.conf.printheader=true;hive.exec.mode.local.auto.inputbytes.max=9999#stab=salesTable;icol=customerID
   *  jdbc:hive2://?hive.cli.conf.printheader=true;hive.exec.mode.local.auto.inputbytes.max=9999#stab=salesTable;icol=customerID
   *  jdbc:hive2://ubuntu:11000/db2;user=foo;password=bar
   *
   *  Connect to http://server:10001/hs2, with specified basicAuth credentials and initial database:
   *  jdbc:hive2://server:10001/db;user=foo;password=bar?hive.server2.transport.mode=http;hive.server2.thrift.http.path=hs2
   *
   * @param uri
   * @return
   * @throws SQLException
   */
  static JdbcConnectionParams parseURL(String uri, Properties info) throws JdbcUriParseException,
      SQLException, ZooKeeperHiveClientException {
    JdbcConnectionParams connParams = new JdbcConnectionParams();

    if (!uri.startsWith(URL_PREFIX)) {
      throw new JdbcUriParseException("Bad URL format: Missing prefix " + URL_PREFIX);
    }

    // For URLs with no other configuration
    // Don't parse them, but set embedded mode as true
    if (uri.equalsIgnoreCase(URL_PREFIX)) {
      connParams.setEmbeddedMode(true);
      return connParams;
    }

    // The JDBC URI now supports specifying multiple host:port if dynamic service discovery is
    // configured on HiveServer2 (like: host1:port1,host2:port2,host3:port3)
    // We'll extract the authorities (host:port combo) from the URI, extract session vars, hive
    // confs & hive vars by parsing it as a Java URI.
    // To parse the intermediate URI as a Java URI, we'll give a dummy authority(dummy:00000).
    // Later, we'll substitute the dummy authority for a resolved authority.
    String dummyAuthorityString = "dummyhost:00000";
    String suppliedAuthorities = getAuthorities(uri, connParams);
    if ((suppliedAuthorities == null) || (suppliedAuthorities.isEmpty())) {
      // Given uri of the form:
      // jdbc:hive2:///dbName;sess_var_list?hive_conf_list#hive_var_list
      connParams.setEmbeddedMode(true);
    } else {
      LOG.info("Supplied authorities: " + suppliedAuthorities);
      String[] authorityList = suppliedAuthorities.split(",");
      connParams.setSuppliedAuthorityList(authorityList);
      uri = uri.replace(suppliedAuthorities, dummyAuthorityString);
    }

    // Now parse the connection uri with dummy authority
    URI jdbcURI = URI.create(uri.substring(URI_JDBC_PREFIX.length()));

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
        sessVars = sessVars.substring(sessVars.indexOf(';') + 1);
        if (sessVars != null) {
          Matcher sessMatcher = pattern.matcher(sessVars);
          while (sessMatcher.find()) {
            if (connParams.getSessionVars().put(sessMatcher.group(1), sessMatcher.group(2)) != null) {
              throw new JdbcUriParseException("Bad URL format: Multiple values for property "
                  + sessMatcher.group(1));
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
    
    // Apply configs supplied in the JDBC connection properties object
    for (Map.Entry<Object, Object> kv : info.entrySet()) {
      if ((kv.getKey() instanceof String)) {
        String key = (String) kv.getKey();
        if (key.startsWith(JdbcConnectionParams.HIVE_VAR_PREFIX)) {
          connParams.getHiveVars().put(
              key.substring(JdbcConnectionParams.HIVE_VAR_PREFIX.length()), info.getProperty(key));
        } else if (key.startsWith(JdbcConnectionParams.HIVE_CONF_PREFIX)) {
          connParams.getHiveConfs().put(
              key.substring(JdbcConnectionParams.HIVE_CONF_PREFIX.length()), info.getProperty(key));
        }
      }
    }
    // Extract user/password from JDBC connection properties if its not supplied
    // in the connection URL
    if (!connParams.getSessionVars().containsKey(JdbcConnectionParams.AUTH_USER)) {
        if (info.containsKey(JdbcConnectionParams.AUTH_USER)) {
            connParams.getSessionVars().put(JdbcConnectionParams.AUTH_USER,
              info.getProperty(JdbcConnectionParams.AUTH_USER));
        }
        if (info.containsKey(JdbcConnectionParams.AUTH_PASSWD)) {
          connParams.getSessionVars().put(JdbcConnectionParams.AUTH_PASSWD,
              info.getProperty(JdbcConnectionParams.AUTH_PASSWD));
        }
    }

    if (info.containsKey(JdbcConnectionParams.AUTH_TYPE)) {
      connParams.getSessionVars().put(JdbcConnectionParams.AUTH_TYPE,
          info.getProperty(JdbcConnectionParams.AUTH_TYPE));
    }

    // Handle all deprecations here:
    String newUsage;
    String usageUrlBase = "jdbc:hive2://<host>:<port>/dbName;";
    // Handle deprecation of AUTH_QOP_DEPRECATED
    newUsage = usageUrlBase + JdbcConnectionParams.AUTH_QOP + "=<qop_value>";
    handleParamDeprecation(connParams.getSessionVars(), connParams.getSessionVars(),
        JdbcConnectionParams.AUTH_QOP_DEPRECATED, JdbcConnectionParams.AUTH_QOP, newUsage);

    // Handle deprecation of TRANSPORT_MODE_DEPRECATED
    newUsage = usageUrlBase + JdbcConnectionParams.TRANSPORT_MODE + "=<transport_mode_value>";
    handleParamDeprecation(connParams.getHiveConfs(), connParams.getSessionVars(),
        JdbcConnectionParams.TRANSPORT_MODE_DEPRECATED, JdbcConnectionParams.TRANSPORT_MODE,
        newUsage);

    // Handle deprecation of HTTP_PATH_DEPRECATED
    newUsage = usageUrlBase + JdbcConnectionParams.HTTP_PATH + "=<http_path_value>";
    handleParamDeprecation(connParams.getHiveConfs(), connParams.getSessionVars(),
        JdbcConnectionParams.HTTP_PATH_DEPRECATED, JdbcConnectionParams.HTTP_PATH, newUsage);
    // Extract host, port
    if (connParams.isEmbeddedMode()) {
      // In case of embedded mode we were supplied with an empty authority.
      // So we never substituted the authority with a dummy one.
      connParams.setHost(jdbcURI.getHost());
      connParams.setPort(jdbcURI.getPort());
    } else {
      // Configure host, port and params from ZooKeeper if used,
      // and substitute the dummy authority with a resolved one
      configureConnParams(connParams);
      // We check for invalid host, port while configuring connParams with configureConnParams()
      String authorityStr = connParams.getHost() + ":" + connParams.getPort();
      LOG.info("Resolved authority: " + authorityStr);
      uri = uri.replace(dummyAuthorityString, authorityStr);
      connParams.setJdbcUriString(uri);
    }
    return connParams;
  }

  /**
   * Remove the deprecatedName param from the fromMap and put the key value in the toMap.
   * Also log a deprecation message for the client.
   * @param fromMap
   * @param toMap
   * @param deprecatedName
   * @param newName
   * @param newUsage
   */
  private static void handleParamDeprecation(Map<String, String> fromMap, Map<String, String> toMap,
      String deprecatedName, String newName, String newUsage) {
    if (fromMap.containsKey(deprecatedName)) {
      LOG.warn("***** JDBC param deprecation *****");
      LOG.warn("The use of " + deprecatedName + " is deprecated.");
      LOG.warn("Please use " + newName +" like so: " + newUsage);
      String paramValue = fromMap.remove(deprecatedName);
      toMap.put(newName, paramValue);
    }
  }

  /**
   * Get the authority string from the supplied uri, which could potentially contain multiple
   * host:port pairs.
   *
   * @param uri
   * @param connParams
   * @return
   * @throws JdbcUriParseException
   */
  private static String getAuthorities(String uri, JdbcConnectionParams connParams)
      throws JdbcUriParseException {
    String authorities;
    /**
     * For a jdbc uri like:
     * jdbc:hive2://<host1>:<port1>,<host2>:<port2>/dbName;sess_var_list?conf_list#var_list
     * Extract the uri host:port list starting after "jdbc:hive2://",
     * till the 1st "/" or "?" or "#" whichever comes first & in the given order
     * Examples:
     * jdbc:hive2://host1:port1,host2:port2,host3:port3/db;k1=v1?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3/;k1=v1?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3#k3=v3
     */
    int fromIndex = Utils.URL_PREFIX.length();
    int toIndex = -1;
    ArrayList<String> toIndexChars = new ArrayList<String>(Arrays.asList("/", "?", "#"));
    for (String toIndexChar : toIndexChars) {
      toIndex = uri.indexOf(toIndexChar, fromIndex);
      if (toIndex > 0) {
        break;
      }
    }
    if (toIndex < 0) {
      authorities = uri.substring(fromIndex);
    } else {
      authorities = uri.substring(fromIndex, toIndex);
    }
    return authorities;
  }

  private static void configureConnParams(JdbcConnectionParams connParams)
      throws JdbcUriParseException, ZooKeeperHiveClientException {
    String serviceDiscoveryMode =
        connParams.getSessionVars().get(JdbcConnectionParams.SERVICE_DISCOVERY_MODE);
    if ((serviceDiscoveryMode != null)
        && (JdbcConnectionParams.SERVICE_DISCOVERY_MODE_ZOOKEEPER
            .equalsIgnoreCase(serviceDiscoveryMode))) {
      // Set ZooKeeper ensemble in connParams for later use
      connParams.setZooKeeperEnsemble(joinStringArray(connParams.getAuthorityList(), ","));
      // Configure using ZooKeeper
      ZooKeeperHiveClientHelper.configureConnParams(connParams);
    } else {
      String authority = connParams.getAuthorityList()[0];
      URI jdbcURI = URI.create(URI_HIVE_PREFIX + "//" + authority);
      // Check to prevent unintentional use of embedded mode. A missing "/"
      // to separate the 'path' portion of URI can result in this.
      // The missing "/" common typo while using secure mode, eg of such url -
      // jdbc:hive2://localhost:10000;principal=hive/HiveServer2Host@YOUR-REALM.COM
      if (jdbcURI.getAuthority() != null) {
        String host = jdbcURI.getHost();
        int port = jdbcURI.getPort();
        if (host == null) {
          throw new JdbcUriParseException("Bad URL format. Hostname not found "
              + " in authority part of the url: " + jdbcURI.getAuthority()
              + ". Are you missing a '/' after the hostname ?");
        }
        // Set the port to default value; we do support jdbc url like:
        // jdbc:hive2://localhost/db
        if (port <= 0) {
          port = Integer.parseInt(Utils.DEFAULT_PORT);
        }
        connParams.setHost(jdbcURI.getHost());
        connParams.setPort(jdbcURI.getPort());
      }
    }
  }

  /**
   * Read the next server coordinates (host:port combo) from ZooKeeper. Ignore the znodes already
   * explored. Also update the host, port, jdbcUriString and other configs published by the server.
   *
   * @param connParams
   * @return true if new server info is retrieved successfully
   */
  static boolean updateConnParamsFromZooKeeper(JdbcConnectionParams connParams) {
    // Add current host to the rejected list
    connParams.getRejectedHostZnodePaths().add(connParams.getCurrentHostZnodePath());
    String oldServerHost = connParams.getHost();
    int oldServerPort = connParams.getPort();
    // Update connection params (including host, port) from ZooKeeper
    try {
      ZooKeeperHiveClientHelper.configureConnParams(connParams);
      connParams.setJdbcUriString(connParams.getJdbcUriString().replace(
          oldServerHost + ":" + oldServerPort, connParams.getHost() + ":" + connParams.getPort()));
      LOG.info("Selected HiveServer2 instance with uri: " + connParams.getJdbcUriString());
    } catch(ZooKeeperHiveClientException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  private static String joinStringArray(String[] stringArray, String seperator) {
    StringBuilder stringBuilder = new StringBuilder();
    for (int cur = 0, end = stringArray.length; cur < end; cur++) {
      if (cur > 0) {
        stringBuilder.append(seperator);
      }
      stringBuilder.append(stringArray[cur]);
    }
    return stringBuilder.toString();
  }

  /**
   * Takes a version string delimited by '.' and '-' characters
   * and returns a partial version.
   *
   * @param fullVersion
   *          version string.
   * @param position
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

  /**
   * The function iterates through the list of cookies in the cookiestore and tries to
   * match them with the cookieName. If there is a match, the cookieStore already
   * has a valid cookie and the client need not send Credentials for validation purpose.
   * @param cookieStore The cookie Store
   * @param cookieName Name of the cookie which needs to be validated
   * @param isSSL Whether this is a http/https connection
   * @return true or false based on whether the client needs to send the credentials or
   * not to the server.
   */
  static boolean needToSendCredentials(CookieStore cookieStore, String cookieName, boolean isSSL) {
    if (cookieName == null || cookieStore == null) {
      return true;
    }

    List<Cookie> cookies = cookieStore.getCookies();

    for (Cookie c : cookies) {
      // If this is a secured cookie and the current connection is non-secured,
      // then, skip this cookie. We need to skip this cookie because, the cookie
      // replay will not be transmitted to the server.
      if (c.isSecure() && !isSSL) {
        continue;
      }
      if (c.getName().equals(cookieName)) {
        return false;
      }
    }
    return true;
  }

  public static String parsePropertyFromUrl(final String url, final String key) {
    String[] tokens = url.split(";");
    for (String token : tokens) {
      if (token.trim().startsWith(key.trim() + "=")) {
        return token.trim().substring((key.trim() + "=").length());
      }
    }
    return null;
  }
}
