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

package org.apache.hive.jdbc;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Arrays;
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

  // To parse the intermediate URI as a Java URI, we'll give a dummy authority(dummyhost:00000).
  // Later, we'll substitute the dummy authority for a resolved authority.
  static final String dummyAuthorityString = "dummyhost:00000";

  /**
   * Hive's default database name
   */
  static final String DEFAULT_DATABASE = "default";

  private static final String URI_JDBC_PREFIX = "jdbc:";

  private static final String URI_HIVE_PREFIX = "hive2:";

  // This value is set to true by the setServiceUnavailableRetryStrategy() when the server returns 401.
  // This value is used only when cookie is sent for authorization. In case the cookie is expired,
  // client will send the actual credentials in the next connection request.
  // If credentials are sent in the first request it self, then no need to retry.
  static final String HIVE_SERVER2_RETRY_KEY = "hive.server2.retryserver";
  static final String HIVE_SERVER2_SENT_CREDENTIALS = "hive.server2.sentCredentials";
  static final String HIVE_SERVER2_CONST_TRUE = "true";
  static final String HIVE_SERVER2_CONST_FALSE = "false";

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
   * jdbc:hive2://&lt;host1&gt;:&lt;port1&gt;,&lt;host2&gt;:&lt;port2&gt;/dbName;sess_var_list?hive_conf_list#hive_var_list
   * where the optional sess, conf and var lists are semicolon separated &lt;key&gt;=&lt;val&gt; pairs.
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
  public static JdbcConnectionParams parseURL(String uri, Properties info)
      throws JdbcUriParseException, SQLException, ZooKeeperHiveClientException {
    JdbcConnectionParams connParams = extractURLComponents(uri, info);
    if (ZooKeeperHiveClientHelper.isZkDynamicDiscoveryMode(connParams.getSessionVars())) {
      configureConnParamsFromZooKeeper(connParams);
    }
    handleAllDeprecations(connParams);
    return connParams;
  }

  /**
   * This method handles the base parsing of the given jdbc uri. Some of JdbcConnectionParams
   * returned from this method are updated if ZooKeeper is used for service discovery
   *
   * @param uri
   * @param info
   * @return
   * @throws JdbcUriParseException
   */
  public static JdbcConnectionParams extractURLComponents(String uri, Properties info)
      throws JdbcUriParseException {
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
    String authorityFromClientJdbcURL = getAuthorityFromJdbcURL(uri);
    if ((authorityFromClientJdbcURL == null) || (authorityFromClientJdbcURL.isEmpty())) {
      // Given uri of the form:
      // jdbc:hive2:///dbName;sess_var_list?hive_conf_list#hive_var_list
      connParams.setEmbeddedMode(true);
    } else {
      connParams.setSuppliedURLAuthority(authorityFromClientJdbcURL);
      uri = uri.replace(authorityFromClientJdbcURL, dummyAuthorityString);
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
            if (connParams.getSessionVars().put(sessMatcher.group(1),
                sessMatcher.group(2)) != null) {
              throw new JdbcUriParseException(
                  "Bad URL format: Multiple values for property " + sessMatcher.group(1));
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
          connParams.getHiveVars().put(key.substring(JdbcConnectionParams.HIVE_VAR_PREFIX.length()),
              info.getProperty(key));
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

    if (!connParams.getSessionVars().containsKey(JdbcConnectionParams.AUTH_PASSWD)) {
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
    // Extract host, port
    if (connParams.isEmbeddedMode()) {
      // In case of embedded mode we were supplied with an empty authority.
      // So we never substituted the authority with a dummy one.
      connParams.setHost(jdbcURI.getHost());
      connParams.setPort(jdbcURI.getPort());
    } else {
      String authorityStr = connParams.getSuppliedURLAuthority();
      // If we're using ZooKeeper, the final host, port will be read from ZooKeeper
      // (in a different method call). Therefore, we put back the original authority string
      // (which basically is the ZooKeeper ensemble) back in the uri
      if (ZooKeeperHiveClientHelper.isZkDynamicDiscoveryMode(connParams.getSessionVars())) {
        uri = uri.replace(dummyAuthorityString, authorityStr);
        // Set ZooKeeper ensemble in connParams for later use
        connParams.setZooKeeperEnsemble(authorityStr);
        ZooKeeperHiveClientHelper.setZkSSLParams(connParams);
      } else {
        URI jdbcBaseURI = URI.create(URI_HIVE_PREFIX + "//" + authorityStr);
        // Check to prevent unintentional use of embedded mode. A missing "/"
        // to separate the 'path' portion of URI can result in this.
        // The missing "/" common typo while using secure mode, eg of such url -
        // jdbc:hive2://localhost:10000;principal=hive/HiveServer2Host@YOUR-REALM.COM
        if (jdbcBaseURI.getAuthority() != null) {
          String host = jdbcBaseURI.getHost();
          int port = jdbcBaseURI.getPort();
          if (host == null) {
            throw new JdbcUriParseException(
                "Bad URL format. Hostname not found " + " in authority part of the url: "
                    + jdbcBaseURI.getAuthority() + ". Are you missing a '/' after the hostname ?");
          }
          // Set the port to default value; we do support jdbc url like:
          // jdbc:hive2://localhost/db
          if (port <= 0) {
            port = Integer.parseInt(Utils.DEFAULT_PORT);
          }
          connParams.setHost(jdbcBaseURI.getHost());
          connParams.setPort(jdbcBaseURI.getPort());
        }
        // We check for invalid host, port while configuring connParams with configureConnParams()
        authorityStr = connParams.getHost() + ":" + connParams.getPort();
        LOG.debug("Resolved authority: " + authorityStr);
        uri = uri.replace(dummyAuthorityString, authorityStr);
      }
    }
    connParams.setJdbcUriString(uri);
    return connParams;
  }

  // Configure using ZooKeeper
  static void configureConnParamsFromZooKeeper(JdbcConnectionParams connParams)
      throws ZooKeeperHiveClientException, JdbcUriParseException {
    ZooKeeperHiveClientHelper.configureConnParams(connParams);
    String authorityStr = connParams.getHost() + ":" + connParams.getPort();
    LOG.debug("Resolved authority: " + authorityStr);
    String jdbcUriString = connParams.getJdbcUriString();
    // Replace ZooKeeper ensemble from the authority component of the JDBC Uri provided by the
    // client, by the host:port of the resolved server instance we will connect to
    connParams.setJdbcUriString(
        jdbcUriString.replace(getAuthorityFromJdbcURL(jdbcUriString), authorityStr));
  }

  private static void handleAllDeprecations(JdbcConnectionParams connParams) {
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
   * @return
   * @throws JdbcUriParseException
   */
  private static String getAuthorityFromJdbcURL(String uri) throws JdbcUriParseException {
    String authorities;
    /**
     * For a jdbc uri like:
     * jdbc:hive2://<host1>:<port1>,<host2>:<port2>/dbName;sess_var_list?conf_list#var_list Extract
     * the uri host:port list starting after "jdbc:hive2://", till the 1st "/" or "?" or "#"
     * whichever comes first & in the given order Examples:
     * jdbc:hive2://host1:port1,host2:port2,host3:port3/db;k1=v1?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3/;k1=v1?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3?k2=v2#k3=v3
     * jdbc:hive2://host1:port1,host2:port2,host3:port3#k3=v3
     */
    int fromIndex = Utils.URL_PREFIX.length();
    int toIndex = -1;
    for (String toIndexChar : Arrays.asList("/", "?", "#")) {
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

  /**
   * Method to get canonical-ized hostname, given a hostname (possibly a CNAME).
   * This should allow for service-principals to use simplified CNAMEs.
   * @param hostName The hostname to be canonical-ized.
   * @return Given a CNAME, the canonical-ized hostname is returned. If not found, the original hostname is returned.
   */
  public static String getCanonicalHostName(String hostName) {
    try {
      return InetAddress.getByName(hostName).getCanonicalHostName();
    }
    catch(UnknownHostException exception) {
      LOG.warn("Could not retrieve canonical hostname for " + hostName, exception);
      return hostName;
    }
  }

}
