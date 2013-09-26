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

import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.hive.jdbc.Utils.JdbcConnectionParams;


/**
 * HiveDriver.
 *
 */
public class HiveDriver implements Driver {
  static {
    try {
      java.sql.DriverManager.registerDriver(new HiveDriver());
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Is this driver JDBC compliant?
   */
  private static final boolean JDBC_COMPLIANT = false;

  /**
   * Property key for the database name.
   */
  private static final String DBNAME_PROPERTY_KEY = "DBNAME";

  /**
   * Property key for the Hive Server2 host.
   */
  private static final String HOST_PROPERTY_KEY = "HOST";

  /**
   * Property key for the Hive Server2 port.
   */
  private static final String PORT_PROPERTY_KEY = "PORT";


  /**
   *
   */
  public HiveDriver() {
    // TODO Auto-generated constructor stub
    SecurityManager security = System.getSecurityManager();
    if (security != null) {
      security.checkWrite("foobah");
    }
  }

  /**
   * Checks whether a given url is in a valid format.
   *
   * The current uri format is: jdbc:hive://[host[:port]]
   *
   * jdbc:hive:// - run in embedded mode jdbc:hive://localhost - connect to
   * localhost default port (10000) jdbc:hive://localhost:5050 - connect to
   * localhost port 5050
   *
   * TODO: - write a better regex. - decide on uri format
   */

  public boolean acceptsURL(String url) throws SQLException {
    return Pattern.matches(Utils.URL_PREFIX + ".*", url);
  }

  /*
   * As per JDBC 3.0 Spec (section 9.2)
   * "If the Driver implementation understands the URL, it will return a Connection object;
   * otherwise it returns null"
   */
  public Connection connect(String url, Properties info) throws SQLException {
    return acceptsURL(url) ? new HiveConnection(url, info) : null;
  }

  /**
   * Package scoped access to the Driver's Major Version
   * @return The Major version number of the driver. -1 if it cannot be determined from the
   * manifest.mf file.
   */
  static int getMajorDriverVersion() {
    int version = -1;
    try {
      String fullVersion = HiveDriver.fetchManifestAttribute(
          Attributes.Name.IMPLEMENTATION_VERSION);
      String[] tokens = fullVersion.split("\\."); //$NON-NLS-1$

      if(tokens != null && tokens.length > 0 && tokens[0] != null) {
        version = Integer.parseInt(tokens[0]);
      }
    } catch (Exception e) {
      // Possible reasons to end up here:
      // - Unable to read version from manifest.mf
      // - Version string is not in the proper X.x.xxx format
      version = -1;
    }
    return version;
  }

  /**
   * Package scoped access to the Driver's Minor Version
   * @return The Minor version number of the driver. -1 if it cannot be determined from the
   * manifest.mf file.
   */
  static int getMinorDriverVersion() {
    int version = -1;
    try {
      String fullVersion = HiveDriver.fetchManifestAttribute(
          Attributes.Name.IMPLEMENTATION_VERSION);
      String[] tokens = fullVersion.split("\\."); //$NON-NLS-1$

      if(tokens != null && tokens.length > 1 && tokens[1] != null) {
        version = Integer.parseInt(tokens[1]);
      }
    } catch (Exception e) {
      // Possible reasons to end up here:
      // - Unable to read version from manifest.mf
      // - Version string is not in the proper X.x.xxx format
      version = -1;
    }
    return version;
  }

  /**
   * Returns the major version of this driver.
   */
  public int getMajorVersion() {
    return HiveDriver.getMajorDriverVersion();
  }

  /**
   * Returns the minor version of this driver.
   */
  public int getMinorVersion() {
    return HiveDriver.getMinorDriverVersion();
  }

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    // JDK 1.7
    throw new SQLFeatureNotSupportedException("Method not supported");
  }

  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    if (info == null) {
      info = new Properties();
    }

    if ((url != null) && url.startsWith(Utils.URL_PREFIX)) {
      info = parseURLforPropertyInfo(url, info);
    }

    DriverPropertyInfo hostProp = new DriverPropertyInfo(HOST_PROPERTY_KEY,
        info.getProperty(HOST_PROPERTY_KEY, ""));
    hostProp.required = false;
    hostProp.description = "Hostname of Hive Server2";

    DriverPropertyInfo portProp = new DriverPropertyInfo(PORT_PROPERTY_KEY,
        info.getProperty(PORT_PROPERTY_KEY, ""));
    portProp.required = false;
    portProp.description = "Port number of Hive Server2";

    DriverPropertyInfo dbProp = new DriverPropertyInfo(DBNAME_PROPERTY_KEY,
        info.getProperty(DBNAME_PROPERTY_KEY, "default"));
    dbProp.required = false;
    dbProp.description = "Database name";

    DriverPropertyInfo[] dpi = new DriverPropertyInfo[3];

    dpi[0] = hostProp;
    dpi[1] = portProp;
    dpi[2] = dbProp;

    return dpi;
  }

  /**
   * Returns whether the driver is JDBC compliant.
   */
  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }

  /**
   * Takes a url in the form of jdbc:hive://[hostname]:[port]/[db_name] and
   * parses it. Everything after jdbc:hive// is optional.
   *
   * The output from Utils.parseUrl() is massaged for the needs of getPropertyInfo
   * @param url
   * @param defaults
   * @return
   * @throws java.sql.SQLException
   */
  private Properties parseURLforPropertyInfo(String url, Properties defaults) throws SQLException {
    Properties urlProps = (defaults != null) ? new Properties(defaults)
        : new Properties();

    if (url == null || !url.startsWith(Utils.URL_PREFIX)) {
      throw new SQLException("Invalid connection url: " + url);
    }

    JdbcConnectionParams params = Utils.parseURL(url);
    String host = params.getHost();
    if (host == null){
      host = "";
    }
    String port = Integer.toString(params.getPort());
    if(host.equals("")){
      port = "";
    }
    else if(port.equals("0")){
      port = Utils.DEFAULT_PORT;
    }
    String db = params.getDbName();
    urlProps.put(HOST_PROPERTY_KEY, host);
    urlProps.put(PORT_PROPERTY_KEY, port);
    urlProps.put(DBNAME_PROPERTY_KEY, db);

    return urlProps;
  }

  /**
   * Lazy-load manifest attributes as needed.
   */
  private static Attributes manifestAttributes = null;

  /**
   * Loads the manifest attributes from the jar.
   *
   * @throws java.net.MalformedURLException
   * @throws IOException
   */
  private static synchronized void loadManifestAttributes() throws IOException {
    if (manifestAttributes != null) {
      return;
    }
    Class<?> clazz = HiveDriver.class;
    String classContainer = clazz.getProtectionDomain().getCodeSource()
        .getLocation().toString();
    URL manifestUrl = new URL("jar:" + classContainer
        + "!/META-INF/MANIFEST.MF");
    Manifest manifest = new Manifest(manifestUrl.openStream());
    manifestAttributes = manifest.getMainAttributes();
  }

  /**
   * Package scoped to allow manifest fetching from other HiveDriver classes
   * Helper to initialize attributes and return one.
   *
   * @param attributeName
   * @return
   * @throws SQLException
   */
  static String fetchManifestAttribute(Attributes.Name attributeName)
      throws SQLException {
    try {
      loadManifestAttributes();
    } catch (IOException e) {
      throw new SQLException("Couldn't load manifest attributes.", e);
    }
    return manifestAttributes.getValue(attributeName);
  }
}
