/**
 *
 */
package org.apache.hadoop.hive.jdbc;

import java.sql.*;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.regex.Pattern;


public class HiveDriver implements java.sql.Driver {
  static {
      try {
      java.sql.DriverManager.registerDriver(new org.apache.hadoop.hive.jdbc.HiveDriver());
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  /**
   * Major version number of this driver.
   */
  private static final int MAJOR_VERSION = 0;

  /**
   * Minor version number of this driver.
   */
  private static final int MINOR_VERSION = 0;

  /**
   * Is this driver JDBC compliant?
   */
  private static final boolean JDBC_COMPLIANT = false;

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
   * The current uri format is:
   * jdbc:hive://[host[:port]]
   *
   * jdbc:hive://                 - run in embedded mode
   * jdbc:hive://localhost        - connect to localhost default port (10000)
   * jdbc:hive://localhost:5050   - connect to localhost port 5050
   *
   * TODO: - write a better regex.
   *       - decide on uri format
   */

  public boolean acceptsURL(String url) throws SQLException {
    return Pattern.matches("jdbc:hive://", url);
  }


  public Connection connect(String url, Properties info) throws SQLException {
    try {
      return new HiveConnection(url, info);
    } catch (Exception ex) {
      throw new SQLException(ex.toString());
    }
  }

  /**
   * Returns the major version of this driver.
   */

  public int getMajorVersion() {
    return MAJOR_VERSION;
  }

  /**
   * Returns the minor version of this driver.
   */

  public int getMinorVersion() {
    return MINOR_VERSION;
  }


  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new SQLException("Method not supported");
  }

  /**
   * Returns whether the driver is JDBC compliant.
   */

  public boolean jdbcCompliant() {
    return JDBC_COMPLIANT;
  }

}
