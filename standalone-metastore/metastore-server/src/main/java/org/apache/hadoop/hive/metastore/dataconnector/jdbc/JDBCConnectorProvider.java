package org.apache.hadoop.hive.metastore.dataconnector.jdbc;

import java.net.ConnectException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.dataconnector.AbstractDataConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class JDBCConnectorProvider extends AbstractDataConnectorProvider {
  private static Logger LOG = LoggerFactory.getLogger(JDBCConnectorProvider.class);

  String type = null; // MYSQL, POSTGRES, ORACLE, DERBY, MSSQL, DB2 etc.
  String driverClassName = null;
  String jdbcUrl = null;
  String username = null;
  String password = null; // TODO convert to byte array

  public JDBCConnectorProvider(String dbName, DataConnector dataConn) {
    super(dbName, dataConn);
    this.type = connector.getType();
    this.jdbcUrl = connector.getUrl();
    this.username = connector.getParameters().get("dataconnector.username");
    this.password = connector.getParameters().get("dataconnector.password");
  }

  @Override public void open() throws ConnectException {
    switch(type.toLowerCase()) {
    case MYSQL_TYPE:
      driverClassName = "com.mysql.jdbc.Driver";
      try {
        Class.forName(driverClassName);
        handle = DriverManager.getConnection(jdbcUrl, username, password);
        isOpen = true;
      } catch (ClassNotFoundException cnfe) {
        LOG.warn("Driver class not found in classpath:" + driverClassName);
        throw new RuntimeException("Driver class not found:" + driverClassName);
      } catch (SQLException sqle) {
        LOG.warn("Could not connect to remote data source at " + jdbcUrl);
        throw new ConnectException("Could not connect to remote datasource at " + jdbcUrl + ",cause:" + sqle.getMessage());
      }
      break;

    default:
      throw new RuntimeException("Unsupported JDBC type");
    }
  }

  private Connection getConnection() {
    try {
      if (!isOpen)
        open();
    } catch (ConnectException ce) {
      throw new RuntimeException(ce.getMessage());
    }

    if (handle instanceof Connection)
      return (Connection)handle;

    throw new RuntimeException("unexpected type for connection handle");
  }

  @Override public void close() {
    if (isOpen) {
      try {
        ((Connection)handle).close();
      } catch (SQLException sqle) {
        LOG.warn("Could not close jdbc connection to " + jdbcUrl, sqle);
      }
    }
  }

  /**
   * Returns Hive Table objects from the remote database for tables that match a name pattern.
   * @return List A collection of objects that match the name pattern, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param regex
   */
  @Override public List<Table> getTables(String regex) throws MetaException {
    return null;
  }

  /**
   * Returns a list of all table names from the remote database.
   * @return List A collection of all the table names, null if there are no tables.
   * @throws MetaException To indicate any failures with executing this API
   */
  @Override public List<String> getTableNames() throws MetaException {
    ResultSet rs = null;
    try {
      rs = getConnection().getMetaData().getTables(null, scoped_db, "*", new String[] { "TABLE" });
      if (rs != null) {
        List<String> tables = new ArrayList<String>();
        while(rs.next()) {
          tables.add(rs.getString(3));
        }
        return tables;
      }
    } catch (SQLException sqle) {
      LOG.warn("Could not retrieve table names from remote datasource, cause:" + sqle.getMessage());
    } finally {
      try {
        if (rs != null) {
          rs.close();
          rs = null;
        }
      } catch(Exception e) { /* ignore */}
    }
    return null;
  }

  /**
   * Fetch a single table with the given name, returns a Hive Table object from the remote database
   * @return Table A Table object for the matching table, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param tableName
   */
  @Override public Table getTable(String tableName) throws MetaException {
    return null;
  }

}
