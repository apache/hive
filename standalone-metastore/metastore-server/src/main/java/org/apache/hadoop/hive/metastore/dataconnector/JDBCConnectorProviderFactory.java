package org.apache.hadoop.hive.metastore.dataconnector;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.MySQLConnectorProvider;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.PostgreSQLConnectorProvider;

import static org.apache.hadoop.hive.metastore.dataconnector.IDataConnectorProvider.MYSQL_TYPE;
import static org.apache.hadoop.hive.metastore.dataconnector.IDataConnectorProvider.POSTGRES_TYPE;


public class JDBCConnectorProviderFactory {

  public static IDataConnectorProvider get(String dbName, DataConnector connector) {
    IDataConnectorProvider provider = null;
    switch(connector.getType().toLowerCase()) {
    case MYSQL_TYPE:
      provider = new MySQLConnectorProvider(dbName, connector);
      /*
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
       */
      break;
    case POSTGRES_TYPE:
      provider = new PostgreSQLConnectorProvider(dbName, connector);
      break;

    default:
      throw new RuntimeException("Unsupported JDBC type");
    }

    return provider;
  }
}
