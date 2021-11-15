package org.apache.hadoop.hive.metastore.dataconnector;

import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.DerbySQLConnectorProvider;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.MySQLConnectorProvider;
import org.apache.hadoop.hive.metastore.dataconnector.jdbc.PostgreSQLConnectorProvider;

import static org.apache.hadoop.hive.metastore.dataconnector.IDataConnectorProvider.*;

public class JDBCConnectorProviderFactory {

  public static IDataConnectorProvider get(String dbName, DataConnector connector) {
    IDataConnectorProvider provider = null;
    switch(connector.getType().toLowerCase()) {
    case MYSQL_TYPE:
      provider = new MySQLConnectorProvider(dbName, connector);
      break;
    case POSTGRES_TYPE:
      provider = new PostgreSQLConnectorProvider(dbName, connector);
      break;

    case DERBY_TYPE:
      provider = new DerbySQLConnectorProvider(dbName, connector);
      break;

    default:
      throw new RuntimeException("Unsupported JDBC type");
    }

    return provider;
  }
}
