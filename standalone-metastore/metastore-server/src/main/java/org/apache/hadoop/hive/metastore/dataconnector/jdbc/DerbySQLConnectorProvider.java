package org.apache.hadoop.hive.metastore.dataconnector.jdbc;

import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class DerbySQLConnectorProvider extends AbstractJDBCConnectorProvider {
  private static Logger LOG = LoggerFactory.getLogger(DerbySQLConnectorProvider.class);

  // private static final String DRIVER_CLASS = "org.apache.derby.jdbc.EmbeddedDriver".intern();
  private static final String DRIVER_CLASS = "org.apache.derby.jdbc.AutoloadedDriver".intern();

  public DerbySQLConnectorProvider(String dbName, DataConnector connector) {
    super(dbName, connector, DRIVER_CLASS);
  }

  /**
   * Returns a list of all table names from the remote database.
   * @return List A collection of all the table names, null if there are no tables.
   * @throws IOException To indicate any failures with executing this API
   */
  @Override
  protected ResultSet fetchTableNames() throws MetaException {
    ResultSet rs = null;
    try {
      rs = getConnection().getMetaData().getTables(scoped_db, null, null, new String[] { "TABLE" });
    } catch (SQLException sqle) {
      LOG.warn("Could not retrieve table names from remote datasource, cause:" + sqle.getMessage());
      throw new MetaException("Could not retrieve table names from remote datasource, cause:" + sqle);
    }
    return rs;
  }

  /**
   * Fetch a single table with the given name, returns a Hive Table object from the remote database
   * @return Table A Table object for the matching table, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param tableName
   */
  @Override
  public ResultSet fetchTableMetadata(String tableName) throws MetaException {
     return null;
  }

  /**
   * Returns Hive Table objects from the remote database for tables that match a name pattern.
   * @return List A collection of objects that match the name pattern, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param regex
   */
  @Override
  public List<Table> getTables(String regex) throws MetaException {
    return null;
  }

  protected String getDataType(String dbDataType, int size) {
    String mappedType = super.getDataType(dbDataType, size);
    if (!mappedType.equalsIgnoreCase(ColumnType.VOID_TYPE_NAME)) {
      return mappedType;
    }

    // map any db specific types here.
    switch (dbDataType.toLowerCase())
    {
    case "integer":
      mappedType = ColumnType.INT_TYPE_NAME;
      break;
    case "long varchar":
      mappedType = ColumnType.STRING_TYPE_NAME;
      break;
    default:
      mappedType = ColumnType.VOID_TYPE_NAME;
      break;
    }
    return mappedType;
  }
}
