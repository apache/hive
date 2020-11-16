package org.apache.hadoop.hive.metastore.dataconnector.jdbc;

import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class PostgreSQLConnectorProvider extends AbstractJDBCConnectorProvider {
  private static Logger LOG = LoggerFactory.getLogger(MySQLConnectorProvider.class);
  private static final String DRIVER_CLASS = "org.postgresql.Driver".intern();

  public PostgreSQLConnectorProvider(String dbName, DataConnector dataConn) {
    super(dbName, dataConn);
    driverClassName = DRIVER_CLASS;
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

  @Override protected ResultSet fetchTableMetadata(String tableName) throws MetaException {
    ResultSet rs = null;
    try {
      rs = getConnection().getMetaData().getTables(scoped_db, null, null, new String[] { "TABLE" });
    } catch (SQLException sqle) {
      LOG.warn("Could not retrieve table names from remote datasource, cause:" + sqle.getMessage());
      throw new MetaException("Could not retrieve table names from remote datasource, cause:" + sqle.getMessage());
    }
    return rs;
  }

  @Override protected ResultSet fetchTableNames() throws MetaException {
    return null;
  }

  private String wrapSize(int size) {
    return "(" + size + ")";
  }

  protected String getDataType(String dbDataType, int size) {
    //TODO: Geomentric, network, bit, array data types of postgresql needs to be supported.
    switch(dbDataType.toLowerCase())
    {
    case "char":
    case "bpchar":
      return ColumnType.CHAR_TYPE_NAME + wrapSize(size);
    case "varchar":
    case "tinytext":
      return ColumnType.VARCHAR_TYPE_NAME + wrapSize(size);
    case "text":
    case "mediumtext":
    case "enum":
    case "set":
    case "tsvector":
    case "tsquery":
    case "uuid":
    case "json":
      return ColumnType.STRING_TYPE_NAME;
    case "blob":
    case "mediumblob":
    case "longblob":
    case "bytea":
      return ColumnType.BINARY_TYPE_NAME;
    case "tinyint":
      return ColumnType.TINYINT_TYPE_NAME;
    case "smallint":
    case "smallserial":
      return ColumnType.SMALLINT_TYPE_NAME;
    case "mediumint":
    case "int":
    case "serial":
      return ColumnType.INT_TYPE_NAME;
    case "bigint":
    case "bigserial":
    case "int8":
    case "money":
      return ColumnType.BIGINT_TYPE_NAME;
    case "float":
    case "real":
      return ColumnType.FLOAT_TYPE_NAME;
    case "double":
    case "double precision":
      return ColumnType.DOUBLE_TYPE_NAME;
    case "decimal":
    case "numeric":
      return ColumnType.DECIMAL_TYPE_NAME;
    case "date":
      return ColumnType.DATE_TYPE_NAME;
    case "datetime":
      return ColumnType.DATETIME_TYPE_NAME;
    case "timestamp":
    case "time":
    case "interval":
      return ColumnType.TIMESTAMP_TYPE_NAME;
    case "timestampz":
    case "timez":
      return ColumnType.TIMESTAMPTZ_TYPE_NAME;
    case "boolean":
      return ColumnType.BOOLEAN_TYPE_NAME;
    default:
      return ColumnType.VOID_TYPE_NAME;
    }
  }
}
