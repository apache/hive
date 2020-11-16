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
import java.sql.Statement;
import java.util.List;

public class MySQLConnectorProvider extends AbstractJDBCConnectorProvider {
  private static Logger LOG = LoggerFactory.getLogger(MySQLConnectorProvider.class);

  private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver".intern();

  public MySQLConnectorProvider(String dbName, DataConnector dataConn) {
    super(dbName, dataConn);
    driverClassName = DRIVER_CLASS;
  }

  /**
   * Returns a list of all table names from the remote database.
   * @return List A collection of all the table names, null if there are no tables.
   * @throws IOException To indicate any failures with executing this API
   */
  @Override protected ResultSet fetchTableNames() throws MetaException {
    ResultSet rs = null;
    try {
      rs = getConnection().getMetaData().getTables(scoped_db, null, null, new String[] { "TABLE" });
    } catch (SQLException sqle) {
      LOG.warn("Could not retrieve table names from remote datasource, cause:" + sqle.getMessage());
      throw new MetaException("Could not retrieve table names from remote datasource, cause:" + sqle.getMessage());
    }
    return rs;
  }

  /**
   * Returns Hive Table objects from the remote database for tables that match a name pattern.
   * @return List A collection of objects that match the name pattern, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param regex
   */
  @Override public  List<Table> getTables(String regex) throws MetaException {
    return null;
  }

  /**
   * Fetch a single table with the given name, returns a Hive Table object from the remote database
   * @return Table A Table object for the matching table, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param tableName
   */
  @Override public ResultSet fetchTableMetadata(String tableName) throws MetaException {
    try {
      Statement stmt = getConnection().createStatement();
      ResultSet rs = stmt.executeQuery(
          "SELECT table_name, column_name, is_nullable, data_type, character_maximum_length FROM INFORMATION_SCHEMA.Columns where table_schema='"
              + scoped_db + "' and table_name='" + tableName + "'");
      /*
      List<FieldSchema> cols = new ArrayList<>();
      // TODO throw exception is RS is empty
      while (rs.next()) {
        FieldSchema fs = new FieldSchema();
        fs.setName(rs.getString("COLUMN_NAME"));
        fs.setType(getDataType(rs.getString("DATA_TYPE"), rs.getInt("CHARACTER_MAXIMUM_LENGTH")));
        fs.setComment("inferred column type");
        cols.add(fs);
      }

      //Setting the storage descriptor.
      StorageDescriptor sd = new StorageDescriptor();
      sd.setCols(cols);
      // sd.se
      SerDeInfo serdeInfo = new SerDeInfo();
      serdeInfo.setName(tableName);
      serdeInfo.setSerializationLib("org.apache.hive.storage.jdbc.JdbcSerDe");
      Map<String, String> serdeParams = new HashMap<String, String>();
      serdeParams.put("serialization.format", "1");
      serdeInfo.setParameters(serdeParams);

      // StorageHandler

      // serdeInfo.setDeserializerClass();
      sd.setSerdeInfo(serdeInfo);
      // sd.getSerdeInfo().setName(tableName);
      sd.setInputFormat("org.apache.hive.storage.jdbc.JdbcInputFormat"); // TODO
      sd.setOutputFormat("org.apache.hive.storage.jdbc.JdbcOutputFormat"); // TODO
      sd.setLocation("/tmp/some_dummy_path"); // TODO
      sd.setBucketCols(new ArrayList<String>());
      sd.setSortCols(new ArrayList<Order>());

      //Setting the table properties.
      Map<String, String> tblProps = new HashMap<>();
      tblProps.put(Constants.JDBC_DATABASE_TYPE, this.type);
      tblProps.put(Constants.JDBC_DRIVER, this.driverClassName);
      tblProps.put(Constants.JDBC_URL, this.jdbcUrl); // "jdbc://localhost:3306/hive"
      tblProps.put(Constants.JDBC_USERNAME, this.username);
      tblProps.put(Constants.JDBC_PASSWORD, this.password);
      tblProps.put(Constants.JDBC_TABLE, tableName);
      tblProps.put(hive_metastoreConstants.META_TABLE_STORAGE, Constants.JDBC_HIVE_STORAGE_HANDLER_ID);
      tblProps.put("EXTERNAL", "TRUE");
      // TODO: Need to include schema, catalog info in the paramters list.

      //Setting the required table information
      Table table = new Table();
      table.setTableName(tableName);
      table.setTableType(TableType.EXTERNAL_TABLE.toString());
      table.setDbName(scoped_db);
      table.setSd(sd);
      table.setParameters(tblProps);
      // set partition keys to empty
      table.setPartitionKeys(new ArrayList<FieldSchema>());

      return table;
       */
      return rs;
    } catch (Exception e) {
      LOG.warn("Exception retrieving remote table " + scoped_db + "." + tableName + " via data connector "
          + connector.getName());
      throw new MetaException("Error retrieving remote table:" + e);
    }
  }

  private String wrapSize(int size) {
    return "(" + size + ")";
  }

  protected String getDataType(String dbDataType, int size) {
    //TODO: Geomentric, network, bit, array data types of postgresql needs to be supported.
    switch(dbDataType.toLowerCase())
    {
    case "char":
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
