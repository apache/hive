package org.apache.hadoop.hive.metastore.dataconnector.jdbc;

import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.dataconnector.AbstractDataConnectorProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractJDBCConnectorProvider extends AbstractDataConnectorProvider {
  private static Logger LOG = LoggerFactory.getLogger(AbstractJDBCConnectorProvider.class);
  protected static Warehouse warehouse = null;

  // duplicate constants from Constants.java to avoid a dependency on hive-common
  public static final String JDBC_HIVE_STORAGE_HANDLER_ID =
      "org.apache.hive.storage.jdbc.JdbcStorageHandler";
  public static final String JDBC_CONFIG_PREFIX = "hive.sql";
  public static final String JDBC_CATALOG = JDBC_CONFIG_PREFIX + ".catalog";
  public static final String JDBC_SCHEMA = JDBC_CONFIG_PREFIX + ".schema";
  public static final String JDBC_TABLE = JDBC_CONFIG_PREFIX + ".table";
  public static final String JDBC_DATABASE_TYPE = JDBC_CONFIG_PREFIX + ".database.type";
  public static final String JDBC_URL = JDBC_CONFIG_PREFIX + ".jdbc.url";
  public static final String JDBC_DRIVER = JDBC_CONFIG_PREFIX + ".jdbc.driver";
  public static final String JDBC_USERNAME = JDBC_CONFIG_PREFIX + ".dbcp.username";
  public static final String JDBC_PASSWORD = JDBC_CONFIG_PREFIX + ".dbcp.password";
  public static final String JDBC_KEYSTORE = JDBC_CONFIG_PREFIX + ".dbcp.password.keystore";
  public static final String JDBC_KEY = JDBC_CONFIG_PREFIX + ".dbcp.password.key";
  public static final String JDBC_QUERY = JDBC_CONFIG_PREFIX + ".query";
  public static final String JDBC_QUERY_FIELD_NAMES = JDBC_CONFIG_PREFIX + ".query.fieldNames";
  public static final String JDBC_QUERY_FIELD_TYPES = JDBC_CONFIG_PREFIX + ".query.fieldTypes";
  public static final String JDBC_SPLIT_QUERY = JDBC_CONFIG_PREFIX + ".query.split";
  public static final String JDBC_PARTITION_COLUMN = JDBC_CONFIG_PREFIX + ".partitionColumn";
  public static final String JDBC_NUM_PARTITIONS = JDBC_CONFIG_PREFIX + ".numPartitions";
  public static final String JDBC_LOW_BOUND = JDBC_CONFIG_PREFIX + ".lowerBound";
  public static final String JDBC_UPPER_BOUND = JDBC_CONFIG_PREFIX + ".upperBound";

  private static final String JDBC_INPUTFORMAT_CLASS = "org.apache.hive.storage.jdbc.JdbcInputFormat".intern();
  private static final String JDBC_OUTPUTFORMAT_CLASS = "org.apache.hive.storage.jdbc.JdbcOutputFormat".intern();

  String type = null; // MYSQL, POSTGRES, ORACLE, DERBY, MSSQL, DB2 etc.
  String driverClassName = null;
  String jdbcUrl = null;
  String username = null;
  String password = null; // TODO convert to byte array

  public AbstractJDBCConnectorProvider(String dbName, DataConnector dataConn, String driverClass) {
    super(dbName, dataConn, driverClass);
    this.type = connector.getType().toUpperCase(); // TODO
    this.jdbcUrl = connector.getUrl();
    this.username = connector.getParameters().get(JDBC_USERNAME);
    this.password = connector.getParameters().get(JDBC_PASSWORD);
    if (this.password == null) {
      String keystore = connector.getParameters().get(JDBC_KEYSTORE);
      String key = connector.getParameters().get(JDBC_KEY);
      try {
        char[] keyValue = MetastoreConf.getValueFromKeystore(keystore, key);
        if (keyValue != null)
          this.password = new String(keyValue);
      } catch (IOException i) {
        LOG.warn("Could not read key value from keystore");
      }
    }

    try {
      warehouse = new Warehouse(MetastoreConf.newMetastoreConf());
    } catch (MetaException e) { /* ignore */ }

    try {
      Class.forName(driverClassName);
    } catch (ClassNotFoundException cnfe) {
      LOG.warn("Driver class not found in classpath:" + driverClassName);
      throw new RuntimeException("Driver class not found:" + driverClass.getClass().getName(), cnfe);
    }
  }

  @Override public void open() throws ConnectException {
    try {
      handle = DriverManager.getConnection(jdbcUrl, username, password);
      isOpen = true;
    } catch (SQLException sqle) {
      LOG.warn("Could not connect to remote data source at " + jdbcUrl);
      throw new ConnectException("Could not connect to remote datasource at " + jdbcUrl + ",cause:" + sqle.getMessage());
    }
  }

  protected Connection getConnection() {
    try {
      if (!isOpen)
        open();
    } catch (ConnectException ce) {
      throw new RuntimeException(ce.getMessage(), ce);
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
        throw new RuntimeException(sqle);
      }
    }
  }

  /**
   * Returns Hive Table objects from the remote database for tables that match a name pattern.
   * @return List A collection of objects that match the name pattern, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param regex
   */
  @Override public abstract List<Table> getTables(String regex) throws MetaException;

  /**
   * Returns a list of all table names from the remote database.
   * @return List A collection of all the table names, null if there are no tables.
   * @throws MetaException To indicate any failures with executing this API
   */
  @Override public List<String> getTableNames() throws MetaException {
    ResultSet rs = null;
    try {
      rs = getConnection().getMetaData().getTables(scoped_db, null, null, new String[] { "TABLE" });
      if (rs != null) {
        List<String> tables = new ArrayList<String>();
        while(rs.next()) {
          tables.add(rs.getString(3));
        }
        return tables;
      }
    } catch (SQLException sqle) {
      LOG.warn("Could not retrieve table names from remote datasource, cause:" + sqle.getMessage());
      throw new MetaException("Error retrieving remote table:" + sqle);
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

  protected abstract ResultSet fetchTableMetadata(String tableName) throws MetaException;

  protected abstract ResultSet fetchTableNames() throws MetaException;

  /**
   * Fetch a single table with the given name, returns a Hive Table object from the remote database
   * @return Table A Table object for the matching table, null otherwise.
   * @throws MetaException To indicate any failures with executing this API
   * @param tableName
   */
  @Override public Table getTable(String tableName) throws MetaException {
    ResultSet rs = null;
    Table table = null;
    try {
      // rs = fetchTableMetadata(tableName);
      rs = fetchTableViaDBMetaData(tableName);
      List<FieldSchema> cols = new ArrayList<>();
      while (rs.next()) {
        FieldSchema fs = new FieldSchema();
        fs.setName(rs.getString("COLUMN_NAME"));
        fs.setType(getDataType(rs.getString("TYPE_NAME"), rs.getInt("COLUMN_SIZE")));
        fs.setComment("inferred column type");
        cols.add(fs);
      }

      if (cols.size() == 0) {
        // table does not exists or could not be fetched
        return null;
      }

      table = buildTableFromColsList(tableName, cols);
      //Setting the table properties.
      table.getParameters().put(JDBC_DATABASE_TYPE, this.type);
      table.getParameters().put(JDBC_DRIVER, this.driverClassName);
      table.getParameters().put(JDBC_TABLE, tableName);
      table.getParameters().put(JDBC_URL, this.jdbcUrl);
      table.getParameters().put(hive_metastoreConstants.META_TABLE_STORAGE, JDBC_HIVE_STORAGE_HANDLER_ID);
      table.getParameters().put("EXTERNAL", "TRUE");
      Map<String, String> connectorParams = connector.getParameters();
      for (String param: connectorParams.keySet()) {
        if (param.startsWith(JDBC_CONFIG_PREFIX)) {
          table.getParameters().put(param, connectorParams.get(param));
        }
      }
      return table;
    } catch (Exception e) {
      LOG.warn("Exception retrieving remote table " + scoped_db + "." + tableName + " via data connector "
          + connector.getName());
      throw new MetaException("Error retrieving remote table:" + e);
    } finally {
      try {
        if (rs != null) {
          rs.close();
        }
      } catch (Exception ex) { /* ignore */ }
    }
  }

  private ResultSet fetchTableViaDBMetaData(String tableName) throws SQLException {
    ResultSet rs = null;
    try {
      rs = getConnection().getMetaData().getColumns(scoped_db, null, tableName, null);
    } catch (SQLException sqle) {
      LOG.warn("Could not retrieve column names from JDBC table, cause:" + sqle.getMessage());
      throw sqle;
    }
    return rs;
  }

  protected String wrapSize(int size) {
    return "(" + size + ")";
  }

  // subclasses call this first, anything that is not mappable by this code is mapped in the subclass
  protected String getDataType(String mySqlType, int size) {
    switch(mySqlType.toLowerCase())
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

  @Override protected String getInputClass() {
    return JDBC_INPUTFORMAT_CLASS;
  }

  @Override protected String getOutputClass() {
    return JDBC_INPUTFORMAT_CLASS;
  }
  @Override protected String getTableLocation(String tableName) {
    if (warehouse != null) {
      try {
        return warehouse.getDefaultTablePath(scoped_db, tableName, true).toString();
      } catch (MetaException e) {
        LOG.info("Error determining default table path, cause:" + e.getMessage());
      }
    }
    return "some_dummy_path";
  }
}
