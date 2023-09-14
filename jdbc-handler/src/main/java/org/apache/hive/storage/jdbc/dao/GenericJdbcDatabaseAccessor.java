/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.storage.jdbc.dao;

import com.google.common.base.Preconditions;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hive.storage.jdbc.conf.JdbcStorageConfig;
import org.apache.hive.storage.jdbc.conf.JdbcStorageConfigManager;
import org.apache.hive.storage.jdbc.exception.HiveJdbcDatabaseAccessException;

import javax.sql.DataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.MoreObjects.firstNonNull;

/**
 * A data accessor that should in theory work with all JDBC compliant database drivers.
 */
public class GenericJdbcDatabaseAccessor implements DatabaseAccessor {

  protected static final String DBCP_CONFIG_PREFIX = Constants.JDBC_CONFIG_PREFIX + ".dbcp";
  protected static final int DEFAULT_FETCH_SIZE = 1000;
  protected static final Logger LOGGER = LoggerFactory.getLogger(GenericJdbcDatabaseAccessor.class);
  private DataSource dbcpDataSource = null;
  static final Pattern fromPattern = Pattern.compile("(.*?\\sfrom\\s)(.*+)", Pattern.CASE_INSENSITIVE|Pattern.DOTALL);

  private static ColumnMetadataAccessor<TypeInfo> typeInfoTranslator = (meta, col) -> {
    JDBCType type = JDBCType.valueOf(meta.getColumnType(col));
    int prec = meta.getPrecision(col);
    int scal = meta.getScale(col);
    switch (type) {
    case BIT:
    case BOOLEAN:
      return TypeInfoFactory.booleanTypeInfo;
    case TINYINT:
      return TypeInfoFactory.byteTypeInfo;
    case SMALLINT:
      return TypeInfoFactory.shortTypeInfo;
    case INTEGER:
      return TypeInfoFactory.intTypeInfo;
    case BIGINT:
      return TypeInfoFactory.longTypeInfo;
    case CHAR:
      return TypeInfoFactory.getCharTypeInfo(prec);
    case VARCHAR:
    case NVARCHAR:
    case LONGNVARCHAR:
    case LONGVARCHAR:
      return TypeInfoFactory.getVarcharTypeInfo(Math.min(prec, 65535));
    case DOUBLE:
      return TypeInfoFactory.doubleTypeInfo;
    case REAL:
    case FLOAT:
      return TypeInfoFactory.floatTypeInfo;
    case DATE:
      return TypeInfoFactory.dateTypeInfo;
    case TIMESTAMP:
    case TIMESTAMP_WITH_TIMEZONE:
      return TypeInfoFactory.timestampTypeInfo;
    case DECIMAL:
    case NUMERIC:
      return TypeInfoFactory.getDecimalTypeInfo(Math.min(prec, 38), scal);
    case ARRAY:
      // Best effort with the info that we have at the moment
      return TypeInfoFactory.getListTypeInfo(TypeInfoFactory.unknownTypeInfo);
    case STRUCT:
      // Best effort with the info that we have at the moment
      return TypeInfoFactory.getStructTypeInfo(Collections.emptyList(), Collections.emptyList());
    default:
      return TypeInfoFactory.unknownTypeInfo;
    }
  };

  public GenericJdbcDatabaseAccessor() {
  }

  private <T> List<T> getColumnMetadata(Configuration conf, ColumnMetadataAccessor<T> colAccessor)
      throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String tableName = getQualifiedTableName(conf);
      // Order is important since we need to obtain the original (as specified by the user) column names. JDBC_QUERY
      // may be a generated/optimized query set by CBO with potentially different aliases than the original columns. 
      String query = firstNonNull(selectAllFromTable(tableName), conf.get(Constants.JDBC_QUERY));
      String metadataQuery = getMetaDataQuery(query);
      LOGGER.debug("Query to execute is [{}]", metadataQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(metadataQuery);
      rs = ps.executeQuery();

      return getColumnMetadata(rs, colAccessor);
    }
    catch (Exception e) {
      throw new HiveJdbcDatabaseAccessException("Caught exception while trying to get columns: " + e.getMessage(), e);
    }
    finally {
      cleanupResources(conn, ps, rs);
    }

  }

  private <T> List<T> getColumnMetadata(ResultSet rs, ColumnMetadataAccessor<T> colAccessor)
      throws Exception {
    assert rs != null;
    ResultSetMetaData metadata = rs.getMetaData();
    int numColumns = metadata.getColumnCount();
    List<T> columnMeta = new ArrayList<>(numColumns);
    for (int i = 0; i < numColumns; i++) {
      columnMeta.add(colAccessor.get(metadata, i + 1));
    }
    return columnMeta;
  }

  @Override
  public List<String> getColumnNames(Configuration conf) throws HiveJdbcDatabaseAccessException {
    return getColumnMetadata(conf, ResultSetMetaData::getColumnName);
  }

  @Override
  public List<TypeInfo> getColumnTypes(Configuration conf) throws HiveJdbcDatabaseAccessException {
    return getColumnMetadata(conf, typeInfoTranslator);
  }

  protected List<String> getColNamesFromRS(ResultSet rs) throws Exception {
    return getColumnMetadata(rs, ResultSetMetaData::getColumnName);
  }

  protected List<TypeInfo> getColTypesFromRS(ResultSet rs) throws Exception {
    return getColumnMetadata(rs, typeInfoTranslator);
  }

  protected String getMetaDataQuery(String sql) {
    return addLimitToQuery(sql, 1);
  }

  @Override
  public int getTotalNumberOfRecords(Configuration conf) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String tableName = getQualifiedTableName(conf);
      // Always use JDBC_QUERY if available both for correctness and performance. JDBC_QUERY can be set by the user
      // or the CBO including pushdown optimizations. SELECT all query should be used only when JDBC_QUERY is null.
      String sql = firstNonNull(conf.get(Constants.JDBC_QUERY), selectAllFromTable(tableName));
      String countQuery = "SELECT COUNT(*) FROM (" + sql + ") tmptable";
      LOGGER.info("Query to execute is [{}]", countQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(countQuery);
      rs = ps.executeQuery();
      if (rs.next()) {
        return rs.getInt(1);
      }
      else {
        LOGGER.warn("The count query did not return any results.", countQuery);
        throw new HiveJdbcDatabaseAccessException("Count query did not return any results.");
      }
    }
    catch (HiveJdbcDatabaseAccessException he) {
      throw he;
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to get the number of records: " + e.getMessage(), e);
      throw new HiveJdbcDatabaseAccessException(e);
    }
    finally {
      cleanupResources(conn, ps, rs);
    }
  }


  @Override
  public JdbcRecordIterator
    getRecordIterator(Configuration conf, String partitionColumn, String lowerBound, String upperBound, int limit, int
          offset) throws
          HiveJdbcDatabaseAccessException {

    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      initializeDatabaseConnection(conf);
      String tableName = getQualifiedTableName(conf);
      // Always use JDBC_QUERY if available both for correctness and performance. JDBC_QUERY can be set by the user
      // or the CBO including pushdown optimizations. SELECT all query should be used only when JDBC_QUERY is null.
      String sql = firstNonNull(conf.get(Constants.JDBC_QUERY), selectAllFromTable(tableName));
      String partitionQuery;
      if (partitionColumn != null) {
        partitionQuery = addBoundaryToQuery(tableName, sql, partitionColumn, lowerBound, upperBound);
      } else {
        partitionQuery = addLimitAndOffsetToQuery(sql, limit, offset);
      }
      LOGGER.info("Query to execute is [{}]", partitionQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(partitionQuery, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
      ps.setFetchSize(getFetchSize(conf));
      rs = ps.executeQuery();

      return new JdbcRecordIterator(this, conn, ps, rs, conf);
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to execute query", e);
      cleanupResources(conn, ps, rs);
      throw new HiveJdbcDatabaseAccessException("Caught exception while trying to execute query: " + e.getMessage(), e);
    }
  }

  public RecordWriter getRecordWriter(TaskAttemptContext context)
          throws IOException {
    Configuration conf = context.getConfiguration();
    String tableName = getQualifiedTableName(conf);

    if (tableName == null || tableName.isEmpty()) {
      throw new IllegalArgumentException("Table name should be defined");
    }
    Connection conn = null;
    PreparedStatement ps = null;
    String[] columnNames = conf.get(serdeConstants.LIST_COLUMNS).split(",");

    try {
      initializeDatabaseConnection(conf);
      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(constructQuery(tableName, columnNames));
      return new org.apache.hadoop.mapreduce.lib.db.DBOutputFormat()
              .new DBRecordWriter(conn, ps) {
        @Override
        public void close(TaskAttemptContext context) throws IOException {
          try {
            super.close(context);
          } finally {
            GenericJdbcDatabaseAccessor.this.close();
          }
        }
      };
    } catch (Exception e) {
      cleanupResources(conn, ps, null);
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Constructs the query used as the prepared statement to insert data.
   *
   * @param table
   *          the table to insert into
   * @param columnNames
   *          the columns to insert into
   */
  protected String constructQuery(String table, String[] columnNames) {
    if(columnNames == null) {
      throw new IllegalArgumentException("Column names may not be null");
    }

    StringBuilder query = new StringBuilder();
    query.append("INSERT INTO ").append(table).append(" VALUES (");

    for (int i = 0; i < columnNames.length; i++) {
      query.append("?");
      if(i != columnNames.length - 1) {
        query.append(",");
      }
    }
    query.append(");");
    return query.toString();
  }

  /**
   * Uses generic JDBC escape functions to add a limit and offset clause to a query string
   *
   * @param sql
   * @param limit
   * @param offset
   * @return
   */
  protected String addLimitAndOffsetToQuery(String sql, int limit, int offset) {
    if (offset == 0) {
      return addLimitToQuery(sql, limit);
    } else if (limit != -1) {
      return sql + " {LIMIT " + limit + " OFFSET " + offset + "}";
    } else {
      return sql + " {OFFSET " + offset + "}";
    }
  }

  /*
   * Uses generic JDBC escape functions to add a limit clause to a query string
   */
  protected String addLimitToQuery(String sql, int limit) {
    if (limit == -1) {
      return sql;
    }
    return sql + " {LIMIT " + limit + "}";
  }

  protected String addBoundaryToQuery(String tableName, String sql, String partitionColumn, String lowerBound,
          String upperBound) {
    String boundaryQuery;
    if (tableName != null) {
      boundaryQuery = "SELECT * FROM " + tableName + " WHERE ";
    } else {
      boundaryQuery = "SELECT * FROM (" + sql + ") tmptable WHERE ";
    }
    if (lowerBound != null) {
      boundaryQuery += quote() + partitionColumn + quote() + " >= " + lowerBound;
    }
    if (upperBound != null) {
      if (lowerBound != null) {
        boundaryQuery += " AND ";
      }
      boundaryQuery += quote() + partitionColumn + quote() + " < " + upperBound;
    }
    if (lowerBound == null && upperBound != null) {
      boundaryQuery += " OR " + quote() + partitionColumn + quote() + " IS NULL";
    }
    String result;
    if (tableName != null) {
      // Looking for table name in from clause, replace with the boundary query
      // TODO consolidate this
      // Currently only use simple string match, this should be improved by looking
      // for only table name in from clause
      String tableString = null;
      Matcher m = fromPattern.matcher(sql);
      Preconditions.checkArgument(m.matches());
      String queryBeforeFrom = m.group(1);
      String queryAfterFrom = " " + m.group(2) + " ";

      Character[] possibleDelimits = new Character[] {'`', '\"', ' '};
      for (Character possibleDelimit : possibleDelimits) {
        if (queryAfterFrom.contains(possibleDelimit + tableName + possibleDelimit)) {
          tableString = possibleDelimit + tableName + possibleDelimit;
          break;
        }
      }
      if (tableString == null) {
        throw new RuntimeException("Cannot find " + tableName + " in sql query " + sql);
      }
      result = queryBeforeFrom + queryAfterFrom.replace(tableString, " (" + boundaryQuery + ") " + tableName + " ");
    } else {
      result = boundaryQuery;
    }
    return result;
  }

  protected void cleanupResources(Connection conn, PreparedStatement ps, ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during resultset cleanup.", e);
    }

    try {
      if (ps != null) {
        ps.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during statement cleanup.", e);
    }

    try {
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
      LOGGER.warn("Caught exception during connection cleanup.", e);
    }
  }

  protected void initializeDatabaseConnection(Configuration conf) throws Exception {
    if (dbcpDataSource == null) {
      synchronized (this) {
        if (dbcpDataSource == null) {
          Properties props = getConnectionPoolProperties(conf);
          dbcpDataSource = BasicDataSourceFactory.createDataSource(props);
        }
      }
    }
  }

  private static String removeDbcpPrefix(String key) {
    if (key.startsWith(DBCP_CONFIG_PREFIX + ".")) {
      return key.substring(DBCP_CONFIG_PREFIX.length() + 1);
    }
    return key;
  }

  private String getFromProperties(Properties dbProperties, String key) {
    return dbProperties.getProperty(removeDbcpPrefix(key));
  }

  protected Properties getConnectionPoolProperties(Configuration conf) throws Exception {
    // Create the default properties object
    Properties dbProperties = getDefaultDBCPProperties();

    // override with user defined properties
    Map<String, String> userProperties = conf.getValByRegex(DBCP_CONFIG_PREFIX + "\\.*");
    if ((userProperties != null) && (!userProperties.isEmpty())) {
      for (Entry<String, String> entry : userProperties.entrySet()) {
        dbProperties.put(removeDbcpPrefix(entry.getKey()), entry.getValue());
      }
    }

    String passwd = JdbcStorageConfigManager.getPasswordFromProperties(dbProperties,
        GenericJdbcDatabaseAccessor::removeDbcpPrefix);
    if (passwd != null) {
      dbProperties.put(removeDbcpPrefix(JdbcStorageConfigManager.CONFIG_PWD), passwd);
    }

    // essential properties that shouldn't be overridden by users
    dbProperties.put("url", conf.get(JdbcStorageConfig.JDBC_URL.getPropertyName()));
    dbProperties.put("driverClassName", conf.get(JdbcStorageConfig.JDBC_DRIVER_CLASS.getPropertyName()));
    dbProperties.put("type", "javax.sql.DataSource");
    return dbProperties;
  }


  protected Properties getDefaultDBCPProperties() {
    Properties props = new Properties();
    props.put("initialSize", "1");
    props.put("maxActive", "3");
    props.put("maxIdle", "0");
    props.put("maxWait", "10000");
    props.put("timeBetweenEvictionRunsMillis", "30000");
    return props;
  }


  protected int getFetchSize(Configuration conf) {
    return conf.getInt(JdbcStorageConfig.JDBC_FETCH_SIZE.getPropertyName(), DEFAULT_FETCH_SIZE);
  }

  @Override
  public Pair<String, String> getBounds(Configuration conf, String partitionColumn, boolean retrieveMin, boolean
          retrieveMax) throws HiveJdbcDatabaseAccessException {
    Connection conn = null;
    PreparedStatement ps = null;
    ResultSet rs = null;

    try {
      Preconditions.checkArgument(retrieveMin || retrieveMax);
      initializeDatabaseConnection(conf);
      String tableName = getQualifiedTableName(conf);
      // Order is important since we need to retain the original (as specified by the user) column names. The partition
      // column, used below, is user specified so the column names should match.
      String sql = firstNonNull(selectAllFromTable(tableName), conf.get(Constants.JDBC_QUERY));
      String minClause = "MIN(" + quote() + partitionColumn  + quote() + ")";
      String maxClause = "MAX(" + quote() + partitionColumn  + quote() + ")";
      String countQuery = "SELECT ";
      if (retrieveMin) {
        countQuery += minClause;
      }
      if (retrieveMax) {
        if (retrieveMin) {
          countQuery += ",";
        }
        countQuery += maxClause;
      }
      countQuery += " FROM (" + sql + ") tmptable " + "WHERE " + quote() + partitionColumn + quote() + " IS NOT NULL";

      LOGGER.debug("MIN/MAX Query to execute is [{}]", countQuery);

      conn = dbcpDataSource.getConnection();
      ps = conn.prepareStatement(countQuery);
      rs = ps.executeQuery();
      String lower = null, upper = null;
      int pos = 1;
      if (rs.next()) {
        if (retrieveMin) {
          lower = rs.getString(pos);
          pos++;
        }
        if (retrieveMax) {
          upper = rs.getString(pos);
        }
        return new ImmutablePair<>(lower, upper);
      }
      else {
        LOGGER.warn("The count query did not return any results.", countQuery);
        throw new HiveJdbcDatabaseAccessException("MIN/MAX query did not return any results.");
      }
    }
    catch (HiveJdbcDatabaseAccessException he) {
      throw he;
    }
    catch (Exception e) {
      LOGGER.error("Caught exception while trying to get MIN/MAX of " + partitionColumn + ": " + e.getMessage(), e);
      throw new HiveJdbcDatabaseAccessException(e);
    }
    finally {
      cleanupResources(conn, ps, rs);
    }
  }

  private String quote() {
    if (needColumnQuote()) {
      return "\"";
    } else {
      return "";
    }
  }
  @Override
  public boolean needColumnQuote() {
    return true;
  }

  private static String getQualifiedTableName(Configuration conf) {
    String tableName = conf.get(Constants.JDBC_TABLE);
    if (tableName == null) {
      return null;
    }
    String schemaName = conf.get(Constants.JDBC_SCHEMA);
    return schemaName == null ? tableName : schemaName + "." + tableName;
  }

  private static String selectAllFromTable(String tableName) {
    return tableName == null ? null : "select * from " + tableName;
  }
  
  private interface ColumnMetadataAccessor<T> {
    T get(ResultSetMetaData metadata, Integer column) throws SQLException;
  }

  @Override
  public void close() {
    if (dbcpDataSource instanceof AutoCloseable) {
      try (AutoCloseable closeable = (AutoCloseable) dbcpDataSource) {
      } catch (Exception e) {
        LOGGER.warn("Caught exception while trying to close the DataSource: "
            + e.getMessage(), e);
      }
      dbcpDataSource = null;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    close();
  }

}
