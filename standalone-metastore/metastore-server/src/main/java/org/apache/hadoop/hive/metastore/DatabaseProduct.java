/*
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

package org.apache.hadoop.hive.metastore;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.Collectors;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/** Database product inferred via JDBC. Encapsulates all SQL logic associated with
 * the database product.
 * This class is a singleton, which is instantiated the first time
 * method determineDatabaseProduct is invoked.
 * Tests that need to create multiple instances can use the reset method
 * */
public class DatabaseProduct implements Configurable {
  static final private Logger LOG = LoggerFactory.getLogger(DatabaseProduct.class.getName());
  private static final Class<SQLException>[] unrecoverableSqlExceptions = new Class[]{
          // TODO: collect more unrecoverable SQLExceptions
          SQLIntegrityConstraintViolationException.class
  };

  public enum DbType {DERBY, MYSQL, POSTGRES, ORACLE, SQLSERVER, CUSTOM, UNDEFINED};
  public DbType dbType;

  // Singleton instance
  private static DatabaseProduct theDatabaseProduct;

  Configuration myConf;
  /**
   * Protected constructor for singleton class
   */
  protected DatabaseProduct() {}

  public static final String DERBY_NAME = "derby";
  public static final String SQL_SERVER_NAME = "sqlserver";
  public static final String MYSQL_NAME = "mysql";
  public static final String MARIADB_NAME = "mariadb";
  public static final String POSTGRESQL_NAME = "postgresql";
  public static final String ORACLE_NAME = "oracle";
  public static final String UNDEFINED_NAME = "other";

  /**
   * Determine the database product type
   * @param productName string to defer database connection
   * @return database product type
   */
  public static DatabaseProduct determineDatabaseProduct(String productName,
      Configuration conf) {
    DbType dbt;

    Preconditions.checkNotNull(conf, "Configuration is null");
    // Check if we are using an external database product
    boolean isExternal = MetastoreConf.getBoolVar(conf, ConfVars.USE_CUSTOM_RDBMS);

    if (theDatabaseProduct != null) {
      dbt = getDbType(productName);
      if (isExternal) {
        dbt = DbType.CUSTOM;
      }
      Preconditions.checkState(theDatabaseProduct.dbType == dbt);
      return theDatabaseProduct;
    }

    // This method may be invoked by concurrent connections
    synchronized (DatabaseProduct.class) {

      if (productName == null) {
        productName = UNDEFINED_NAME;
      }

      dbt = getDbType(productName);

      // Check for null again in case of race condition
      if (theDatabaseProduct == null) {
        if (isExternal) {
          // The DatabaseProduct will be created by instantiating an external class via
          // reflection. The external class can override any method in the current class
          String className = MetastoreConf.getVar(conf, ConfVars.CUSTOM_RDBMS_CLASSNAME);

          if (className != null) {
            try {
              theDatabaseProduct = (DatabaseProduct)
                  ReflectionUtils.newInstance(Class.forName(className), conf);

              LOG.info(String.format("Using custom RDBMS %s", className));
              dbt = DbType.CUSTOM;
            } catch (Exception e) {
              throw new RuntimeException(
                  "Caught exception instantiating custom database product", e);
            }
          } else {
            throw new RuntimeException(
                "Unexpected: metastore.use.custom.database.product was set, " +
                    "but metastore.custom.database.product.classname was not");
          }
        }

        if (theDatabaseProduct == null) {
          theDatabaseProduct = new DatabaseProduct();
        }

        theDatabaseProduct.dbType = dbt;
      }
    }
    return theDatabaseProduct;
  }

  private static DbType getDbType(String productName) {
    DbType dbt;
    productName = productName.replaceAll("\\s+", "").toLowerCase();

    if (productName.contains(DERBY_NAME)) {
      dbt = DbType.DERBY;
    } else if (productName.contains(SQL_SERVER_NAME)) {
      dbt = DbType.SQLSERVER;
    } else if (productName.contains(MYSQL_NAME) || productName.contains(MARIADB_NAME)) {
      dbt = DbType.MYSQL;
    } else if (productName.contains(ORACLE_NAME)) {
      dbt = DbType.ORACLE;
    } else if (productName.contains(POSTGRESQL_NAME)) {
      dbt = DbType.POSTGRES;
    } else {
      dbt = DbType.UNDEFINED;
    }
    return dbt;
  }

  public static boolean isRecoverableException(Throwable t) {
    return Stream.of(unrecoverableSqlExceptions)
                 .allMatch(ex -> ExceptionUtils.indexOfType(t, ex) < 0);
  }

  public final boolean isDERBY() {
    return dbType == DbType.DERBY;
  }

  public final boolean isMYSQL() {
    return dbType == DbType.MYSQL;
  }

  public final boolean isORACLE() {
    return dbType == DbType.ORACLE;
  }

  public final boolean isSQLSERVER() {
    return dbType == DbType.SQLSERVER;
  }

  public final boolean isPOSTGRES() {
    return dbType == DbType.POSTGRES;
  }

  public final boolean isCUSTOM() {
    return dbType == DbType.CUSTOM;
  }

  public final boolean isUNDEFINED() {
    return dbType == DbType.UNDEFINED;
  }

  public boolean isDeadlock(SQLException e) {
    return e instanceof SQLTransactionRollbackException
        || ((isMYSQL() || isPOSTGRES() || isSQLSERVER() || isCUSTOM())
            && "40001".equals(e.getSQLState()))
        || (isPOSTGRES() && "40P01".equals(e.getSQLState()))
        || (isORACLE() && (e.getMessage() != null && (e.getMessage().contains("deadlock detected")
            || e.getMessage().contains("can't serialize access for this transaction"))));
  }

  /**
   * Is the given exception a table not found exception
   * @param t Exception
   * @return
   */
  public boolean isTableNotExistsError(Throwable t) {
    SQLException e = TxnUtils.getSqlException(t);    
    return (isPOSTGRES() && "42P01".equalsIgnoreCase(e.getSQLState()))
        || (isMYSQL() && "42S02".equalsIgnoreCase(e.getSQLState()))
        || (isORACLE() && "42000".equalsIgnoreCase(e.getSQLState()) && e.getMessage().contains("ORA-00942"))
        || (isSQLSERVER() && "S0002".equalsIgnoreCase(e.getSQLState()) && e.getMessage().contains("Invalid object"))
        || (isDERBY() && "42X05".equalsIgnoreCase(e.getSQLState()));
  }

  /**
   * Whether the RDBMS has restrictions on IN list size (explicit, or poor perf-based).
   */
  protected boolean needsInBatching() {
    return isORACLE() || isSQLSERVER();
  }

  /**
   * Whether the RDBMS has a bug in join and filter operation order described in DERBY-6358.
   */
  protected boolean hasJoinOperationOrderBug() {
    return isDERBY() || isORACLE() || isPOSTGRES();
  }

  public String getHiveSchemaPostfix() {
    switch (dbType) {
    case SQLSERVER:
      return "mssql";
    case DERBY:
    case MYSQL:
    case POSTGRES:
    case ORACLE:
    case CUSTOM:
      return dbType.name().toLowerCase();
    case UNDEFINED:
    default:
      return null;
    }
  }

  @VisibleForTesting
  public static void reset() {
    theDatabaseProduct = null;
  }

  protected String toDate(String tableValue) {
    if (isORACLE()) {
      return "TO_DATE(" + tableValue + ", 'YYYY-MM-DD')";
    } else {
      return "cast(" + tableValue + " as date)";
    }
  }

  protected String toTimestamp(String tableValue) {
    if (isORACLE()) {
      return "TO_TIMESTAMP(" + tableValue + ", 'YYYY-MM-DD HH:mm:ss')";
    } else {
      return "cast(" + tableValue + " as TIMESTAMP)";
    }
  }

  /**
   * Returns db-specific logic to be executed at the beginning of a transaction.
   * Used in pooled connections.
   */
  public String getPrepareTxnStmt() {
    if (isMYSQL()) {
      return "SET @@session.sql_mode=ANSI_QUOTES";
    }
    else {
     return null;
    }
  }

  private static final EnumMap<DatabaseProduct.DbType, String> DB_EPOCH_FN =
      new EnumMap<DatabaseProduct.DbType, String>(DatabaseProduct.DbType.class) {{
        put(DbType.DERBY, "{ fn timestampdiff(sql_tsi_frac_second, timestamp('" + new Timestamp(0) +
            "'), current_timestamp) } / 1000000");
        put(DbType.CUSTOM, "{ fn timestampdiff(sql_tsi_frac_second, timestamp('" + new Timestamp(0) +
            "'), current_timestamp) } / 1000000");
        put(DbType.MYSQL, "round(unix_timestamp(now(3)) * 1000)");
        put(DbType.POSTGRES, "round(extract(epoch from current_timestamp) * 1000)");
        put(DbType.ORACLE, "(cast(systimestamp at time zone 'UTC' as date) - date '1970-01-01')*24*60*60*1000 " +
            "+ cast(mod( extract( second from systimestamp ), 1 ) * 1000 as int)");
        put(DbType.SQLSERVER, "datediff_big(millisecond, '19700101', sysutcdatetime())");
      }};

  private static final EnumMap<DatabaseProduct.DbType, String> DB_SEED_FN =
      new EnumMap<DatabaseProduct.DbType, String>(DatabaseProduct.DbType.class) {{
        put(DbType.DERBY, "ALTER TABLE \"TXNS\" ALTER \"TXN_ID\" RESTART WITH %s");
        put(DbType.CUSTOM, "ALTER TABLE \"TXNS\" ALTER \"TXN_ID\" RESTART WITH %s");
        put(DbType.MYSQL, "ALTER TABLE \"TXNS\" AUTO_INCREMENT = %s");
        put(DbType.POSTGRES, "ALTER SEQUENCE \"TXNS_TXN_ID_seq\" RESTART WITH %s");
        put(DbType.ORACLE, "ALTER TABLE \"TXNS\" MODIFY \"TXN_ID\" GENERATED BY DEFAULT AS IDENTITY (START WITH %s )");
        put(DbType.SQLSERVER, "DBCC CHECKIDENT ('txns', RESEED, %s )");
      }};

  public String getTxnSeedFn(long seedTxnId) {
    return String.format(DB_SEED_FN.get(dbType), seedTxnId);
  }

  /**
   * Get database specific function which returns the milliseconds value after the epoch.
   * @throws MetaException For unknown database type.
   */
  public String getMillisAfterEpochFn() throws MetaException {
    String epochFn = DB_EPOCH_FN.get(dbType);
    if (epochFn != null) {
      return epochFn;
    } else {
      String msg = "Unknown database product: " + dbType.toString();
      LOG.error(msg);
      throw new MetaException(msg);
    }
  }

  /**
   * Returns the query to fetch the current timestamp in milliseconds in the database
   * @throws MetaException when the dbType is unknown.
   */
  public String getDBTime() throws MetaException {
    String s;
    switch (dbType) {
    case DERBY:
    case CUSTOM: // ANSI SQL
      s = "values current_timestamp";
      break;

    case MYSQL:
    case POSTGRES:
    case SQLSERVER:
      s = "select current_timestamp";
      break;

    case ORACLE:
      s = "select current_timestamp from dual";
      break;

    default:
      String msg = "Unknown database product: " + dbType.toString();
      LOG.error(msg);
      throw new MetaException(msg);
  }
    return s;
  }

  /**
   * Given an expr (such as a column name) returns a database specific SQL function string
   * which when executed returns if the value of the expr is within the given interval of
   * the current timestamp in the database
   * @param expr
   * @param intervalInSeconds
   * @return
   * @throws MetaException
   */
  public String isWithinCheckInterval(String expr, long intervalInSeconds) throws MetaException {
    String condition;
    switch (dbType) {
    case DERBY:
    case CUSTOM:
      condition = " {fn TIMESTAMPDIFF(sql_tsi_second, " + expr + ", current_timestamp)} <= " + intervalInSeconds;
      break;
    case MYSQL:
    case POSTGRES:
      condition = expr + " >= current_timestamp - interval '" + intervalInSeconds + "' second";
      break;
    case SQLSERVER:
      condition = "DATEDIFF(second, " + expr + ", current_timestamp) <= " + intervalInSeconds;
      break;
    case ORACLE:
      condition = expr + " >= current_timestamp - numtodsinterval(" + intervalInSeconds + " , 'second')";
      break;
    default:
      String msg = "Unknown database product: " + dbType.toString();
      LOG.error(msg);
      throw new MetaException(msg);
  }
    return condition;
  }

  public String addForUpdateClause(String selectStatement) throws MetaException {
    switch (dbType) {
    case DERBY:
      //https://db.apache.org/derby/docs/10.1/ref/rrefsqlj31783.html
      //sadly in Derby, FOR UPDATE doesn't meant what it should
      return selectStatement;
    case MYSQL:
      //http://dev.mysql.com/doc/refman/5.7/en/select.html
    case ORACLE:
      //https://docs.oracle.com/cd/E17952_01/refman-5.6-en/select.html
    case POSTGRES:
      //http://www.postgresql.org/docs/9.0/static/sql-select.html
    case CUSTOM: // ANSI SQL
      return selectStatement + " for update";
    case SQLSERVER:
      //https://msdn.microsoft.com/en-us/library/ms189499.aspx
      //https://msdn.microsoft.com/en-us/library/ms187373.aspx
      String modifier = " with (updlock)";
      int wherePos = selectStatement.toUpperCase().indexOf(" WHERE ");
      if (wherePos < 0) {
        return selectStatement + modifier;
      }
      return selectStatement.substring(0, wherePos) + modifier +
          selectStatement.substring(wherePos, selectStatement.length());
    default:
      String msg = "Unrecognized database product name <" + dbType + ">";
      LOG.error(msg);
      throw new MetaException(msg);
    }
  }

  /**
   * Add a limit clause to a given query
   * @param numRows
   * @param noSelectsqlQuery
   * @return
   * @throws MetaException
   */
  public String addLimitClause(int numRows, String noSelectsqlQuery) throws MetaException {
    switch (dbType) {
    case DERBY:
    case CUSTOM: // ANSI SQL
      //http://db.apache.org/derby/docs/10.7/ref/rrefsqljoffsetfetch.html
      return "select " + noSelectsqlQuery + " fetch first " + numRows + " rows only";
    case MYSQL:
      //http://www.postgresql.org/docs/7.3/static/queries-limit.html
    case POSTGRES:
      //https://dev.mysql.com/doc/refman/5.0/en/select.html
      return "select " + noSelectsqlQuery + " limit " + numRows;
    case ORACLE:
      //newer versions (12c and later) support OFFSET/FETCH
      return "select * from (select " + noSelectsqlQuery + ") where rownum <= " + numRows;
    case SQLSERVER:
      //newer versions (2012 and later) support OFFSET/FETCH
      //https://msdn.microsoft.com/en-us/library/ms189463.aspx
      return "select TOP(" + numRows + ") " + noSelectsqlQuery;
    default:
      String msg = "Unrecognized database product name <" + dbType + ">";
      LOG.error(msg);
      throw new MetaException(msg);
    }
  }

  /**
   * Returns the SQL query to lock the given table name in either shared/exclusive mode
   * @param txnLockTable
   * @param shared
   * @return
   * @throws MetaException
   */
  public String lockTable(String txnLockTable, boolean shared) throws MetaException {
    switch (dbType) {
    case MYSQL:
      // For Mysql we do not use lock table statement for two reasons
      // It is not released automatically on commit/rollback
      // It requires to lock every table that will be used by the statement
      // https://dev.mysql.com/doc/refman/8.0/en/lock-tables.html
      return "SELECT  \"TXN_LOCK\" FROM \"" + txnLockTable + "\" " + (shared ? "LOCK IN SHARE MODE" : "FOR UPDATE");
    case POSTGRES:
      // https://www.postgresql.org/docs/9.4/sql-lock.html
    case DERBY:
      // https://db.apache.org/derby/docs/10.4/ref/rrefsqlj40506.html
    case ORACLE:
      // https://docs.oracle.com/cd/B19306_01/server.102/b14200/statements_9015.htm
    case CUSTOM: // ANSI SQL
      return "LOCK TABLE \"" + txnLockTable + "\" IN " + (shared ? "SHARE" : "EXCLUSIVE") + " MODE";
    case SQLSERVER:
      // https://docs.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-table?view=sql-server-ver15
      return "SELECT * FROM \"" + txnLockTable + "\" WITH (" + (shared ? "TABLOCK" : "TABLOCKX") + ", HOLDLOCK)";
    default:
      String msg = "Unrecognized database product name <" + dbType + ">";
      LOG.error(msg);
      throw new MetaException(msg);
    }
  }

  public List<String> getResetTxnSequenceStmts() {
    List<String> stmts = new ArrayList<>();
    switch (dbType) {

    case DERBY:
    case CUSTOM: //ANSI SQL
      stmts.add("ALTER TABLE \"TXNS\" ALTER \"TXN_ID\" RESTART WITH 1");
      stmts.add("INSERT INTO \"TXNS\" (\"TXN_ID\", \"TXN_STATE\", \"TXN_STARTED\","
          + " \"TXN_LAST_HEARTBEAT\", \"TXN_USER\", \"TXN_HOST\")"
          + "  VALUES(0, 'c', 0, 0, '', '')");
      break;
    case MYSQL:
      stmts.add("ALTER TABLE \"TXNS\" AUTO_INCREMENT=1");
      stmts.add("SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO,ANSI_QUOTES'");
      stmts.add("INSERT INTO \"TXNS\" (\"TXN_ID\", \"TXN_STATE\", \"TXN_STARTED\","
          + " \"TXN_LAST_HEARTBEAT\", \"TXN_USER\", \"TXN_HOST\")"
          + "  VALUES(0, 'c', 0, 0, '', '')");
      break;
    case POSTGRES:
      stmts.add("INSERT INTO \"TXNS\" (\"TXN_ID\", \"TXN_STATE\", \"TXN_STARTED\","
          + " \"TXN_LAST_HEARTBEAT\", \"TXN_USER\", \"TXN_HOST\")"
          + "  VALUES(0, 'c', 0, 0, '', '')");
      stmts.add("ALTER SEQUENCE \"TXNS_TXN_ID_seq\" RESTART");
      break;
    case ORACLE:
      stmts.add("ALTER TABLE \"TXNS\" MODIFY \"TXN_ID\" GENERATED BY DEFAULT AS IDENTITY (START WITH 1)");
      stmts.add("INSERT INTO \"TXNS\" (\"TXN_ID\", \"TXN_STATE\", \"TXN_STARTED\","
          + " \"TXN_LAST_HEARTBEAT\", \"TXN_USER\", \"TXN_HOST\")"
          + "  VALUES(0, 'c', 0, 0, '_', '_')");
      break;
    case SQLSERVER:
      stmts.add("DBCC CHECKIDENT ('txns', RESEED, 0)");
      stmts.add("SET IDENTITY_INSERT TXNS ON");
      stmts.add("INSERT INTO \"TXNS\" (\"TXN_ID\", \"TXN_STATE\", \"TXN_STARTED\","
          + " \"TXN_LAST_HEARTBEAT\", \"TXN_USER\", \"TXN_HOST\")"
          + "  VALUES(0, 'c', 0, 0, '', '')");
      break;
    case UNDEFINED:
    default:
      break;
    }
    return stmts;
  }

  public String getTruncateStatement(String name) {
    if (isPOSTGRES() || isMYSQL()) {
      return ("DELETE FROM \"" + name + "\"");
    } else {
      return("DELETE FROM " + name);
    }
  }

  /**
   * Checks if the dbms supports the getGeneratedKeys for multiline insert statements.
   * @return true if supports
   * @throws MetaException
   */
  public boolean supportsGetGeneratedKeys() throws MetaException {
    switch (dbType) {
    case DERBY:
    case CUSTOM:
    case SQLSERVER:
      // The getGeneratedKeys is not supported for multi line insert
      return false;
    case ORACLE:
    case MYSQL:
    case POSTGRES:
      return true;
    case UNDEFINED:
    default:
      String msg = "Unknown database product: " + dbType.toString();
      LOG.error(msg);
      throw new MetaException(msg);
    }
  }

  public boolean isDuplicateKeyError(Throwable t) {
    SQLException sqlEx = TxnUtils.getSqlException(t); 
    switch (dbType) {
    case DERBY:
    case CUSTOM: // ANSI SQL
      if("23505".equals(sqlEx.getSQLState())) {
        return true;
      }
      break;
    case MYSQL:
      //https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html
      if((sqlEx.getErrorCode() == 1022 || sqlEx.getErrorCode() == 1062 || sqlEx.getErrorCode() == 1586)
        && "23000".equals(sqlEx.getSQLState())) {
        return true;
      }
      break;
    case SQLSERVER:
      //2627 is unique constaint violation incl PK, 2601 - unique key
      if ((sqlEx.getErrorCode() == 2627 || sqlEx.getErrorCode() == 2601) && "23000".equals(sqlEx.getSQLState())) {
        return true;
      }
      break;
    case ORACLE:
      if(sqlEx.getErrorCode() == 1 && "23000".equals(sqlEx.getSQLState())) {
        return true;
      }
      break;
    case POSTGRES:
      //http://www.postgresql.org/docs/8.1/static/errcodes-appendix.html
      if("23505".equals(sqlEx.getSQLState())) {
        return true;
      }
      break;
    default:
      String msg = sqlEx.getMessage() +
                " (SQLState=" + sqlEx.getSQLState() + ", ErrorCode=" + sqlEx.getErrorCode() + ")";
      throw new IllegalArgumentException("Unexpected DB type: " + dbType + "; " + msg);
  }
  return false;

  }

  /**
   * Generates "Insert into T(a,b,c) values(1,2,'f'),(3,4,'c')" for appropriate DB.
   *
   * @param tblColumns e.g. "T(a,b,c)"
   * @param rows       e.g. list of Strings like 3,4,'d'
   * @param rowsCountInStmts Output the number of rows in each insert statement returned.
   * @param conf
   * @return fully formed INSERT INTO ... statements
   */
  public List<String> createInsertValuesStmt(String tblColumns, List<String> rows,
                                             List<Integer> rowsCountInStmts, Configuration conf) {
    List<String> insertStmts = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    int numRowsInCurrentStmt = 0;
    switch (dbType) {
    case ORACLE:
      if (rows.size() > 1) {
        //http://www.oratable.com/oracle-insert-all/
        //https://livesql.oracle.com/apex/livesql/file/content_BM1LJQ87M5CNIOKPOWPV6ZGR3.html
        for (int numRows = 0; numRows < rows.size(); numRows++) {
          if (numRows % MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_MAX_ELEMENTS_VALUES_CLAUSE) == 0) {
            if (numRows > 0) {
              sb.append(" select * from dual");
              insertStmts.add(sb.toString());
              if (rowsCountInStmts != null) {
                rowsCountInStmts.add(numRowsInCurrentStmt);
              }
              numRowsInCurrentStmt = 0;
            }
            sb.setLength(0);
            sb.append("insert all ");
          }
          sb.append("into ").append(tblColumns).append(" values(").append(rows.get(numRows))
              .append(") ");
          numRowsInCurrentStmt++;
        }
        sb.append("select * from dual");
        insertStmts.add(sb.toString());
        if (rowsCountInStmts != null) {
          rowsCountInStmts.add(numRowsInCurrentStmt);
        }
        return insertStmts;
      }
      //fall through
    case DERBY:
    case MYSQL:
    case POSTGRES:
    case SQLSERVER:
    case CUSTOM:
      for (int numRows = 0; numRows < rows.size(); numRows++) {
        if (numRows % MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_MAX_ELEMENTS_VALUES_CLAUSE) == 0) {
          if (numRows > 0) {
            insertStmts.add(sb.substring(0, sb.length() - 1));//exclude trailing comma
            if (rowsCountInStmts != null) {
              rowsCountInStmts.add(numRowsInCurrentStmt);
            }
            numRowsInCurrentStmt = 0;
          }
          sb.setLength(0);
          sb.append("insert into ").append(tblColumns).append(" values");
        }
        sb.append('(').append(rows.get(numRows)).append("),");
        numRowsInCurrentStmt++;
      }
      insertStmts.add(sb.substring(0, sb.length() - 1));//exclude trailing comma
      if (rowsCountInStmts != null) {
        rowsCountInStmts.add(numRowsInCurrentStmt);
      }
      return insertStmts;
    default:
      String msg = "Unrecognized database product name <" + dbType + ">";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }
  }

  public String addEscapeCharacters(String s) {
    if (isMYSQL()) {
      return s.replaceAll("\\\\", "\\\\\\\\");
    }
    return s;
  }

  /**
   * Get datasource properties for connection pools
   *
   */
  public Map<String, String> getDataSourceProperties() {
    Map<String, String> map = new HashMap<>();

    switch (dbType){
    case MYSQL:
      map.put("allowMultiQueries", "true");
      map.put("rewriteBatchedStatements", "true");
      break;
    case POSTGRES:
      map.put("reWriteBatchedInserts", "true");
      break;
    default:
      break;
    }
    return map;
  }

  /**
   * Gets the multiple row insert query for the given table with specified columns and row format
   * @param tableName table name to be used in query
   * @param columns comma separated column names string
   * @param rowFormat values format string used in the insert query. Format is like (?,?...?) and the number of
   *                  question marks in the format is equal to number of column names in the columns argument
   * @param batchCount number of rows in the query
   * @return database specific multiple row insert query
   */
  public String getBatchInsertQuery(String tableName, String columns, String rowFormat, int batchCount) {
    StringBuilder sb = new StringBuilder();
    String fixedPart = tableName + " " + columns + " values ";
    String row;
    if (isORACLE()) {
      sb.append("insert all ");
      row = "into " + fixedPart + rowFormat + " ";
    } else {
      sb.append("insert into " + fixedPart);
      row = rowFormat + ',';
    }
    for (int i = 0; i < batchCount; i++) {
      sb.append(row);
    }
    if (isORACLE()) {
      sb.append("select * from dual ");
    }
    sb.setLength(sb.length() - 1);
    return sb.toString();
  }

  /**
   * Gets the boolean value specific to database for the given input
   * @param val boolean value
   * @return database specific value
   */
  public Object getBoolean(boolean val) {
    if (isDERBY()) {
      return val ? "Y" : "N";
    }
    return val;
  }

  /**
   * Get the max rows in a query with paramSize.
   * @param batch the configured batch size
   * @param paramSize the parameter size in a query statement
   * @return the max allowed rows in a query
   */
  public int getMaxRows(int batch, int paramSize) {
    if (isSQLSERVER()) {
      // SQL Server supports a maximum of 2100 parameters in a request. Adjust the maxRowsInBatch accordingly
      int maxAllowedRows = (2100 - paramSize) / paramSize;
      return Math.min(batch, maxAllowedRows);
    }
    return batch;
  }

  // This class implements the Configurable interface for the benefit
  // of "plugin" instances created via reflection (see invocation of
  // ReflectionUtils.newInstance in method determineDatabaseProduct)
  @Override
  public Configuration getConf() {
    return myConf;
  }

  @Override
  public void setConf(Configuration c) {
    myConf = c;
  }
}
