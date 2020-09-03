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
import java.sql.SQLTransactionRollbackException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Database product infered via JDBC.
 * This class is a singleton, which is instantiated the first time
 * method determineDatabaseProduct is invoked.
 * Tests that need to create multiple instances can use the reset method
 * */
public class DatabaseProduct implements Configurable {
  static final private Logger LOG = LoggerFactory.getLogger(DatabaseProduct.class.getName());

  private static enum DbType {DERBY, MYSQL, POSTGRES, ORACLE, SQLSERVER, EXTERNAL, OTHER};

  private Configuration conf;
  public DbType dbType;
  
  private static DatabaseProduct theDatabaseProduct;
  
  /**
   * Private constructor for singleton class
   * @param id
   */
  private DatabaseProduct(DbType dbt) {
    dbType = dbt;
  }
  
  public static final String DERBY_NAME = "derby";
  public static final String SQL_SERVER_NAME = "microsoft sql server";
  public static final String MYSQL_NAME = "mysql";
  public static final String POSTGRESQL_NAME = "postgresql";
  public static final String ORACLE_NAME = "oracle";
  public static final String OTHER_NAME = "other";
  
  /**
   * Determine the database product type
   * @param productName string to defer database connection
   * @param conf Configuration object for hive-site.xml or metastore-site.xml
   * @return database product type
   */
  public static DatabaseProduct determineDatabaseProduct(String productName, Configuration conf) {
    DbType id;

    if (productName == null) {
      productName = OTHER_NAME;
    }

    productName = productName.toLowerCase();

    if (productName.contains(DERBY_NAME)) {
      id = DbType.DERBY;
    } else if (productName.contains(SQL_SERVER_NAME)) {
      id = DbType.SQLSERVER;
    } else if (productName.contains(MYSQL_NAME)) {
      id = DbType.MYSQL;
    } else if (productName.contains(ORACLE_NAME)) {
      id = DbType.ORACLE;
    } else if (productName.contains(POSTGRESQL_NAME)) {
      id = DbType.POSTGRES;
    } else {
      id = DbType.OTHER;
    }

    // If the singleton instance exists, ensure it is consistent  
    if (theDatabaseProduct != null) {
        if (theDatabaseProduct.dbType != id) {
            throw new RuntimeException(String.format("Unexpected mismatched database products. Expected=%s. Got=%s",
                    theDatabaseProduct.dbType.name(),id.name()));
        }
        return theDatabaseProduct;
    }

    if (conf == null) {
        // TODO: how to get the right conf object for hive-site.xml or metastore-site.xml?
        conf = new Configuration();
    }

    boolean isExternal = conf.getBoolean("metastore.use.custom.database.product", false) ||
            conf.getBoolean("hive.metastore.use.custom.database.product", false);

    DatabaseProduct databaseProduct = null;
    if (isExternal) {

      String className = conf.get("metastore.custom.database.product.classname");
      
      if (className != null) {
        try {
          databaseProduct = (DatabaseProduct)
              ReflectionUtils.newInstance(Class.forName(className), conf);
        }catch (Exception e) {
          LOG.warn("Unable to instantiate custom database product. Reverting to default", e);
        }
  
        id = DbType.EXTERNAL;
      }
      else {
        LOG.warn("metastore.use.custom.database.product was set, " +
                 "but metastore.custom.database.product.classname was not. Reverting to default");
      }
    }

    if (databaseProduct == null) {
      databaseProduct = new DatabaseProduct(id);
    }

    databaseProduct.setConf(conf);
    return databaseProduct;
  }

  public boolean isDeadlock(SQLException e) {
    return e instanceof SQLTransactionRollbackException
        || ((isMYSQL() || isPOSTGRES() || isSQLSERVER() || isEXTERNAL())
            && "40001".equals(e.getSQLState()))
        || (isPOSTGRES() && "40P01".equals(e.getSQLState()))
        || (isORACLE() && (e.getMessage() != null && (e.getMessage().contains("deadlock detected")
            || e.getMessage().contains("can't serialize access for this transaction"))));
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
    case EXTERNAL:
      return dbType.name().toLowerCase();
    case OTHER:
    default:
      return null;
    }
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

  public final boolean isEXTERNAL() {
    return dbType == DbType.EXTERNAL;
  }

  public final boolean isOTHER() {
    return dbType == DbType.OTHER;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
    
  @Override
  public void setConf(Configuration c) {
    conf = c;
  }

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

  public  String getPrepareTxnStmt() {
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
        put(DbType.EXTERNAL, "{ fn timestampdiff(sql_tsi_frac_second, timestamp('" + new Timestamp(0) +
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
        put(DbType.EXTERNAL, "ALTER TABLE \"TXNS\" ALTER \"TXN_ID\" RESTART WITH %s");
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
   * @param dbProduct The type of the db which is used
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

  public String getDBTime() throws MetaException {
    String s;
    switch (dbType) {
    case DERBY:
    case EXTERNAL: // ANSI SQL
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

  public String isWithinCheckInterval(String expr, long intervalInSeconds) throws MetaException {
    String condition;
    switch (dbType) {
    case DERBY:
    case EXTERNAL:
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
        // Assume ANSI SQL for external product
    case EXTERNAL:
      //http://www.postgresql.org/docs/9.0/static/sql-select.html
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

  public String addLimitClause(int numRows, String noSelectsqlQuery) throws MetaException {
    switch (dbType) {
    case DERBY:
    case EXTERNAL: // ANSI SQL
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
    case EXTERNAL: // ANSI SQL
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

  public List<String> getDataSourceProperties(Configuration conf) {
    List <String> list = new ArrayList<>();
    switch (dbType){
    case MYSQL:
      list.add("allowMultiQueries=true");
      list.add("rewriteBatchedStatements=true");
      break;
    case POSTGRES:
      list.add("reWriteBatchedInserts=true");
      break;
    default:
      break;
    }
    return list;
  }

  public List<String> getResetTxnSequenceStmts() {
    List<String> stmts = new ArrayList<>();
    switch (dbType) {

    case DERBY:
    case EXTERNAL: //ANSI SQL
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
    case OTHER:
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

  public boolean supportsGetGeneratedKeys() throws MetaException {
    switch (dbType) {
    case DERBY:
    case EXTERNAL:
    case SQLSERVER:
      // The getGeneratedKeys is not supported for multi line insert
      return false;
    case ORACLE:
    case MYSQL:
    case POSTGRES:
      return true;
    case OTHER:
    default:
      String msg = "Unknown database product: " + dbType.toString();
      LOG.error(msg);
      throw new MetaException(msg);
    }
  }

  private static String getMessage(SQLException ex) {
    return ex.getMessage() + " (SQLState=" + ex.getSQLState() + ", ErrorCode=" + ex.getErrorCode() + ")";
  }

  public boolean isDuplicateKeyError(SQLException ex) {
    switch (dbType) {
    case DERBY:
    case EXTERNAL: // ANSI SQL
      if("23505".equals(ex.getSQLState())) {
        return true;
      }
      break;
    case MYSQL:
      //https://dev.mysql.com/doc/refman/5.5/en/error-messages-server.html
      if((ex.getErrorCode() == 1022 || ex.getErrorCode() == 1062 || ex.getErrorCode() == 1586)
        && "23000".equals(ex.getSQLState())) {
        return true;
      }
      break;
    case SQLSERVER:
      //2627 is unique constaint violation incl PK, 2601 - unique key
      if ((ex.getErrorCode() == 2627 || ex.getErrorCode() == 2601) && "23000".equals(ex.getSQLState())) {
        return true;
      }
      break;
    case ORACLE:
      if(ex.getErrorCode() == 1 && "23000".equals(ex.getSQLState())) {
        return true;
      }
      break;
    case POSTGRES:
      //http://www.postgresql.org/docs/8.1/static/errcodes-appendix.html
      if("23505".equals(ex.getSQLState())) {
        return true;
      }
      break;
    default:
      throw new IllegalArgumentException("Unexpected DB type: " + dbType + "; " + getMessage(ex));
  }
  return false;

  }

  public List<String> createInsertValuesStmt(String tblColumns, List<String> rows, List<Integer> rowsCountInStmts) {
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
    case EXTERNAL:
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
}
