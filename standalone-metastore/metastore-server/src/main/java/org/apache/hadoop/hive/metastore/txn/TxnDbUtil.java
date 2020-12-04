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
package org.apache.hadoop.hive.metastore.txn;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.IMetaStoreSchemaInfo;
import org.apache.hadoop.hive.metastore.MetaStoreSchemaInfoFactory;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.DatabaseProduct.*;

/**
 * Utility methods for creating and destroying txn database/schema, plus methods for
 * querying against metastore tables.
 * Placed here in a separate class so it can be shared across unit tests.
 */
public final class TxnDbUtil {

  private static final Logger LOG = LoggerFactory.getLogger(TxnDbUtil.class.getName());
  private static final String TXN_MANAGER = "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager";

  private static int deadlockCnt = 0;

  private TxnDbUtil() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  /**
   * Set up the configuration so it will use the DbTxnManager, concurrency will be set to true,
   * and the JDBC configs will be set for putting the transaction and lock info in the embedded
   * metastore.
   *
   * @param conf HiveConf to add these values to
   */
  public static void setConfValues(Configuration conf) {
    MetastoreConf.setVar(conf, ConfVars.HIVE_TXN_MANAGER, TXN_MANAGER);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
  }

  /**
   * Prepares the metastore database for unit tests.
   * Runs the latest init schema against the database configured in the CONNECT_URL_KEY param.
   * Ignores any duplication (table, index etc.) So it can be called multiple times for the same database.
   * @param conf Metastore configuration
   * @throws Exception Initialization failure
   */
  public static synchronized void prepDb(Configuration conf) throws Exception {
    LOG.info("Creating transactional tables");
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = getConnection(conf);
      String s = conn.getMetaData().getDatabaseProductName();
      DatabaseProduct dbProduct = DatabaseProduct.determineDatabaseProduct(s, conf);
      stmt = conn.createStatement();
      if (checkDbPrepared(stmt)) {
        return;
      }
      String schemaRootPath = getSchemaRootPath();
      IMetaStoreSchemaInfo metaStoreSchemaInfo =
          MetaStoreSchemaInfoFactory.get(conf, schemaRootPath, dbProduct.getHiveSchemaPostfix());
      String initFile = metaStoreSchemaInfo.generateInitFileName(null);
      try (InputStream is = new FileInputStream(
          metaStoreSchemaInfo.getMetaStoreScriptDir() + File.separator + initFile)) {
        LOG.info("Reinitializing the metastore db with {} on the database {}", initFile,
            MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY));
        importSQL(stmt, is);
      }
    } catch (SQLException e) {
      try {
        if (conn != null) {
          conn.rollback();
        }
      } catch (SQLException re) {
        LOG.error("Error rolling back: " + re.getMessage());
      }
      // This might be a deadlock, if so, let's retry
      if (e instanceof SQLTransactionRollbackException && deadlockCnt++ < 5) {
        LOG.warn("Caught deadlock, retrying db creation");
        prepDb(conf);
      } else {
        throw e;
      }
    } finally {
      deadlockCnt = 0;
      closeResources(conn, stmt, null);
    }
  }

  private static boolean checkDbPrepared(Statement stmt) {
    /*
     * If the transactional tables are already there we don't want to run everything again
     */
    try {
      stmt.execute("SELECT * FROM \"TXNS\"");
    } catch (SQLException e) {
      return false;
    }
    return true;
  }

  private static void importSQL(Statement stmt, InputStream in) throws SQLException {
    Set<String> knownErrors = getAlreadyExistsErrorCodes();
    Scanner s = new Scanner(in, "UTF-8");
    s.useDelimiter("(;(\r)?\n)|(--.*\n)");
    while (s.hasNext()) {
      String line = s.next();

      if (line.trim().length() > 0) {
        try {
          stmt.execute(line);
        } catch (SQLException e) {
          if (knownErrors.contains(e.getSQLState())) {
            LOG.debug("Ignoring sql error {}", e.getMessage());
          } else {
            throw e;
          }
        }
      }
    }
  }

  private static Set<String> getAlreadyExistsErrorCodes() {
    // function already exists, table already exists, index already exists, duplicate key
    Set<String> knownErrors = new HashSet<>();
    // derby
    knownErrors.addAll(Arrays.asList("X0Y68", "X0Y32", "X0Y44", "42Z93", "23505"));
    // postgres
    knownErrors.addAll(Arrays.asList("42P07", "42P16", "42710"));
    // mssql
    knownErrors.addAll(Arrays.asList("S0000", "S0001", "23000"));
    // mysql
    knownErrors.addAll(Arrays.asList("42S01", "HY000"));
    // oracle
    knownErrors.addAll(Arrays.asList("42000"));
    return knownErrors;
  }
  private static Set<String> getTableNotExistsErrorCodes() {
    Set<String> knownErrors = new HashSet<>();
    knownErrors.addAll(Arrays.asList("42X05", "42P01", "42S02", "S0002", "42000"));
    return knownErrors;
  }

  private static String getSchemaRootPath() {
    String hiveRoot = System.getProperty("hive.root");
    if (StringUtils.isNotEmpty(hiveRoot)) {
      return ensurePathEndsInSlash(hiveRoot) + "standalone-metastore/metastore-server/target/tmp/";
    } else {
      return ensurePathEndsInSlash(System.getProperty("test.tmp.dir", "target/tmp"));
    }
  }

  private static String ensurePathEndsInSlash(String path) {
    if (path == null) {
      throw new NullPointerException("Path cannot be null");
    }
    if (path.endsWith(File.separator)) {
      return path;
    } else {
      return path + File.separator;
    }
  }

  public static void cleanDb(Configuration conf) throws Exception {
    LOG.info("Cleaning transactional tables");

    boolean success = true;
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = getConnection(conf);
      stmt = conn.createStatement();
      if (!checkDbPrepared(stmt)){
        // Nothing to clean
        return;
      }

      // We want to try these, whether they succeed or fail.
      success &= truncateTable(conn, conf, stmt, "TXN_COMPONENTS");
      success &= truncateTable(conn, conf, stmt, "COMPLETED_TXN_COMPONENTS");
      success &= truncateTable(conn, conf, stmt, "TXNS");
      success &= truncateTable(conn, conf, stmt, "TXN_TO_WRITE_ID");
      success &= truncateTable(conn, conf, stmt, "NEXT_WRITE_ID");
      success &= truncateTable(conn, conf, stmt, "HIVE_LOCKS");
      success &= truncateTable(conn, conf, stmt, "NEXT_LOCK_ID");
      success &= truncateTable(conn, conf, stmt, "COMPACTION_QUEUE");
      success &= truncateTable(conn, conf, stmt, "NEXT_COMPACTION_QUEUE_ID");
      success &= truncateTable(conn, conf, stmt, "COMPLETED_COMPACTIONS");
      success &= truncateTable(conn, conf, stmt, "AUX_TABLE");
      success &= truncateTable(conn, conf, stmt, "WRITE_SET");
      success &= truncateTable(conn, conf, stmt, "REPL_TXN_MAP");
      success &= truncateTable(conn, conf, stmt, "MATERIALIZATION_REBUILD_LOCKS");
      success &= truncateTable(conn, conf, stmt, "MIN_HISTORY_LEVEL");
      try {
        String dbProduct = conn.getMetaData().getDatabaseProductName();
        DatabaseProduct databaseProduct = determineDatabaseProduct(dbProduct, conf);
        try {
          resetTxnSequence(databaseProduct, stmt);
          stmt.executeUpdate("INSERT INTO \"NEXT_LOCK_ID\" VALUES(1)");
          stmt.executeUpdate("INSERT INTO \"NEXT_COMPACTION_QUEUE_ID\" VALUES(1)");
        } catch (SQLException e) {
          if (!databaseProduct.isTableNotExistsError(e)) {
            LOG.error("Error initializing sequence values", e);
            success = false;
          }
        }
      } catch (SQLException e) {
        LOG.error("Unable determine database product ", e);
        success = false;
      }
      /*
       * Don't drop NOTIFICATION_LOG, SEQUENCE_TABLE and NOTIFICATION_SEQUENCE as its used by other
       * table which are not txn related to generate primary key. So if these tables are dropped
       *  and other tables are not dropped, then it will create key duplicate error while inserting
       *  to other table.
       */
    } finally {
      closeResources(conn, stmt, null);
    }
    if(success) {
      return;
    }
    throw new RuntimeException("Failed to clean up txn tables");
  }

  private static void resetTxnSequence(DatabaseProduct databaseProduct, Statement stmt) throws SQLException {
    for (String s : databaseProduct.getResetTxnSequenceStmts()) {
      stmt.execute(s);
    }
  }

  /**
   * Restarts the txnId sequence with the given seed value.
   * It is the responsibility of the caller to not set the sequence backward.
   * @param conn database connection
   * @param stmt sql statement
   * @param seedTxnId the seed value for the sequence
   * @throws SQLException ex
   */
  public static void seedTxnSequence(Connection conn, Configuration conf, Statement stmt, long seedTxnId) throws SQLException {
    String dbProduct = conn.getMetaData().getDatabaseProductName();
    DatabaseProduct databaseProduct = determineDatabaseProduct(dbProduct, conf);
    stmt.execute(databaseProduct.getTxnSeedFn(seedTxnId));
  }

  private static boolean truncateTable(Connection conn, Configuration conf, Statement stmt, String name) {
    try {
      String dbProduct = conn.getMetaData().getDatabaseProductName();
      DatabaseProduct databaseProduct = determineDatabaseProduct(dbProduct, conf);
      try {
        // We can not use actual truncate due to some foreign keys, but we don't expect much data during tests

        String s = databaseProduct.getTruncateStatement(name);
        stmt.execute(s);

        LOG.debug("Successfully truncated table " + name);
        return true;
      } catch (SQLException e) {
        if (databaseProduct.isTableNotExistsError(e)) {
          LOG.debug("Not truncating " + name + " because it doesn't exist");
          return true;
        }
        LOG.error("Unable to truncate table " + name, e);
      }
    } catch (SQLException e) {
      LOG.error("Unable determine database product ", e);
    }
    return false;
  }

  /**
   * A tool to count the number of partitions, tables,
   * and databases locked by a particular lockId.
   *
   * @param lockId lock id to look for lock components
   *
   * @return number of components, or 0 if there is no lock
   */
  public static int countLockComponents(Configuration conf, long lockId) throws Exception {
    Connection conn = null;
    PreparedStatement stmt = null;
    ResultSet rs = null;
    try {
      conn = getConnection(conf);
      stmt = conn.prepareStatement("SELECT count(*) FROM hive_locks WHERE hl_lock_ext_id = ?");
      stmt.setLong(1, lockId);
      rs = stmt.executeQuery();
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    } finally {
      closeResources(conn, stmt, rs);
    }
  }

  /**
   * Utility method used to run COUNT queries like "select count(*) from ..." against metastore tables
   * @param countQuery countQuery text
   * @return count countQuery result
   * @throws Exception
   */
  public static int countQueryAgent(Configuration conf, String countQuery) throws Exception {
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try {
      conn = getConnection(conf);
      stmt = conn.createStatement();
      rs = stmt.executeQuery(countQuery);
      if (!rs.next()) {
        return 0;
      }
      return rs.getInt(1);
    } finally {
      closeResources(conn, stmt, rs);
    }
  }
  @VisibleForTesting
  public static String queryToString(Configuration conf, String query) throws Exception {
    return queryToString(conf, query, true);
  }
  public static String queryToString(Configuration conf, String query, boolean includeHeader)
      throws Exception {
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    StringBuilder sb = new StringBuilder();
    try {
      conn = getConnection(conf);
      stmt = conn.createStatement();
      rs = stmt.executeQuery(query);
      ResultSetMetaData rsmd = rs.getMetaData();
      if(includeHeader) {
        for (int colPos = 1; colPos <= rsmd.getColumnCount(); colPos++) {
          sb.append(rsmd.getColumnName(colPos)).append("   ");
        }
        sb.append('\n');
      }
      while(rs.next()) {
        for (int colPos = 1; colPos <= rsmd.getColumnCount(); colPos++) {
          sb.append(rs.getObject(colPos)).append("   ");
        }
        sb.append('\n');
      }
    } finally {
      closeResources(conn, stmt, rs);
    }
    return sb.toString();
  }

  /**
   * This is only for testing, it does not use the connectionPool from TxnHandler!
   * @param conf
   * @param query
   * @throws Exception
   */
  @VisibleForTesting
  public static void executeUpdate(Configuration conf, String query)
      throws Exception {
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = getConnection(conf);
      stmt = conn.createStatement();
      stmt.executeUpdate(query);
    } finally {
      closeResources(conn, stmt, null);
    }
  }

  static Connection getConnection(Configuration conf) throws Exception {
    String jdbcDriver = MetastoreConf.getVar(conf, ConfVars.CONNECTION_DRIVER);
    Driver driver = (Driver) Class.forName(jdbcDriver).newInstance();
    Properties prop = new Properties();
    String driverUrl = MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY);
    String user = MetastoreConf.getVar(conf, ConfVars.CONNECTION_USER_NAME);
    String passwd = MetastoreConf.getPassword(conf, MetastoreConf.ConfVars.PWD);
    prop.setProperty("user", user);
    prop.setProperty("password", passwd);
    Connection conn = driver.connect(driverUrl, prop);
    conn.setAutoCommit(true);
    return conn;
  }

  static void closeResources(Connection conn, Statement stmt, ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException e) {
        LOG.error("Error closing ResultSet: " + e.getMessage());
      }
    }

    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException e) {
        System.err.println("Error closing Statement: " + e.getMessage());
      }
    }

    if (conn != null) {
      try {
        conn.rollback();
      } catch (SQLException e) {
        System.err.println("Error rolling back: " + e.getMessage());
      }
      try {
        conn.close();
      } catch (SQLException e) {
        System.err.println("Error closing Connection: " + e.getMessage());
      }
    }
  }

  /**
   * Get database specific function which returns the milliseconds value after the epoch.
   * @param dbProduct The type of the db which is used
   * @throws MetaException For unknown database type.
   */
  static String getEpochFn(DatabaseProduct dbProduct) throws MetaException {
    return dbProduct.getMillisAfterEpochFn();
  }

  /**
   * Calls queries in batch, but does not return affected row numbers. Same as executeQueriesInBatch,
   * with the only difference when the db is Oracle. In this case it is called as an anonymous stored
   * procedure instead of batching, since batching is not optimized. See:
   * https://docs.oracle.com/cd/E11882_01/java.112/e16548/oraperf.htm#JJDBC28752
   * @param dbProduct The type of the db which is used
   * @param stmt Statement which will be used for batching and execution.
   * @param queries List of sql queries to execute in a Statement batch.
   * @param batchSize maximum number of queries in a single batch
   * @throws SQLException Thrown if an execution error occurs.
   */
  static void executeQueriesInBatchNoCount(DatabaseProduct dbProduct, Statement stmt, List<String> queries, int batchSize) throws SQLException {
    if (dbProduct.isORACLE()) {
      int queryCounter = 0;
      StringBuilder sb = new StringBuilder();
      sb.append("begin ");
      for (String query : queries) {
        LOG.debug("Adding query to batch: <" + query + ">");
        queryCounter++;
        sb.append(query).append(";");
        if (queryCounter % batchSize == 0) {
          sb.append("end;");
          String batch = sb.toString();
          LOG.debug("Going to execute queries in oracle anonymous statement. " + batch);
          stmt.execute(batch);
          sb.setLength(0);
          sb.append("begin ");
        }
      }
      if (queryCounter % batchSize != 0) {
        sb.append("end;");
        String batch = sb.toString();
        LOG.debug("Going to execute queries in oracle anonymous statement. " + batch);
        stmt.execute(batch);
      }
    } else {
      executeQueriesInBatch(stmt, queries, batchSize);
    }
  }

  /**
   * @param stmt Statement which will be used for batching and execution.
   * @param queries List of sql queries to execute in a Statement batch.
   * @param batchSize maximum number of queries in a single batch
   * @return A list with the number of rows affected by each query in queries.
   * @throws SQLException Thrown if an execution error occurs.
   */
  static List<Integer> executeQueriesInBatch(Statement stmt, List<String> queries, int batchSize) throws SQLException {
    List<Integer> affectedRowsByQuery = new ArrayList<>();
    int queryCounter = 0;
    for (String query : queries) {
      LOG.debug("Adding query to batch: <" + query + ">");
      queryCounter++;
      stmt.addBatch(query);
      if (queryCounter % batchSize == 0) {
        LOG.debug("Going to execute queries in batch. Batch size: " + batchSize);
        int[] affectedRecordsByQuery = stmt.executeBatch();
        Arrays.stream(affectedRecordsByQuery).forEach(affectedRowsByQuery::add);
      }
    }
    if (queryCounter % batchSize != 0) {
      LOG.debug("Going to execute queries in batch. Batch size: " + queryCounter % batchSize);
      int[] affectedRecordsByQuery = stmt.executeBatch();
      Arrays.stream(affectedRecordsByQuery).forEach(affectedRowsByQuery::add);
    }
    return affectedRowsByQuery;
  }
}
