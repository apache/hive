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
package org.apache.hadoop.hive.metastore.tools;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Helper class that generates SQL queries with syntax specific to target DB
 * todo: why throw MetaException?
 */
@VisibleForTesting
public final class SQLGenerator {
  static final private Logger LOG = LoggerFactory.getLogger(SQLGenerator.class.getName());
  private final DatabaseProduct dbProduct;

  private final Configuration conf;

  public SQLGenerator(DatabaseProduct dbProduct, Configuration conf) {
    this.dbProduct = dbProduct;
    this.conf = conf;
  }

  /**
   * Generates "Insert into T(a,b,c) values(1,2,'f'),(3,4,'c')" for appropriate DB
   *
   * @param tblColumns   e.g. "T(a,b,c)"
   * @param rows         e.g. list of Strings like 3,4,'d'
   * @param paramsList   List of parameters which in turn is list of Strings to be set in PreparedStatement object
   * @return List PreparedStatement objects for fully formed INSERT INTO ... statements
   */
  public List<PreparedStatement> createInsertValuesPreparedStmt(Connection dbConn,
                                                                String tblColumns, List<String> rows,
                                                                List<List<String>> paramsList)
          throws SQLException {
    if (rows == null || rows.size() == 0) {
      return Collections.emptyList();
    }
    assert((paramsList == null) || (rows.size() == paramsList.size()));

    List<Integer> rowsCountInStmts = new ArrayList<>();
    List<String> insertStmts = createInsertValuesStmt(tblColumns, rows, rowsCountInStmts);
    assert(insertStmts.size() == rowsCountInStmts.size());

    List<PreparedStatement> preparedStmts = new ArrayList<>();
    int paramsListFromIdx = 0;
    try {
      for (int stmtIdx = 0; stmtIdx < insertStmts.size(); stmtIdx++) {
        String sql = insertStmts.get(stmtIdx);
        PreparedStatement pStmt = prepareStmtWithParameters(dbConn, sql, null);
        if (paramsList != null) {
          int paramIdx = 1;
          int paramsListToIdx = paramsListFromIdx + rowsCountInStmts.get(stmtIdx);
          for (int paramsListIdx = paramsListFromIdx; paramsListIdx < paramsListToIdx; paramsListIdx++) {
            List<String> params = paramsList.get(paramsListIdx);
            for (int i = 0; i < params.size(); i++, paramIdx++) {
              pStmt.setString(paramIdx, params.get(i));
            }
          }
          paramsListFromIdx = paramsListToIdx;
        }
        preparedStmts.add(pStmt);
      }
    } catch (SQLException e) {
      for (PreparedStatement pst : preparedStmts) {
        pst.close();
      }
      throw e;
    }
    return preparedStmts;
  }

  /**
   * Generates "Insert into T(a,b,c) values(1,2,'f'),(3,4,'c')" for appropriate DB.
   *
   * @param tblColumns e.g. "T(a,b,c)"
   * @param rows       e.g. list of Strings like 3,4,'d'
   * @return fully formed INSERT INTO ... statements
   */
  public List<String> createInsertValuesStmt(String tblColumns, List<String> rows) {
    return createInsertValuesStmt(tblColumns, rows, null);
  }

  /**
   * Generates "Insert into T(a,b,c) values(1,2,'f'),(3,4,'c')" for appropriate DB.
   *
   * @param tblColumns e.g. "T(a,b,c)"
   * @param rows       e.g. list of Strings like 3,4,'d'
   * @param rowsCountInStmts Output the number of rows in each insert statement returned.
   * @return fully formed INSERT INTO ... statements
   */
  private List<String> createInsertValuesStmt(String tblColumns, List<String> rows, List<Integer> rowsCountInStmts) {
    if (rows == null || rows.size() == 0) {
      return Collections.emptyList();
    }
    return dbProduct.createInsertValuesStmt(tblColumns, rows, rowsCountInStmts, conf);
  }

  /**
   * Given a {@code selectStatement}, decorated it with FOR UPDATE or semantically equivalent
   * construct.  If the DB doesn't support, return original select.
   */
  public String addForUpdateClause(String selectStatement) throws MetaException {
    return dbProduct.addForUpdateClause(selectStatement);
  }

  /**
   * Suppose you have a query "select a,b from T" and you want to limit the result set
   * to the first 5 rows.  The mechanism to do that differs in different DBs.
   * Make {@code noSelectsqlQuery} to be "a,b from T" and this method will return the
   * appropriately modified row limiting query.
   * <p>
   * Note that if {@code noSelectsqlQuery} contains a join, you must make sure that
   * all columns are unique for Oracle.
   */
  public String addLimitClause(int numRows, String noSelectsqlQuery) throws MetaException {
    return dbProduct.addLimitClause(numRows, noSelectsqlQuery);
  }

  /**
   * Returns the SQL query to lock the given table name in either shared/exclusive mode
   * @param txnLockTable
   * @param shared
   * @return
   * @throws MetaException
   */
  public String lockTable(String txnLockTable, boolean shared) throws MetaException {
    return dbProduct.lockTable(txnLockTable, shared);
  }

  /**
   * Make PreparedStatement object with list of String type parameters to be set.
   * It is assumed the input sql string have the number of "?" equal to number of parameters
   * passed as input.
   * @param dbConn - Connection object
   * @param sql - SQL statement with "?" for input parameters.
   * @param parameters - List of String type parameters to be set in PreparedStatement object
   * @return PreparedStatement type object
   * @throws SQLException
   */
  public PreparedStatement prepareStmtWithParameters(Connection dbConn, String sql, List<String> parameters)
      throws SQLException {
    PreparedStatement pst = dbConn.prepareStatement(addEscapeCharacters(sql));
    if ((parameters == null) || parameters.isEmpty()) {
      return pst;
    }
    try {
      for (int i = 1; i <= parameters.size(); i++) {
        pst.setString(i, parameters.get(i - 1));
      }
    } catch (SQLException e) {
      pst.close();
      throw e;
    }
    return pst;
  }

  public DatabaseProduct getDbProduct() {
    return dbProduct;
  }

  // This is required for SQL executed directly. If the SQL has double quotes then some dbs tend to
  // remove the escape characters and store the variable without double quote.
  public String addEscapeCharacters(String s) {
    return dbProduct.addEscapeCharacters(s);
  }

  /**
   * Creates a lock statement for open/commit transaction based on the dbProduct in shared read / exclusive mode.
   * @param shared shared or exclusive lock
   * @return sql statement to execute
   * @throws MetaException if the dbProduct is unknown
   */
  public String createTxnLockStatement(boolean shared) throws MetaException{
    String txnLockTable = "TXN_LOCK_TBL";
    return dbProduct.lockTable(txnLockTable, shared);
  }
}
