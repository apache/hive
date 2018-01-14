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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
   * Genereates "Insert into T(a,b,c) values(1,2,'f'),(3,4,'c')" for appropriate DB
   *
   * @param tblColumns e.g. "T(a,b,c)"
   * @param rows       e.g. list of Strings like 3,4,'d'
   * @return fully formed INSERT INTO ... statements
   */
  public List<String> createInsertValuesStmt(String tblColumns, List<String> rows) {
    if (rows == null || rows.size() == 0) {
      return Collections.emptyList();
    }
    List<String> insertStmts = new ArrayList<>();
    StringBuilder sb = new StringBuilder();
    switch (dbProduct) {
    case ORACLE:
      if (rows.size() > 1) {
        //http://www.oratable.com/oracle-insert-all/
        //https://livesql.oracle.com/apex/livesql/file/content_BM1LJQ87M5CNIOKPOWPV6ZGR3.html
        for (int numRows = 0; numRows < rows.size(); numRows++) {
          if (numRows % MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_MAX_ELEMENTS_VALUES_CLAUSE) == 0) {
            if (numRows > 0) {
              sb.append(" select * from dual");
              insertStmts.add(sb.toString());
            }
            sb.setLength(0);
            sb.append("insert all ");
          }
          sb.append("into ").append(tblColumns).append(" values(").append(rows.get(numRows))
              .append(") ");
        }
        sb.append("select * from dual");
        insertStmts.add(sb.toString());
        return insertStmts;
      }
      //fall through
    case DERBY:
    case MYSQL:
    case POSTGRES:
    case SQLSERVER:
      for (int numRows = 0; numRows < rows.size(); numRows++) {
        if (numRows % MetastoreConf.getIntVar(conf, ConfVars.DIRECT_SQL_MAX_ELEMENTS_VALUES_CLAUSE) == 0) {
          if (numRows > 0) {
            insertStmts.add(sb.substring(0, sb.length() - 1));//exclude trailing comma
          }
          sb.setLength(0);
          sb.append("insert into ").append(tblColumns).append(" values");
        }
        sb.append('(').append(rows.get(numRows)).append("),");
      }
      insertStmts.add(sb.substring(0, sb.length() - 1));//exclude trailing comma
      return insertStmts;
    default:
      String msg = "Unrecognized database product name <" + dbProduct + ">";
      LOG.error(msg);
      throw new IllegalStateException(msg);
    }
  }

  /**
   * Given a {@code selectStatement}, decorated it with FOR UPDATE or semantically equivalent
   * construct.  If the DB doesn't support, return original select.
   */
  public String addForUpdateClause(String selectStatement) throws MetaException {
    switch (dbProduct) {
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
      String msg = "Unrecognized database product name <" + dbProduct + ">";
      LOG.error(msg);
      throw new MetaException(msg);
    }
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
    switch (dbProduct) {
    case DERBY:
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
      String msg = "Unrecognized database product name <" + dbProduct + ">";
      LOG.error(msg);
      throw new MetaException(msg);
    }
  }

  public DatabaseProduct getDbProduct() {
    return dbProduct;
  }

}
