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
package org.apache.hadoop.hive.metastore.txn.jdbc.queries;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class MinUncommittedTxnIdHandler implements QueryHandler<Long> {

  //language=SQL
  private static String minHistoryLevelSql = "SELECT MIN(\"RES\".\"ID\") AS \"ID\" FROM (" +
      " SELECT MAX(\"TXN_ID\") + 1 AS \"ID\" FROM \"TXNS\"" +
      "   UNION" +
      " SELECT MIN(\"TXN_ID\") AS \"ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = :abortedState) \"RES\"";
  //language=SQL
  private static String noMinHistoryLevelSql = "SELECT MIN(\"RES\".\"ID\") AS \"ID\" FROM (" +
      " SELECT MAX(\"TXN_ID\") + 1 AS \"ID\" FROM \"TXNS\"" +
      "   UNION" +
      " SELECT MIN(\"WS_TXNID\") AS \"ID\" FROM \"WRITE_SET\"" +
      "   UNION" +
      " SELECT MIN(\"TXN_ID\") AS \"ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = " + TxnStatus.ABORTED +
      "   OR \"TXN_STATE\" = " + TxnStatus.OPEN +
      " ) \"RES\"";

  private final boolean useMinHistoryLevel;

  public MinUncommittedTxnIdHandler(boolean useMinHistoryLevel) {
    this.useMinHistoryLevel = useMinHistoryLevel;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return useMinHistoryLevel ? minHistoryLevelSql : noMinHistoryLevelSql;
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("abortedState", TxnStatus.ABORTED.getSqlConst(), Types.CHAR);
    if (!useMinHistoryLevel) {
      params.addValue("openState", TxnStatus.OPEN.getSqlConst(), Types.CHAR);
    }
    return params;
  }

  @Override
  public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
    if (rs.next()) {
      return rs.getLong(1);
    } else {
      return null;
    }
  }

}
