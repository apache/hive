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
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class OpenTxnTimeoutLowBoundaryTxnIdHandler implements QueryHandler<Long> {
  
  private final long openTxnTimeOutMillis;

  public OpenTxnTimeoutLowBoundaryTxnIdHandler(long openTxnTimeOutMillis) {
    this.openTxnTimeOutMillis = openTxnTimeOutMillis;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return "SELECT MAX(\"TXN_ID\") FROM \"TXNS\" WHERE \"TXN_STARTED\" < (" + getEpochFn(databaseProduct) + " - "
        + openTxnTimeOutMillis + ")";
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource();
  }

  @Override
  public Long extractData(ResultSet rs) throws SQLException, DataAccessException {
    rs.next();
    long result = rs.getLong(1);
    if (rs.wasNull()) {
      /*
       * TXNS always contains at least one transaction,
       * the row where txnid = (select max(txnid) where txn_started < epoch - TXN_OPENTXN_TIMEOUT) is never deleted
       */
      throw new SQLException("Transaction tables not properly " + "initialized, null record found in MAX(TXN_ID)");
    }
    return result;
  }
}
