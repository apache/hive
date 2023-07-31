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
package org.apache.hadoop.hive.metastore.txn.impl;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionCandidate;
import org.apache.hadoop.hive.metastore.txn.retryhandling.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

public class AbortedTxnHandler extends QueryHandler<Set<CompactionCandidate>> {

  private final long abortedTimeThreshold;
  private final int abortedThreshold;
  private final boolean checkAbortedTimeThreshold;
  private final long systemTime;

  //language=SQL
  @Override
  protected String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return "SELECT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", " +
        "MIN(\"TXN_STARTED\"), COUNT(*) FROM \"TXNS\", \"TXN_COMPONENTS\" " +
        " WHERE \"TXN_ID\" = \"TC_TXNID\" AND \"TXN_STATE\" = :state " +
        "GROUP BY \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\" ";
  }

  @Override
  protected SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("state", TxnStatus.ABORTED.getSqlConst(), Types.CHAR);
  }

  @Override
  public Set<CompactionCandidate> extractData(ResultSet rs) throws SQLException, DataAccessException {
    Set<CompactionCandidate> response = new HashSet<>();
    while (rs.next()) {
      boolean pastTimeThreshold = checkAbortedTimeThreshold && rs.getLong(4) + abortedTimeThreshold < systemTime;
      int numAbortedTxns = rs.getInt(5);
      if (numAbortedTxns > abortedThreshold || pastTimeThreshold) {
        CompactionCandidate candidate = new CompactionCandidate();
        candidate.dbname = rs.getString(1);
        candidate.tableName = rs.getString(2);
        candidate.partName = rs.getString(3);
        candidate.tooManyAborts = numAbortedTxns > abortedThreshold;
        candidate.hasOldAbort = pastTimeThreshold;
        response.add(candidate);
      }
    }
    return response;
  }

  public AbortedTxnHandler(long abortedTimeThreshold, int abortedThreshold) {
    this.abortedTimeThreshold = abortedTimeThreshold;
    this.abortedThreshold = abortedThreshold;
    checkAbortedTimeThreshold = abortedTimeThreshold >= 0;
    systemTime = System.currentTimeMillis();
  }
}
