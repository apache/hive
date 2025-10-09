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
package org.apache.hadoop.hive.metastore.txn.jdbc.functions;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;

public class CleanTxnToWriteIdTableFunction implements TransactionalFunction<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(CleanTxnToWriteIdTableFunction.class);

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

  private final long minTxnIdSeenOpen;

  public CleanTxnToWriteIdTableFunction(long minTxnIdSeenOpen) {
    this.minTxnIdSeenOpen = minTxnIdSeenOpen;
  }

  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    NamedParameterJdbcTemplate jdbcTemplate = jdbcResource.getJdbcTemplate();
    String sql = TxnHandler.ConfVars.useMinHistoryLevel() ? minHistoryLevelSql : noMinHistoryLevelSql;
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("abortedState", TxnStatus.ABORTED.getSqlConst(), Types.CHAR);
    if (!TxnHandler.ConfVars.useMinHistoryLevel()) {
      params.addValue("openState", TxnStatus.OPEN.getSqlConst(), Types.CHAR);
    }

    // First need to find the min_uncommitted_txnid which is currently seen by any open transactions.
    // If there are no txns which are currently open or aborted in the system, then current value of
    // max(TXNS.txn_id) could be min_uncommitted_txnid.
    Long minTxnId = jdbcTemplate.query(sql, params, rs -> {
      if (rs.next()) {
        return rs.getLong(1);
      } else {
        return null;
      }
    });

    if (minTxnId == null) {
      throw new MetaException("Transaction tables not properly initialized, no record found in TXNS");
    }
    long minUncommitedTxnid = Math.min(minTxnId, minTxnIdSeenOpen);

    // As all txns below min_uncommitted_txnid are either committed or empty_aborted, we are allowed
    // to clean up the entries less than min_uncommitted_txnid from the TXN_TO_WRITE_ID table.
    int rc = jdbcTemplate.update("DELETE FROM \"TXN_TO_WRITE_ID\" WHERE \"T2W_TXNID\" < :txnId",
        new MapSqlParameterSource("txnId", minUncommitedTxnid));
    LOG.info("Removed {} rows from TXN_TO_WRITE_ID with Txn Low-Water-Mark: {}", rc, minUncommitedTxnid);
    return null;
  }
}
