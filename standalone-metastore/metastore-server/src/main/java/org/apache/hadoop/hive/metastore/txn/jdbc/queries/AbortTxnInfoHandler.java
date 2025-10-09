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
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.READY_FOR_CLEANING;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class AbortTxnInfoHandler implements QueryHandler<List<CompactionInfo>> {

  // Three inner sub-queries which are under left-join to fetch the required data for aborted txns.
  //language=SQL
  private static final String SELECT_ABORTS_WITH_MIN_OPEN_WRITETXN_QUERY =
      " \"res1\".\"TC_DATABASE\" AS \"DB\", \"res1\".\"TC_TABLE\" AS \"TBL\", \"res1\".\"TC_PARTITION\" AS \"PART\", " +
          " \"res1\".\"MIN_TXN_START_TIME\" AS \"MIN_TXN_START_TIME\", \"res1\".\"ABORTED_TXN_COUNT\" AS \"ABORTED_TXN_COUNT\", " +
          " \"res2\".\"MIN_OPEN_WRITE_TXNID\" AS \"MIN_OPEN_WRITE_TXNID\", \"res3\".\"RETRY_RETENTION\" AS \"RETRY_RETENTION\", " +
          " \"res3\".\"ID\" AS \"RETRY_CQ_ID\" " +
          " FROM " +
          // First sub-query - Gets the aborted txns with min txn start time, number of aborted txns
          // for corresponding db, table, partition.
          " ( SELECT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", MIN(\"TXN_STARTED\") AS \"MIN_TXN_START_TIME\", " +
          " COUNT(*) AS \"ABORTED_TXN_COUNT\" FROM \"TXNS\", \"TXN_COMPONENTS\" " +
          " WHERE \"TXN_ID\" = \"TC_TXNID\" AND \"TXN_STATE\" = :abortedState" +
          " GROUP BY \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\" %s ) \"res1\" " +
          " LEFT JOIN" +
          // Second sub-query - Gets the min open txn id for corresponding db, table, partition.
          "( SELECT \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\", MIN(\"TC_TXNID\") AS \"MIN_OPEN_WRITE_TXNID\" " +
          " FROM \"TXNS\", \"TXN_COMPONENTS\" " +
          " WHERE \"TXN_ID\" = \"TC_TXNID\" AND \"TXN_STATE\" = :openState" +
          " GROUP BY \"TC_DATABASE\", \"TC_TABLE\", \"TC_PARTITION\" ) \"res2\"" +
          " ON \"res1\".\"TC_DATABASE\" = \"res2\".\"TC_DATABASE\"" +
          " AND \"res1\".\"TC_TABLE\" = \"res2\".\"TC_TABLE\"" +
          " AND (\"res1\".\"TC_PARTITION\" = \"res2\".\"TC_PARTITION\" " +
          " OR (\"res1\".\"TC_PARTITION\" IS NULL AND \"res2\".\"TC_PARTITION\" IS NULL)) " +
          " LEFT JOIN " +
          // Third sub-query - Gets the retry entries for corresponding db, table, partition.
          "( SELECT \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", MAX(\"CQ_ID\") AS \"ID\", " +
          " MAX(\"CQ_RETRY_RETENTION\") AS \"RETRY_RETENTION\", " +
          " MIN(\"CQ_COMMIT_TIME\") - %s + MAX(\"CQ_RETRY_RETENTION\") AS \"RETRY_RECORD_CHECK\" FROM \"COMPACTION_QUEUE\" " +
          " WHERE \"CQ_TYPE\" = :type" +
          " GROUP BY \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\") \"res3\" " +
          " ON \"res1\".\"TC_DATABASE\" = \"res3\".\"CQ_DATABASE\" " +
          " AND \"res1\".\"TC_TABLE\" = \"res3\".\"CQ_TABLE\" " +
          " AND (\"res1\".\"TC_PARTITION\" = \"res3\".\"CQ_PARTITION\" " +
          " OR (\"res1\".\"TC_PARTITION\" IS NULL AND \"res3\".\"CQ_PARTITION\" IS NULL))" +
          " WHERE \"res3\".\"RETRY_RECORD_CHECK\" <= 0 OR \"res3\".\"RETRY_RECORD_CHECK\" IS NULL";

  private final long abortedTimeThreshold;
  private final int abortedThreshold;
  private final int fetchSize;
  
  public String getParameterizedQueryString(DatabaseProduct dbProduct) throws MetaException {
    return dbProduct.addLimitClause(
        fetchSize,
        String.format(AbortTxnInfoHandler.SELECT_ABORTS_WITH_MIN_OPEN_WRITETXN_QUERY,
            abortedTimeThreshold >= 0 ? "" : " HAVING COUNT(*) > " + abortedThreshold, getEpochFn(dbProduct)));
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("abortedState", TxnStatus.ABORTED.getSqlConst(), Types.CHAR)
        .addValue("openState", TxnStatus.OPEN.getSqlConst(), Types.CHAR)
        .addValue("type", Character.toString(TxnStore.ABORT_TXN_CLEANUP_TYPE), Types.CHAR);
  }

  @Override
  public List<CompactionInfo> extractData(ResultSet rs) throws DataAccessException, SQLException {
    List<CompactionInfo> readyToCleanAborts = new ArrayList<>();
    long systemTime = System.currentTimeMillis();
    boolean checkAbortedTimeThreshold = abortedTimeThreshold >= 0;
    while (rs.next()) {
      boolean pastTimeThreshold =
          checkAbortedTimeThreshold && rs.getLong("MIN_TXN_START_TIME") + abortedTimeThreshold < systemTime;
      int numAbortedTxns = rs.getInt("ABORTED_TXN_COUNT");
      if (numAbortedTxns > abortedThreshold || pastTimeThreshold) {
        CompactionInfo info = new CompactionInfo();
        info.dbname = rs.getString("DB");
        info.tableName = rs.getString("TBL");
        info.partName = rs.getString("PART");
        // In this case, this field contains min open write txn ID.
        long value = rs.getLong("MIN_OPEN_WRITE_TXNID");
        info.minOpenWriteTxnId = value > 0 ? value : Long.MAX_VALUE;
        // The specific type, state assigned to abort cleanup.
        info.type = CompactionType.ABORT_TXN_CLEANUP;
        info.state = READY_FOR_CLEANING;
        info.retryRetention = rs.getLong("RETRY_RETENTION");
        info.id = rs.getLong("RETRY_CQ_ID");
        readyToCleanAborts.add(info);
      }
    }
    return readyToCleanAborts;
  }

  public AbortTxnInfoHandler(long abortedTimeThreshold, int abortedThreshold, int fetchSize) {
    this.abortedTimeThreshold = abortedTimeThreshold;
    this.abortedThreshold = abortedThreshold;
    this.fetchSize = fetchSize;
  }
}
