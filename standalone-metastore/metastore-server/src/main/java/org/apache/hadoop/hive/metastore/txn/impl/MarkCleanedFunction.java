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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.retryhandling.TransactionalVoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.TransactionStatus;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.SUCCEEDED_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class MarkCleanedFunction implements TransactionalVoidFunction {

  private static final Logger LOG = LoggerFactory.getLogger(MarkCleanedFunction.class);

  private static final String DELETE_CQ_ENTRIES = "DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = :id";

  private final CompactionInfo info;
  private final DatabaseProduct dbProduct;
  private final Configuration conf;

  public MarkCleanedFunction(CompactionInfo info, DatabaseProduct dbProduct, Configuration conf) {
    this.info = info;
    this.dbProduct = dbProduct;
    this.conf = conf;
  }

  @Override
  public void call(TransactionStatus status, NamedParameterJdbcTemplate jdbcTemplate) throws SQLException, MetaException {
    MapSqlParameterSource param;
    if (!info.isAbortedTxnCleanup()) {
      param = new MapSqlParameterSource()
          .addValue("id", info.id)
          .addValue("succeeded", Character.toString(SUCCEEDED_STATE), Types.CHAR);
      jdbcTemplate.update(
          "INSERT INTO \"COMPLETED_COMPACTIONS\"(\"CC_ID\", \"CC_DATABASE\", "
              + "\"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\", \"CC_TYPE\", \"CC_TBLPROPERTIES\", \"CC_WORKER_ID\", "
              + "\"CC_START\", \"CC_END\", \"CC_RUN_AS\", \"CC_HIGHEST_WRITE_ID\", \"CC_META_INFO\", "
              + "\"CC_HADOOP_JOB_ID\", \"CC_ERROR_MESSAGE\", \"CC_ENQUEUE_TIME\", "
              + "\"CC_WORKER_VERSION\", \"CC_INITIATOR_ID\", \"CC_INITIATOR_VERSION\", "
              + "\"CC_NEXT_TXN_ID\", \"CC_TXN_ID\", \"CC_COMMIT_TIME\", \"CC_POOL_NAME\", \"CC_NUMBER_OF_BUCKETS\","
              + "\"CC_ORDER_BY\") "
              + "SELECT \"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", \"CQ_PARTITION\", "
              + ":succeeded, \"CQ_TYPE\", \"CQ_TBLPROPERTIES\", \"CQ_WORKER_ID\", \"CQ_START\", "
              + getEpochFn(dbProduct) + ", \"CQ_RUN_AS\", \"CQ_HIGHEST_WRITE_ID\", \"CQ_META_INFO\", "
              + "\"CQ_HADOOP_JOB_ID\", \"CQ_ERROR_MESSAGE\", \"CQ_ENQUEUE_TIME\", "
              + "\"CQ_WORKER_VERSION\", \"CQ_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\", "
              + "\"CQ_NEXT_TXN_ID\", \"CQ_TXN_ID\", \"CQ_COMMIT_TIME\", \"CQ_POOL_NAME\", \"CQ_NUMBER_OF_BUCKETS\", "
              + "\"CQ_ORDER_BY\" "
              + "FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = :id", param);
    }
    
    /* Remove compaction queue record corresponding to the compaction which has been successful as well as
     * remove all abort retry associated metadata of table/partition in the COMPACTION_QUEUE both when compaction
     * or abort cleanup is successful. We don't want a situation wherein we have an abort retry entry for a table
     * but no corresponding entry in TXN_COMPONENTS table. Successful compaction will delete
     * the retry metadata, so that abort cleanup is retried again (an optimistic retry approach).
     */
    removeCompactionAndAbortRetryEntries(info, jdbcTemplate);

    if (!info.isAbortedTxnCleanup()) {
      // Remove entries from completed_txn_components as well, so we don't start looking there
      // again but only up to the highest write ID include in this compaction job.
      //highestWriteId will be NULL in upgrade scenarios
      String query = "DELETE FROM \"COMPLETED_TXN_COMPONENTS\" WHERE \"CTC_DATABASE\" = :db AND \"CTC_TABLE\" = :table";
      if (info.partName != null) {
        query += " AND \"CTC_PARTITION\" = :partition";
      }
      if (info.highestWriteId != 0) {
        query += " AND \"CTC_WRITEID\" <= :writeId";
      }
      param = new MapSqlParameterSource()
          .addValue("db", info.dbname)
          .addValue("table", info.tableName)
          .addValue("writeId", info.highestWriteId);
      if (info.partName != null) {
        param.addValue("partition", info.partName);
      }
      LOG.debug("Going to execute update <{}>", query);
      int updCount = jdbcTemplate.update(query, param);
      if (updCount < 1) {
        LOG.warn("Expected to remove at least one row from completed_txn_components when " +
            "marking compaction entry as clean!");
      }
      LOG.debug("Removed {} records from completed_txn_components", updCount);
    }

    // Do cleanup of metadata in TXN_COMPONENTS table.
    removeTxnComponents(info, jdbcTemplate);
  }

  private void removeTxnComponents(CompactionInfo info, NamedParameterJdbcTemplate jdbcTemplate) {
    /*
     * compaction may remove data from aborted txns above tc_writeid bit it only guarantees to
     * remove it up to (inclusive) tc_writeid, so it's critical to not remove metadata about
     * aborted TXN_COMPONENTS above tc_writeid (and consequently about aborted txns).
     * See {@link ql.txn.compactor.Cleaner.removeFiles()}
     */
    String sql = "DELETE FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\" IN ( "
        + "SELECT \"TXN_ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = ?) "
        + "AND \"TC_DATABASE\" = ? AND \"TC_TABLE\" = ? "
        + "AND \"TC_PARTITION\" " + (info.partName != null ? "= ? " : "IS NULL ");

    List<String> queries = new ArrayList<>();
    final Iterator<Long> writeIdsIter;
    List<Integer> counts = null;

    if (info.writeIds != null && !info.writeIds.isEmpty()) {
      StringBuilder prefix = new StringBuilder(sql).append(" AND ");
      List<String> questions = Collections.nCopies(info.writeIds.size(), "?");

      counts = TxnUtils.buildQueryWithINClauseStrings(conf, queries, prefix,
          new StringBuilder(), questions, "\"TC_WRITEID\"", false, false);
      writeIdsIter = info.writeIds.iterator();
    } else {
      writeIdsIter = null;
      if (!info.hasUncompactedAborts) {
        if (info.highestWriteId != 0) {
          sql += " AND \"TC_WRITEID\" <= ?";
        }
        queries.add(sql);
      }
    }

    int totalCount = 0;
    for (int i = 0; i < queries.size(); i++) {
      String query = queries.get(i);
      int writeIdCount = (counts != null) ? counts.get(i) : 0;
      totalCount += Objects.requireNonNull(jdbcTemplate.getJdbcTemplate().execute((ConnectionCallback<Integer>) con -> {
        int sumCount = 0;        
        try (PreparedStatement pStmt = con.prepareStatement(query)) {
          LOG.debug("Going to execute update <{}>", query);
          int paramCount = 1;
          pStmt.setString(paramCount++, TxnStatus.ABORTED.getSqlConst());
          pStmt.setString(paramCount++, info.dbname);
          pStmt.setString(paramCount++, info.tableName);
          if (info.partName != null) {
            pStmt.setString(paramCount++, info.partName);
          }
          if (info.highestWriteId != 0 && writeIdCount == 0) {
            pStmt.setLong(paramCount, info.highestWriteId);
          }
          for (int j = 0; j < writeIdCount; j++) {
            if (writeIdsIter.hasNext()) {
              pStmt.setLong(paramCount + j, writeIdsIter.next());
            }
          }
          sumCount += pStmt.executeUpdate();
        }
        return sumCount;
      }));
      LOG.debug("Removed {} records from txn_components", totalCount);
    }
  }

  private void removeCompactionAndAbortRetryEntries(CompactionInfo info, NamedParameterJdbcTemplate jdbcTemplate) {
    // Do not perform delete when the related records do not exist.
    // This is valid in case of no abort retry.
    if (info.id == 0) {
      return;
    }

    String query = DELETE_CQ_ENTRIES;
    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("id", info.id);
    
    if (!info.isAbortedTxnCleanup()) {
      query += " OR ( \"CQ_DATABASE\" = :db AND \"CQ_TABLE\" = :table AND \"CQ_PARTITION\" " +
          (info.partName != null ? " = :partition" : "IS NULL") + " AND \"CQ_TYPE\" = :type )";

      params.addValue("db", info.dbname)
            .addValue("table", info.tableName)
            .addValue("type", Character.toString(TxnStore.ABORT_TXN_CLEANUP_TYPE), Types.CHAR);
      if (info.partName != null) {
        params.addValue("partition", info.partName);
      }
    }

    LOG.debug("Going to execute update <{}>", query);
    int rc = jdbcTemplate.update(query,params);
    LOG.debug("Removed {} records in COMPACTION_QUEUE", rc);
  }  
  
}
