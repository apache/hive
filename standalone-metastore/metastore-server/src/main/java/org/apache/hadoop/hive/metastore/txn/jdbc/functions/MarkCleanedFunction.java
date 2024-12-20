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

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.jdbc.InClauseBatchCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;
import java.util.ArrayList;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.SUCCEEDED_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class MarkCleanedFunction implements TransactionalFunction<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(MarkCleanedFunction.class);

  private final CompactionInfo info;

  public MarkCleanedFunction(CompactionInfo info) {
    this.info = info;
  }

  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    NamedParameterJdbcTemplate jdbcTemplate = jdbcResource.getJdbcTemplate();
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
              + getEpochFn(jdbcResource.getDatabaseProduct()) + ", \"CQ_RUN_AS\", \"CQ_HIGHEST_WRITE_ID\", \"CQ_META_INFO\", "
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
    removeTxnComponents(info, jdbcResource);
    return null;
  }

  private void removeTxnComponents(CompactionInfo info, MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    /*
     * compaction may remove data from aborted txns above tc_writeid bit it only guarantees to
     * remove it up to (inclusive) tc_writeid, so it's critical to not remove metadata about
     * aborted TXN_COMPONENTS above tc_writeid (and consequently about aborted txns).
     * See {@link ql.txn.compactor.Cleaner.removeFiles()}
     */

    MapSqlParameterSource params = new MapSqlParameterSource()
        .addValue("state", TxnStatus.ABORTED.getSqlConst(), Types.CHAR)
        .addValue("db", info.dbname)
        .addValue("table", info.tableName)
        .addValue("partition", info.partName);

    int totalCount = 0;
    if (!info.hasUncompactedAborts && info.highestWriteId != 0) {
      totalCount = jdbcResource.getJdbcTemplate().update(
          "DELETE FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\" IN ( "
              + "SELECT \"TXN_ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = :state) "
              + "AND \"TC_DATABASE\" = :db AND \"TC_TABLE\" = :table "
              + "AND (:partition is NULL OR \"TC_PARTITION\" = :partition) "
              + "AND \"TC_WRITEID\" <= :id",
          params.addValue("id", info.highestWriteId));
    } else if (CollectionUtils.isNotEmpty(info.writeIds)) {
      params.addValue("ids", new ArrayList<>(info.writeIds));
      totalCount = jdbcResource.execute(new InClauseBatchCommand<>(
          "DELETE FROM \"TXN_COMPONENTS\" WHERE \"TC_TXNID\" IN ( "
              + "SELECT \"TXN_ID\" FROM \"TXNS\" WHERE \"TXN_STATE\" = :state) "
              + "AND \"TC_DATABASE\" = :db AND \"TC_TABLE\" = :table "
              + "AND (:partition is NULL OR \"TC_PARTITION\" = :partition) "
              + "AND \"TC_WRITEID\" IN (:ids)", 
          params, "ids", Long::compareTo));
    }
    LOG.debug("Removed {} records from txn_components", totalCount);
  }

  private void removeCompactionAndAbortRetryEntries(CompactionInfo info, NamedParameterJdbcTemplate jdbcTemplate) {
    // Do not perform delete when the related records do not exist.
    // This is valid in case of no abort retry.
    if (info.id == 0) {
      return;
    }

    MapSqlParameterSource params = new MapSqlParameterSource("id", info.id);
    String query;
    if (info.isAbortedTxnCleanup()) {
      query = "DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = :id";
    } else {
      query = "DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = :id " +
          "OR (\"CQ_DATABASE\" = :db AND \"CQ_TABLE\" = :table AND \"CQ_TYPE\" = :type AND (:partition is NULL OR \"CQ_PARTITION\" = :partition))";
      params.addValue("db", info.dbname)
          .addValue("table", info.tableName)
          .addValue("type", Character.toString(TxnStore.ABORT_TXN_CLEANUP_TYPE), Types.CHAR)
          .addValue("partition", info.partName, Types.VARCHAR);
    }

    LOG.debug("Going to execute update <{}>", query);
    int rc = jdbcTemplate.update(query, params);
    LOG.debug("Removed {} records in COMPACTION_QUEUE", rc);
  }

}
