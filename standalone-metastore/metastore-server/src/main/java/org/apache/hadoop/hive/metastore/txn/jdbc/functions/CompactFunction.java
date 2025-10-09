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

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionState;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.InsertCompactionRequestCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.INITIATED_RESPONSE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.INITIATED_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.READY_FOR_CLEANING;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.REFUSED_RESPONSE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.WORKING_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getFullTableName;

public class CompactFunction implements TransactionalFunction<CompactionResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(CompactFunction.class);
  
  private final CompactionRequest rqst;
  private final long openTxnTimeOutMillis;
  private final TxnStore.MutexAPI mutexAPI;

  public CompactFunction(CompactionRequest rqst, long openTxnTimeOutMillis, TxnStore.MutexAPI mutexAPI) {
    this.rqst = rqst;
    this.openTxnTimeOutMillis = openTxnTimeOutMillis;
    this.mutexAPI = mutexAPI;
  }

  @Override
  public CompactionResponse execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    // Put a compaction request in the queue.
    TxnStore.MutexAPI.LockHandle handle = null;
    try {
      /**
       * MUTEX_KEY.CompactionScheduler lock ensures that there is only 1 entry in
       * Initiated/Working state for any resource.  This ensures that we don't run concurrent
       * compactions for any resource.
       */
      handle = mutexAPI.acquireLock(TxnStore.MUTEX_KEY.CompactionScheduler.name());

      GetValidWriteIdsRequest request = new GetValidWriteIdsRequest(
          Collections.singletonList(getFullTableName(rqst.getDbname(), rqst.getTablename())));
      final ValidCompactorWriteIdList tblValidWriteIds = TxnUtils.createValidCompactWriteIdList(
          new GetValidWriteIdsFunction(request, openTxnTimeOutMillis).execute(jdbcResource).getTblValidWriteIds().get(0));

      if (LOG.isDebugEnabled()) {
        LOG.debug("ValidCompactWriteIdList: {}", tblValidWriteIds.writeToString());
      }

      NamedParameterJdbcTemplate npJdbcTemplate = jdbcResource.getJdbcTemplate();
      Pair<Long, String> existing = npJdbcTemplate.query(
          "SELECT \"CQ_ID\", \"CQ_STATE\" FROM \"COMPACTION_QUEUE\" WHERE (\"CQ_STATE\" IN(:states) OR" +
              " (\"CQ_STATE\" = :readyForCleaningState AND \"CQ_HIGHEST_WRITE_ID\" = :highestWriteId)) AND" +
              " \"CQ_DATABASE\"= :dbName AND \"CQ_TABLE\"= :tableName AND ((:partition is NULL AND \"CQ_PARTITION\" IS NULL) OR \"CQ_PARTITION\" = :partition)",
          new MapSqlParameterSource()
              .addValue("states", Arrays.asList(Character.toString(INITIATED_STATE), Character.toString(WORKING_STATE)))
              .addValue("readyForCleaningState", READY_FOR_CLEANING, Types.VARCHAR)
              .addValue("highestWriteId", tblValidWriteIds.getHighWatermark())
              .addValue("dbName", rqst.getDbname())
              .addValue("tableName", rqst.getTablename())
              .addValue("partition", rqst.getPartitionname(), Types.VARCHAR),
          rs -> {
            if (rs.next()) {
              return new ImmutablePair<>(rs.getLong("CQ_ID"), rs.getString("CQ_STATE"));
            }
            return null;
          });
      if (existing != null) {
        String state = CompactionState.fromSqlConst(existing.getValue()).toString();
        LOG.info("Ignoring request to compact {}/{}/{} since it is already {} with id={}", rqst.getDbname(),
            rqst.getTablename(), rqst.getPartitionname(), state, existing.getKey());
        CompactionResponse resp = new CompactionResponse(-1, REFUSED_RESPONSE, false);
        resp.setErrormessage("Compaction is already scheduled with state='" + state + "' and id=" + existing.getKey());
        return resp;
      }

      long id = new GenerateCompactionQueueIdFunction().execute(jdbcResource);
      jdbcResource.execute(new InsertCompactionRequestCommand(id, CompactionState.INITIATED, rqst));
      return new CompactionResponse(id, INITIATED_RESPONSE, true);
    } finally {
      if (handle != null) {
        handle.releaseLocks();
      }
    }
  }



}
