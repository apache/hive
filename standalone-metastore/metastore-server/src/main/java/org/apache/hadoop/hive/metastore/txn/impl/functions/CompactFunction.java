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
package org.apache.hadoop.hive.metastore.txn.impl.functions;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.CompactionState;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.txn.retryhandling.SqlRetryHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.INITIATED_RESPONSE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.INITIATED_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.READY_FOR_CLEANING;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.REFUSED_RESPONSE;
import static org.apache.hadoop.hive.metastore.txn.TxnStore.WORKING_STATE;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;
import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getFullTableName;

public class CompactFunction implements TransactionalFunction<CompactionResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(CompactFunction.class);
  
  private final CompactionRequest rqst;
  private final long openTxnTimeOutMillis;
  private final SQLGenerator sqlGenerator;
  private final TxnStore.MutexAPI mutexAPI;

  public CompactFunction(CompactionRequest rqst, long openTxnTimeOutMillis, SQLGenerator sqlGenerator, TxnStore.MutexAPI mutexAPI) {
    this.rqst = rqst;
    this.openTxnTimeOutMillis = openTxnTimeOutMillis;
    this.sqlGenerator = sqlGenerator;
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
              " \"CQ_DATABASE\"= :dbName AND \"CQ_TABLE\"= :tableName AND (:partition is NULL OR \"CQ_PARTITION\" = :partition)",
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

      long id = generateCompactionQueueId(jdbcResource.getConnection().createStatement());
      npJdbcTemplate.update("INSERT INTO \"COMPACTION_QUEUE\" (\"CQ_ID\", \"CQ_DATABASE\", \"CQ_TABLE\", " +
          "\"CQ_PARTITION\", \"CQ_STATE\", \"CQ_TYPE\", \"CQ_ENQUEUE_TIME\", \"CQ_POOL_NAME\", \"CQ_NUMBER_OF_BUCKETS\", " +
          "\"CQ_ORDER_BY\", \"CQ_TBLPROPERTIES\", \"CQ_RUN_AS\", \"CQ_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\") " +
          "VALUES(:id, :dbName, :tableName, :partition, :state, :type, " + getEpochFn(jdbcResource.getDatabaseProduct()) + 
              ", :poolName, :buckets, :orderBy, :tblProperties, :runAs, :initiatorId, :initiatorVersion)", 
          new MapSqlParameterSource()
              .addValue("id", id)
              .addValue("dbName", rqst.getDbname())
              .addValue("tableName", rqst.getTablename())
              .addValue("partition", rqst.getPartitionname(), Types.VARCHAR)
              .addValue("state", INITIATED_STATE, Types.VARCHAR)
              .addValue("type", TxnUtils.thriftCompactionType2DbType(rqst.getType()), Types.VARCHAR)
              .addValue("poolName", rqst.getPoolName())
              .addValue("buckets", rqst.isSetNumberOfBuckets() ? rqst.getNumberOfBuckets() : null, Types.INTEGER)
              .addValue("orderBy", rqst.getOrderByClause(), Types.VARCHAR)
              .addValue("tblProperties", rqst.getProperties(), Types.VARCHAR)
              .addValue("runAs", rqst.getRunas(), Types.VARCHAR)
              .addValue("initiatorId", rqst.getInitiatorId(), Types.VARCHAR)
              .addValue("initiatorVersion", rqst.getInitiatorVersion(), Types.VARCHAR));
      return new CompactionResponse(id, INITIATED_RESPONSE, true);
    } catch (SQLException e) {
      throw new MetaException("Unable to put the compaction request into the queue: " + SqlRetryHandler.getMessage(e));
    } finally {
      if (handle != null) {
        handle.releaseLocks();
      }
    }
  }

  @Deprecated
  long generateCompactionQueueId(Statement stmt) throws SQLException, MetaException {
    // Get the id for the next entry in the 
    String s = sqlGenerator.addForUpdateClause("SELECT \"NCQ_NEXT\" FROM \"NEXT_COMPACTION_QUEUE_ID\"");
    LOG.debug("going to execute query <{}>", s);
    try (ResultSet rs = stmt.executeQuery(s)) {
      if (!rs.next()) {
        throw new IllegalStateException("Transaction tables not properly initiated, "
            + "no record found in next_compaction_queue_id");
      }
      long id = rs.getLong(1);
      s = "UPDATE \"NEXT_COMPACTION_QUEUE_ID\" SET \"NCQ_NEXT\" = " + (id + 1) + " WHERE \"NCQ_NEXT\" = " + id;
      LOG.debug("Going to execute update <{}>", s);
      if (stmt.executeUpdate(s) != 1) {
        //TODO: Eliminate this id generation by implementing: https://issues.apache.org/jira/browse/HIVE-27121
        LOG.info("The returned compaction ID ({}) already taken, obtaining new", id);
        return generateCompactionQueueId(stmt);
      }
      return id;
    }
  }

}
