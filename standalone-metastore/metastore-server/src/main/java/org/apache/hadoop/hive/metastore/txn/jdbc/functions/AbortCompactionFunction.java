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

import org.apache.hadoop.hive.metastore.api.AbortCompactResponse;
import org.apache.hadoop.hive.metastore.api.AbortCompactionRequest;
import org.apache.hadoop.hive.metastore.api.AbortCompactionResponseElement;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionState;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.InsertCompactionInfoCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.DbTimeHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionContext;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryCallProperties;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryFunction;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryHandler;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRED;

public class AbortCompactionFunction implements TransactionalFunction<AbortCompactResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(AbortCompactionFunction.class);

  public static final String SELECT_COMPACTION_QUEUE_BY_COMPID =
      "SELECT XX.* FROM ( SELECT " +
          "   \"CQ_ID\" AS \"CC_ID\", \"CQ_DATABASE\" AS \"CC_DATABASE\", \"CQ_TABLE\" AS \"CC_TABLE\", \"CQ_PARTITION\" AS \"CC_PARTITION\", " +
          "   \"CQ_STATE\" AS \"CC_STATE\", \"CQ_TYPE\" AS \"CC_TYPE\", \"CQ_TBLPROPERTIES\" AS \"CC_TBLPROPERTIES\", \"CQ_WORKER_ID\" AS \"CC_WORKER_ID\", " +
          "   \"CQ_START\" AS \"CC_START\", \"CQ_RUN_AS\" AS \"CC_RUN_AS\", \"CQ_HIGHEST_WRITE_ID\" AS \"CC_HIGHEST_WRITE_ID\", \"CQ_META_INFO\" AS \"CC_META_INFO\"," +
          "   \"CQ_HADOOP_JOB_ID\" AS \"CC_HADOOP_JOB_ID\", \"CQ_ERROR_MESSAGE\" AS \"CC_ERROR_MESSAGE\",  \"CQ_ENQUEUE_TIME\" AS \"CC_ENQUEUE_TIME\"," +
          "   \"CQ_WORKER_VERSION\" AS \"CC_WORKER_VERSION\", \"CQ_INITIATOR_ID\" AS \"CC_INITIATOR_ID\", \"CQ_INITIATOR_VERSION\" AS \"CC_INITIATOR_VERSION\", " +
          "   \"CQ_RETRY_RETENTION\" AS \"CC_RETRY_RETENTION\", \"CQ_NEXT_TXN_ID\" AS \"CC_NEXT_TXN_ID\", \"CQ_TXN_ID\" AS \"CC_TXN_ID\", " +
          "   \"CQ_COMMIT_TIME\" AS \"CC_COMMIT_TIME\", \"CQ_POOL_NAME\" AS \"CC_POOL_NAME\",  " +
          "   \"CQ_NUMBER_OF_BUCKETS\" AS \"CC_NUMBER_OF_BUCKETS\", \"CQ_ORDER_BY\" AS \"CC_ORDER_BY\" " +
          "   FROM " +
          "   \"COMPACTION_QUEUE\" " +
          "   UNION ALL " +
          "   SELECT " +
          "   \"CC_ID\", \"CC_DATABASE\", \"CC_TABLE\", \"CC_PARTITION\", \"CC_STATE\", \"CC_TYPE\", " +
          "   \"CC_TBLPROPERTIES\", \"CC_WORKER_ID\", \"CC_START\", \"CC_RUN_AS\", " +
          "   \"CC_HIGHEST_WRITE_ID\", \"CC_META_INFO\", \"CC_HADOOP_JOB_ID\", \"CC_ERROR_MESSAGE\", " +
          "   \"CC_ENQUEUE_TIME\", \"CC_WORKER_VERSION\", \"CC_INITIATOR_ID\", \"CC_INITIATOR_VERSION\", " +
          "    -1 , \"CC_NEXT_TXN_ID\", \"CC_TXN_ID\", \"CC_NEXT_TXN_ID\", \"CC_POOL_NAME\", " +
          "   \"CC_NUMBER_OF_BUCKETS\", \"CC_ORDER_BY\" " +
          "   FROM   " +
          "   \"COMPLETED_COMPACTIONS\") XX WHERE \"CC_ID\" IN (:ids) ";
  

  private final AbortCompactionRequest reqst;
  private final SqlRetryHandler sqlRetryHandler;

  public AbortCompactionFunction(AbortCompactionRequest reqst, SqlRetryHandler sqlRetryHandler) {
    this.reqst = reqst;
    this.sqlRetryHandler = sqlRetryHandler;
  }

  @Override
  public AbortCompactResponse execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    Map<Long, AbortCompactionResponseElement> abortCompactionResponseElements = new HashMap<>();
    AbortCompactResponse response = new AbortCompactResponse(new HashMap<>());
    response.setAbortedcompacts(abortCompactionResponseElements);

    reqst.getCompactionIds().forEach(x -> abortCompactionResponseElements.put(x, getAbortCompactionResponseElement(x,"Error","No Such Compaction Id Available")));

    List<CompactionInfo> eligibleCompactionsToAbort = 
        findEligibleCompactionsToAbort(jdbcResource, abortCompactionResponseElements, reqst.getCompactionIds());
    
    CompactionAborter aborter = new CompactionAborter(jdbcResource);    
    for (CompactionInfo compactionInfo : eligibleCompactionsToAbort) {
      try {
        AbortCompactionResponseElement responseElement = sqlRetryHandler.executeWithRetry(
            new SqlRetryCallProperties().withCallerId("abortCompaction"), 
            aborter.withCompactionInfo(compactionInfo));
        abortCompactionResponseElements.put(compactionInfo.id, responseElement);
      } catch (TException e) {
        throw new MetaException(e.getMessage());
      }
    }
    return response;
  }

  private List<CompactionInfo> findEligibleCompactionsToAbort(MultiDataSourceJdbcResource jdbcResource,
      Map<Long, AbortCompactionResponseElement> abortCompactionResponseElements, List<Long> requestedCompId) {

    return jdbcResource.getJdbcTemplate().query(
        SELECT_COMPACTION_QUEUE_BY_COMPID,
        new MapSqlParameterSource().addValue("ids", requestedCompId),
        rs -> {
          List<CompactionInfo> compactionInfoList = new ArrayList<>();
          while (rs.next()) {
            char compState = rs.getString(5).charAt(0);
            long compID = rs.getLong(1);
            if (CompactionState.INITIATED.equals(CompactionState.fromSqlConst(compState))) {
              compactionInfoList.add(CompactionInfo.loadFullFromCompactionQueue(rs));
            } else {
              abortCompactionResponseElements.put(compID, getAbortCompactionResponseElement(compID,"Error",
                  "Error while aborting compaction as compaction is in state-" + CompactionState.fromSqlConst(compState)));
            }
          }
          return compactionInfoList;
        });    
  }

  private AbortCompactionResponseElement getAbortCompactionResponseElement(long compactionId, String status, String message) {
    AbortCompactionResponseElement resEle = new AbortCompactionResponseElement(compactionId);
    resEle.setMessage(message);
    resEle.setStatus(status);
    return resEle;
  }


  private class CompactionAborter implements SqlRetryFunction<AbortCompactionResponseElement> {

    private final MultiDataSourceJdbcResource jdbcResource;
    private CompactionInfo compactionInfo;

    public CompactionAborter(MultiDataSourceJdbcResource jdbcResource) {
      this.jdbcResource = jdbcResource;
    }

    public CompactionAborter withCompactionInfo(CompactionInfo compactionInfo) {
      this.compactionInfo = compactionInfo;
      return this;
    }

    @Override
    public AbortCompactionResponseElement execute() {
      try (TransactionContext context = jdbcResource.getTransactionManager().getNewTransaction(PROPAGATION_REQUIRED)) {
        compactionInfo.state = TxnStore.ABORTED_STATE;
        compactionInfo.errorMessage = "Compaction Aborted by Abort Comapction request.";
        int updCount;
        try {
          updCount = jdbcResource.execute(new InsertCompactionInfoCommand(compactionInfo, jdbcResource.execute(new DbTimeHandler()).getTime()));
        } catch (Exception e) {
          LOG.error("Unable to update compaction record: {}.", compactionInfo);
          return getAbortCompactionResponseElement(compactionInfo.id, "Error",
              "Error while aborting compaction:Unable to update compaction record in COMPLETED_COMPACTIONS");
        }
        LOG.debug("Inserted {} entries into COMPLETED_COMPACTIONS", updCount);
        try {
          updCount = jdbcResource.getJdbcTemplate().update("DELETE FROM \"COMPACTION_QUEUE\" WHERE \"CQ_ID\" = :id",
              new MapSqlParameterSource().addValue("id", compactionInfo.id));
          if (updCount != 1) {
            LOG.error("Unable to update compaction record: {}. updCnt={}", compactionInfo, updCount);
            return getAbortCompactionResponseElement(compactionInfo.id, "Error",
                "Error while aborting compaction: Unable to update compaction record in COMPACTION_QUEUE");
          } else {
            jdbcResource.getTransactionManager().commit(context);
            return getAbortCompactionResponseElement(compactionInfo.id, "Success",
                "Successfully aborted compaction");
          }
        } catch (DataAccessException e) {
          return getAbortCompactionResponseElement(compactionInfo.id, "Error",
              "Error while aborting compaction:" + e.getMessage());
        }
      }
    }

  }

}
