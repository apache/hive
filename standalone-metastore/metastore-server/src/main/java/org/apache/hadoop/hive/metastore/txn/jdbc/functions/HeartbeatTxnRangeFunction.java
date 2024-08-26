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

import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionContext;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

public class HeartbeatTxnRangeFunction implements TransactionalFunction<HeartbeatTxnRangeResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatTxnRangeFunction.class);

  private final HeartbeatTxnRangeRequest rqst;

  public HeartbeatTxnRangeFunction(HeartbeatTxnRangeRequest rqst) {
    this.rqst = rqst;
  }

  @Override
  public HeartbeatTxnRangeResponse execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    HeartbeatTxnRangeResponse rsp = new HeartbeatTxnRangeResponse();
    Set<Long> nosuch = new HashSet<>();
    Set<Long> aborted = new HashSet<>();
    rsp.setNosuch(nosuch);
    rsp.setAborted(aborted);
    /**
     * READ_COMMITTED is sufficient since {@link #heartbeatTxn(java.sql.Connection, long)}
     * only has 1 update statement in it and
     * we only update existing txns, i.e. nothing can add additional txns that this operation
     * would care about (which would have required SERIALIZABLE)
     */
    /*do fast path first (in 1 statement) if doesn't work, rollback and do the long version*/
    List<String> queries = new ArrayList<>();
    int numTxnsToHeartbeat = (int) (rqst.getMax() - rqst.getMin() + 1);
    List<Long> txnIds = new ArrayList<>(numTxnsToHeartbeat);
    for (long txn = rqst.getMin(); txn <= rqst.getMax(); txn++) {
      txnIds.add(txn);
    }
    TransactionContext context = jdbcResource.getTransactionManager().getActiveTransaction();
    Object savePoint = context.createSavepoint();
    TxnUtils.buildQueryWithINClause(jdbcResource.getConf(), queries,
        new StringBuilder("UPDATE \"TXNS\" SET \"TXN_LAST_HEARTBEAT\" = " + getEpochFn(jdbcResource.getDatabaseProduct()) +
            " WHERE \"TXN_STATE\" = " + TxnStatus.OPEN + " AND "),
        new StringBuilder(""), txnIds, "\"TXN_ID\"", true, false);
    int updateCnt = 0;
    for (String query : queries) {
      LOG.debug("Going to execute update <{}>", query);
      updateCnt += jdbcResource.getJdbcTemplate().update(query, new MapSqlParameterSource());
    }
    if (updateCnt == numTxnsToHeartbeat) {
      //fast pass worked, i.e. all txns we were asked to heartbeat were Open as expected
      context.createSavepoint();
      return rsp;
    }
    //if here, do the slow path so that we can return info txns which were not in expected state
    context.rollbackToSavepoint(savePoint);
    for (long txn = rqst.getMin(); txn <= rqst.getMax(); txn++) {
      try {
        new HeartbeatTxnFunction(txn).execute(jdbcResource);
      } catch (NoSuchTxnException e) {
        nosuch.add(txn);
      } catch (TxnAbortedException e) {
        aborted.add(txn);
      } catch (NoSuchLockException e) {
        throw new RuntimeException(e);
      }
    }
    return rsp;
  }

}
