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

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ReleaseMaterializationRebuildLocks implements TransactionalFunction<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(ReleaseMaterializationRebuildLocks.class);

  private final ValidTxnList validTxnList;
  private final long timeout;

  public ReleaseMaterializationRebuildLocks(ValidTxnList validTxnList, long timeout) {
    this.validTxnList = validTxnList;
    this.timeout = timeout;
  }

  @Override
  public Long execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    // Aux values
    long cnt = 0L;
    List<Long> txnIds = new ArrayList<>();
    long timeoutTime = Instant.now().toEpochMilli() - timeout;


    String selectQ = "SELECT \"MRL_TXN_ID\", \"MRL_LAST_HEARTBEAT\" FROM \"MATERIALIZATION_REBUILD_LOCKS\"";
    LOG.debug("Going to execute query <{}>", selectQ);

    jdbcResource.getJdbcTemplate().query(selectQ, rs -> {
      long lastHeartbeat = rs.getLong(2);
      if (lastHeartbeat < timeoutTime) {
        // The heartbeat has timeout, double check whether we can remove it
        long txnId = rs.getLong(1);
        if (validTxnList.isTxnValid(txnId) || validTxnList.isTxnAborted(txnId)) {
          // Txn was committed (but notification was not received) or it was aborted.
          // Either case, we can clean it up
          txnIds.add(txnId);
        }
      }
      return null;
    });

    if (!txnIds.isEmpty()) {
      String deleteQ = "DELETE FROM \"MATERIALIZATION_REBUILD_LOCKS\" WHERE \"MRL_TXN_ID\" IN(:txnIds)";
      LOG.debug("Going to execute update <{}>", deleteQ);
      cnt = jdbcResource.getJdbcTemplate().update(deleteQ, new MapSqlParameterSource().addValue("txnIds", txnIds));
    }
    LOG.debug("Going to commit");
    return cnt;
  }

}