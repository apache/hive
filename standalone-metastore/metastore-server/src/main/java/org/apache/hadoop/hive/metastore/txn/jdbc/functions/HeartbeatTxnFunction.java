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
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.Types;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

/**
 * Heartbeats on the txn table. This commits, so do not enter it with any state. 
 */
public class HeartbeatTxnFunction implements TransactionalFunction<Void> {

  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatTxnFunction.class);

  private final long txnId;

  public HeartbeatTxnFunction(long txnId) {
    this.txnId = txnId;
  }

  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException, NoSuchTxnException, TxnAbortedException, NoSuchLockException {
    // If the txnid is 0, then there are no transactions in this heartbeat
    if (txnId == 0) {
      return null;
    }
    
    int rc = jdbcResource.getJdbcTemplate().update(
        "UPDATE \"TXNS\" SET \"TXN_LAST_HEARTBEAT\" = " + getEpochFn(jdbcResource.getDatabaseProduct()) +
        " WHERE \"TXN_ID\" = :txnId AND \"TXN_STATE\" = :state", 
        new MapSqlParameterSource()
            .addValue("txnId", txnId)
            .addValue("state", TxnStatus.OPEN.getSqlConst(), Types.CHAR));
    
    if (rc < 1) {
      new EnsureValidTxnFunction(txnId).execute(jdbcResource); // This should now throw some useful exception.
      LOG.error("Can neither heartbeat txn (txnId={}) nor confirm it as invalid.", txnId);
      throw new NoSuchTxnException("No such txn: " + txnId);
    }
    
    LOG.debug("Successfully heartbeated for txnId={}", txnId);
    jdbcResource.getTransactionManager().getActiveTransaction().createSavepoint();
    return null;
  }

}
