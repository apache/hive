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

import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.txn.TxnLockManager;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.InsertHiveLocksCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.InsertTxnComponentsCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetOpenTxnTypeAndLockHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetWriteIdsHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.Objects;

public class EnqueueLockFunction implements TransactionalFunction<Long> {

  private static final Logger LOG = LoggerFactory.getLogger(EnqueueLockFunction.class);  

  private static final String INCREMENT_NEXT_LOCK_ID_QUERY = "UPDATE \"NEXT_LOCK_ID\" SET \"NL_NEXT\" = %s";
  private static final String UPDATE_HIVE_LOCKS_EXT_ID_QUERY = "UPDATE \"HIVE_LOCKS\" SET \"HL_LOCK_EXT_ID\" = %s " +
      "WHERE \"HL_LOCK_EXT_ID\" = %s";

  private final LockRequest lockRequest;

  public EnqueueLockFunction(LockRequest lockRequest) {
    this.lockRequest = lockRequest;
  }

  /**
   * This enters locks into the queue in {@link org.apache.hadoop.hive.metastore.txn.entities.LockInfo#LOCK_WAITING} mode.
   * Isolation Level Notes:
   * 1. We use S4U (withe read_committed) to generate the next (ext) lock id.  This serializes
   * any 2 {@code enqueueLockWithRetry()} calls.
   * 2. We use S4U on the relevant TXNS row to block any concurrent abort/commit/etc. operations
   * @see TxnLockManager#checkLock(long, long, boolean, boolean)
   */
  @Override
  public Long execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException, TxnAbortedException, NoSuchTxnException {
    long txnid = lockRequest.getTxnid();
    if (TxnUtils.isValidTxn(txnid)) {
      //this also ensures that txn is still there in expected state
      TxnType txnType = jdbcResource.execute(new GetOpenTxnTypeAndLockHandler(jdbcResource.getSqlGenerator(), txnid));
      if (txnType == null) {
        new EnsureValidTxnFunction(txnid).execute(jdbcResource);
      }
    }
        /* Insert txn components and hive locks (with a temp extLockId) first, before getting the next lock ID in a select-for-update.
           This should minimize the scope of the S4U and decrease the table lock duration. */
    if (txnid > 0) {
      jdbcResource.execute(new InsertTxnComponentsCommand(lockRequest, jdbcResource.execute(new GetWriteIdsHandler(lockRequest))));
    }
    long tempExtLockId = TxnUtils.generateTemporaryId();
    jdbcResource.execute(new InsertHiveLocksCommand(lockRequest, tempExtLockId));

    /* Get the next lock id.
     * This has to be atomic with adding entries to HIVE_LOCK entries (1st add in W state) to prevent a race.
     * Suppose ID gen is a separate txn and 2 concurrent lock() methods are running.  1st one generates nl_next=7,
     * 2nd nl_next=8.  Then 8 goes first to insert into HIVE_LOCKS and acquires the locks.  Then 7 unblocks,
     * and add it's W locks, but it won't see locks from 8 since to be 'fair' {@link #checkLock(java.sql.Connection, long)}
     * doesn't block on locks acquired later than one it's checking*/
    long extLockId = getNextLockIdForUpdate(jdbcResource);
    incrementLockIdAndUpdateHiveLocks(jdbcResource.getJdbcTemplate().getJdbcTemplate(), extLockId, tempExtLockId);

    jdbcResource.getTransactionManager().getActiveTransaction().createSavepoint();

    return extLockId;
  }

  private long getNextLockIdForUpdate(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    String s = jdbcResource.getSqlGenerator().addForUpdateClause("SELECT \"NL_NEXT\" FROM \"NEXT_LOCK_ID\"");
    LOG.debug("Going to execute query <{}>", s);
    
    try {
      return Objects.requireNonNull(
          jdbcResource.getJdbcTemplate().queryForObject(s, new MapSqlParameterSource(), Long.class),
          "This never should be null, it's just to suppress warnings");
    } catch (EmptyResultDataAccessException e) {
      LOG.error("Failure to get next lock ID for update! SELECT query returned empty ResultSet.");
      throw new MetaException("Transaction tables not properly " +
          "initialized, no record found in next_lock_id");      
    }
  }

  private void incrementLockIdAndUpdateHiveLocks(JdbcTemplate jdbcTemplate, long extLockId, long tempId) {    
    String incrCmd = String.format(INCREMENT_NEXT_LOCK_ID_QUERY, (extLockId + 1));
    // update hive locks entries with the real EXT_LOCK_ID (replace temp ID)
    String updateLocksCmd = String.format(UPDATE_HIVE_LOCKS_EXT_ID_QUERY, extLockId, tempId);
    LOG.debug("Going to execute updates in batch: <{}>, and <{}>", incrCmd, updateLocksCmd);
    jdbcTemplate.batchUpdate(incrCmd, updateLocksCmd);
  }
  
}
