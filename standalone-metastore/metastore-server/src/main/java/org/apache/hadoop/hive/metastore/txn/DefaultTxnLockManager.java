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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.txn.entities.LockInfo;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.CheckLockFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.functions.EnqueueLockFunction;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetLocksByLockId;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.ShowLocksHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.RollbackException;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.Types;
import java.util.List;

import static org.apache.hadoop.hive.metastore.txn.entities.LockInfo.LOCK_WAITING;

public class DefaultTxnLockManager implements TxnLockManager {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultTxnLockManager.class);

  private final MultiDataSourceJdbcResource jdbcResource;

  public DefaultTxnLockManager(MultiDataSourceJdbcResource jdbcResource) {
    this.jdbcResource = jdbcResource;
  }

  @Override
  public long enqueueLock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException {
    return new EnqueueLockFunction(rqst).execute(jdbcResource);
  }

  @Override
  public LockResponse checkLock(long extLockId, long txnId, boolean zeroWaitReadEnabled, boolean isExclusiveCTAS) 
      throws NoSuchTxnException, NoSuchLockException, TxnAbortedException, MetaException {
    return new CheckLockFunction(extLockId, txnId, zeroWaitReadEnabled, isExclusiveCTAS).execute(jdbcResource);
  }

  @Override
  public void unlock(UnlockRequest rqst) throws TxnOpenException, MetaException {
    long extLockId = rqst.getLockid();
    /**
     * This method is logically like commit for read-only auto commit queries.
     * READ_COMMITTED since this only has 1 delete statement and no new entries with the
     * same hl_lock_ext_id can be added, i.e. all rows with a given hl_lock_ext_id are
     * created in a single atomic operation.
     * Theoretically, this competes with {@link #lock(org.apache.hadoop.hive.metastore.api.LockRequest)}
     * but hl_lock_ext_id is not known until that method returns.
     * Also competes with {@link #checkLock(org.apache.hadoop.hive.metastore.api.CheckLockRequest)}
     * but using SERIALIZABLE doesn't materially change the interaction.
     * If "delete" stmt misses, additional logic is best effort to produce meaningful error msg.
     */
    //hl_txnid <> 0 means it's associated with a transaction
    int rc = jdbcResource.getJdbcTemplate().update("DELETE FROM \"HIVE_LOCKS\" WHERE \"HL_LOCK_EXT_ID\" = :extLockId " +
            " AND (\"HL_TXNID\" = 0 OR (\"HL_TXNID\" <> 0 AND \"HL_LOCK_STATE\" = :state))", 
        new MapSqlParameterSource()
            .addValue("extLockId", extLockId)
            .addValue("state", Character.toString(LOCK_WAITING), Types.CHAR));
    //(hl_txnid <> 0 AND hl_lock_state = '" + LOCK_WAITING + "') is for multi-statement txns where
    //some query attempted to lock (thus LOCK_WAITING state) but is giving up due to timeout for example
    if (rc < 1) {
      LOG.info("Failure to unlock any locks with extLockId={}.", extLockId);
      List<LockInfo> lockInfos = jdbcResource.execute(new GetLocksByLockId(extLockId, 1, jdbcResource.getSqlGenerator()));
      if (CollectionUtils.isEmpty(lockInfos)) {
        //didn't find any lock with extLockId but at ReadCommitted there is a possibility that
        //it existed when above delete ran but it didn't have the expected state.
        LOG.info("No lock in {} mode found for unlock({})", LOCK_WAITING,
            JavaUtils.lockIdToString(rqst.getLockid()));
        
        //bail here to make the operation idempotent
        throw new RollbackException(null);
      }
      LockInfo lockInfo = lockInfos.get(0);
      if (TxnUtils.isValidTxn(lockInfo.getTxnId())) {
        String msg = "Unlocking locks associated with transaction not permitted.  " + lockInfo;
        //if a lock is associated with a txn we can only "unlock" it if it's in WAITING state
        // which really means that the caller wants to give up waiting for the lock
        LOG.error(msg);
        throw new TxnOpenException(msg);
      } else {
        //we didn't see this lock when running DELETE stmt above but now it showed up
        //so should "should never happen" happened...
        String msg = "Found lock in unexpected state " + lockInfo;
        LOG.error(msg);
        throw new MetaException(msg);
      }
    }
    LOG.debug("Successfully unlocked at least 1 lock with extLockId={}", extLockId);
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException {
    return jdbcResource.execute(new ShowLocksHandler(rqst));
  }
  
}
