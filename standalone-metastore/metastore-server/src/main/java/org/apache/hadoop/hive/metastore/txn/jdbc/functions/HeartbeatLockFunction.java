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
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;

/**
 * Heartbeats on the lock table.  This commits, so do not enter it with any state.
 * Should not be called on a lock that belongs to transaction.
 */
public class HeartbeatLockFunction implements TransactionalFunction<Void> {

  private final long extLockId;

  public HeartbeatLockFunction(long extLockId) {
    this.extLockId = extLockId;
  }
  
  @Override
  public Void execute(MultiDataSourceJdbcResource jdbcResource) 
      throws MetaException, NoSuchTxnException, TxnAbortedException, NoSuchLockException {
    // If the lock id is 0, then there are no locks in this heartbeat
    if (extLockId == 0) {
      return null;
    }
    
    int rc = jdbcResource.getJdbcTemplate().update("UPDATE \"HIVE_LOCKS\" SET \"HL_LAST_HEARTBEAT\" = " +
        getEpochFn(jdbcResource.getDatabaseProduct()) + " WHERE \"HL_LOCK_EXT_ID\" = :extLockId",
        new MapSqlParameterSource().addValue("extLockId", extLockId));
    if (rc < 1) {
      throw new NoSuchLockException("No such lock: " + JavaUtils.lockIdToString(extLockId));
    }
    jdbcResource.getTransactionManager().getActiveTransaction().createSavepoint();    
    return null;
  }
  
}
