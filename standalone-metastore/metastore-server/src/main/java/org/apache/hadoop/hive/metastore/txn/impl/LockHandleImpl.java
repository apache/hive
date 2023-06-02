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
package org.apache.hadoop.hive.metastore.txn.impl;

import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionContext;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.util.concurrent.Semaphore;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.POOL_MUTEX;

public final class LockHandleImpl implements TxnStore.MutexAPI.LockHandle {

  private static final Logger LOG = LoggerFactory.getLogger(LockHandleImpl.class);
  private static final String HOSTNAME = JavaUtils.hostname();

  private final MultiDataSourceJdbcResource jdbcResource;
  private final TransactionContext context;
  private final Semaphore derbySemaphore;
  private final String key;
  private final Long lastUpdateTime;

  public LockHandleImpl(MultiDataSourceJdbcResource jdbcResource, TransactionContext context,  String key, 
                        Long lastUpdateTime, Semaphore derbySemaphore) {
    assert derbySemaphore == null || derbySemaphore.availablePermits() == 0 : "Expected locked Semaphore";
    
    this.jdbcResource = jdbcResource;
    this.context = context;
    this.derbySemaphore = derbySemaphore;
    this.key = key;
    this.lastUpdateTime = lastUpdateTime == null ? -1L : lastUpdateTime;
  }

  @Override
  public void releaseLocks() {
    try {
      jdbcResource.bindDataSource(POOL_MUTEX);
      jdbcResource.getTransactionManager().rollback(context);
      if (derbySemaphore != null) {
        derbySemaphore.release();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} unlocked by {}", key, HOSTNAME);
      }
    } finally {
      jdbcResource.unbindDataSource();
    }
  }

  @Override
  public Long getLastUpdateTime() {
    return lastUpdateTime;
  }

  @Override
  public void releaseLocks(Long timestamp) {
    try {
      jdbcResource.bindDataSource(POOL_MUTEX);
      try {
        jdbcResource.getJdbcTemplate().update("UPDATE \"AUX_TABLE\" SET \"MT_KEY2\" = :time WHERE \"MT_KEY1\"= :key",
            new MapSqlParameterSource()
                .addValue("time", timestamp)
                .addValue("key", key));
        jdbcResource.getTransactionManager().commit(context);
      } catch (DataAccessException ex) {
        LOG.warn("Unable to update MT_KEY2 value for MT_KEY1=" + key, ex);
      }
      
      if (derbySemaphore != null) {
        derbySemaphore.release();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} unlocked by {}", key, HOSTNAME);
      }
    } finally {
      jdbcResource.unbindDataSource();
    }
  }

  @Override
  public void close() {
    releaseLocks();
  }

}
