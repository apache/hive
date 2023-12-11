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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.tools.SQLGenerator;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionContext;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryHandler;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.POOL_MUTEX;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRED;

public class TxnStoreMutex implements TxnStore.MutexAPI {

  private static final Logger LOG = LoggerFactory.getLogger(TxnStoreMutex.class);
  /**
   * must be static since even in UT there may be > 1 instance of TxnHandler
   * (e.g. via Compactor services)
   */
  private final static ConcurrentHashMap<String, Semaphore> derbyKey2Lock = new ConcurrentHashMap<>();
  

  private final SQLGenerator sqlGenerator;
  private final MultiDataSourceJdbcResource jdbcResource;

  public TxnStoreMutex(SQLGenerator sqlGenerator, MultiDataSourceJdbcResource jdbcResource) {
    this.sqlGenerator = sqlGenerator;
    this.jdbcResource = jdbcResource;
  }

  @Override
  public LockHandle acquireLock(String key) throws MetaException {
    /**
     * The implementation here is a bit kludgey but done so that code exercised by unit tests
     * (which run against Derby which has no support for select for update) is as similar to
     * production code as possible.
     * In particular, with Derby we always run in a single process with a single metastore and
     * the absence of For Update is handled via a Semaphore.  The later would strictly speaking
     * make the SQL statements below unnecessary (for Derby), but then they would not be tested.
     */
    TransactionContext context = null;
    try {
      jdbcResource.bindDataSource(POOL_MUTEX);
      context = jdbcResource.getTransactionManager().getNewTransaction(PROPAGATION_REQUIRED);
      
      MapSqlParameterSource paramSource = new MapSqlParameterSource().addValue("key", key);
      String sqlStmt = sqlGenerator.addForUpdateClause("SELECT \"MT_COMMENT\", \"MT_KEY2\" FROM \"AUX_TABLE\" WHERE \"MT_KEY1\" = :key");

      LOG.debug("About to execute SQL: {}", sqlStmt);

      Long lastUpdateTime = jdbcResource.getJdbcTemplate().query(sqlStmt, paramSource, rs -> rs.next() ? rs.getLong("MT_KEY2") : null);
      if (lastUpdateTime == null) {
        try {
          jdbcResource.getJdbcTemplate().update("INSERT INTO \"AUX_TABLE\" (\"MT_KEY1\", \"MT_KEY2\") VALUES(:key, 0)", paramSource);
          context.createSavepoint();
        } catch (DataAccessException e) {
          if (!jdbcResource.getDatabaseProduct().isDuplicateKeyError(e)) {
            throw new RuntimeException("Unable to lock " + key + " due to: " + SqlRetryHandler.getMessage(e), e);
          }
          //if here, it means a concrurrent acquireLock() inserted the 'key'

          //rollback is done for the benefit of Postgres which throws (SQLState=25P02, ErrorCode=0) if
          //you attempt any stmt in a txn which had an error.
          try {
            jdbcResource.getConnection().rollback();
          } catch (SQLException ex) {
            throw new MetaException("Unable to lock " + key + " due to: " + SqlRetryHandler.getMessage(ex) + "; " + StringUtils.stringifyException(ex));
          }
        }
        lastUpdateTime = jdbcResource.getJdbcTemplate().query(sqlStmt, paramSource, rs -> rs.next() ? rs.getLong("MT_KEY2") : null);        
        if (lastUpdateTime ==null) {
          throw new IllegalStateException("Unable to lock " + key + ".  Expected row in AUX_TABLE is missing.");
        }
      }
      Semaphore derbySemaphore = null;
      if (jdbcResource.getDatabaseProduct().isDERBY()) {
        derbyKey2Lock.putIfAbsent(key, new Semaphore(1));
        derbySemaphore = derbyKey2Lock.get(key);
        derbySemaphore.acquire();
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} locked by {}", key, JavaUtils.hostname());
      }
      //OK, so now we have a lock
      return new LockHandleImpl(jdbcResource, context, key, lastUpdateTime, derbySemaphore);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      if (context != null) {
        jdbcResource.getTransactionManager().rollback(context);
      }
      throw new MetaException("Unable to lock " + key + " due to: " + ex.getMessage() + StringUtils.stringifyException(ex));
    } catch (Throwable e) {
      if (context != null) {
        jdbcResource.getTransactionManager().rollback(context);
      }
      throw e;
    } finally {
      jdbcResource.unbindDataSource();
    }
  }

  @Override
  public void acquireLock(String key, LockHandle handle) throws MetaException {
    //the idea is that this will use LockHandle.dbConn
    throw new NotImplementedException("acquireLock(String, LockHandle) is not implemented");
  }

  public static final class LockHandleImpl implements LockHandle {
  
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
}
