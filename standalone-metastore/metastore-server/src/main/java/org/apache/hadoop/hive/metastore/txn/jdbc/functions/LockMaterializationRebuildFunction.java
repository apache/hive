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

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;

import java.sql.ResultSet;
import java.time.Instant;

public class LockMaterializationRebuildFunction implements TransactionalFunction<LockResponse> {

  private static final Logger LOG = LoggerFactory.getLogger(LockMaterializationRebuildFunction.class);

  private final String dbName;
  private final String tableName;
  private final long txnId;
  private final TxnStore.MutexAPI mutexAPI;

  public LockMaterializationRebuildFunction(String dbName, String tableName, long txnId, TxnStore.MutexAPI mutexAPI) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.txnId = txnId;
    this.mutexAPI = mutexAPI;
  }

  @Override
  public LockResponse execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Acquiring lock for materialization rebuild with {} for {}",
          JavaUtils.txnIdToString(txnId), TableName.getDbTable(dbName, tableName));
    }

    /**
     * MUTEX_KEY.MaterializationRebuild lock ensures that there is only 1 entry in
     * Initiated/Working state for any resource. This ensures we do not run concurrent
     * rebuild operations on any materialization.
     */
    try (TxnStore.MutexAPI.LockHandle ignored = mutexAPI.acquireLock(TxnStore.MUTEX_KEY.MaterializationRebuild.name())){
      MapSqlParameterSource params = new MapSqlParameterSource()
          .addValue("dbName", dbName)
          .addValue("tableName", tableName);

      String selectQ = "SELECT \"MRL_TXN_ID\" FROM \"MATERIALIZATION_REBUILD_LOCKS\" WHERE" +
          " \"MRL_DB_NAME\" = :dbName AND \"MRL_TBL_NAME\" = :tableName";
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query {}", selectQ);
      }
      boolean found = Boolean.TRUE.equals(jdbcResource.getJdbcTemplate().query(selectQ, params, ResultSet::next));
      
      if(found) {
        LOG.info("Ignoring request to rebuild {}/{} since it is already being rebuilt", dbName, tableName);
        return new LockResponse(txnId, LockState.NOT_ACQUIRED);
      }
      
      String insertQ = "INSERT INTO \"MATERIALIZATION_REBUILD_LOCKS\" " +
          "(\"MRL_TXN_ID\", \"MRL_DB_NAME\", \"MRL_TBL_NAME\", \"MRL_LAST_HEARTBEAT\") " +
          "VALUES (:txnId, :dbName, :tableName, " + Instant.now().toEpochMilli() + ")";
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute update {}", insertQ);
      }
      jdbcResource.getJdbcTemplate().update(insertQ, params.addValue("txnId", txnId));
      return new LockResponse(txnId, LockState.ACQUIRED);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}