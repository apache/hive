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
package org.apache.hadoop.hive.metastore.txn.impl.functions;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.util.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

public class LockMaterializationRebuildFunction implements TransactionalFunction<LockResponse> {
  
  @Override
  public LockResponse execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Acquiring lock for materialization rebuild with {} for {}",
          JavaUtils.txnIdToString(txnId), TableName.getDbTable(dbName, tableName));
    }

    TxnStore.MutexAPI.LockHandle handle = null;
    Connection dbConn = null;
    PreparedStatement pst = null;
    ResultSet rs = null;
    try {
      lockInternal();
      /**
       * MUTEX_KEY.MaterializationRebuild lock ensures that there is only 1 entry in
       * Initiated/Working state for any resource. This ensures we do not run concurrent
       * rebuild operations on any materialization.
       */
      handle = getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.MaterializationRebuild.name());
      dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);

      List<String> params = Arrays.asList(dbName, tableName);
      String selectQ = "SELECT \"MRL_TXN_ID\" FROM \"MATERIALIZATION_REBUILD_LOCKS\" WHERE" +
          " \"MRL_DB_NAME\" = ? AND \"MRL_TBL_NAME\" = ?";
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, selectQ, params);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute query <" + selectQ.replace("?", "{}") + ">",
            quoteString(dbName), quoteString(tableName));
      }
      rs = pst.executeQuery();
      if(rs.next()) {
        LOG.info("Ignoring request to rebuild {}/{} since it is already being rebuilt", dbName, tableName);
        return new LockResponse(txnId, LockState.NOT_ACQUIRED);
      }
      String insertQ = "INSERT INTO \"MATERIALIZATION_REBUILD_LOCKS\" " +
          "(\"MRL_TXN_ID\", \"MRL_DB_NAME\", \"MRL_TBL_NAME\", \"MRL_LAST_HEARTBEAT\") VALUES (" + txnId +
          ", ?, ?, " + Instant.now().toEpochMilli() + ")";
      closeStmt(pst);
      pst = sqlGenerator.prepareStmtWithParameters(dbConn, insertQ, params);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Going to execute update <" + insertQ.replace("?", "{}") + ">",
            quoteString(dbName), quoteString(tableName));
      }
      pst.executeUpdate();
      LOG.debug("Going to commit");
      dbConn.commit();
      return new LockResponse(txnId, LockState.ACQUIRED);
    } catch (SQLException ex) {
      LOG.warn("lockMaterializationRebuild failed due to " + getMessage(ex), ex);
      throw new MetaException("Unable to retrieve materialization invalidation information due to " +
          StringUtils.stringifyException(ex));
    } finally {
      close(rs, pst, dbConn);
      if(handle != null) {
        handle.releaseLocks();
      }
      unlockInternal();
    }
  }
}
