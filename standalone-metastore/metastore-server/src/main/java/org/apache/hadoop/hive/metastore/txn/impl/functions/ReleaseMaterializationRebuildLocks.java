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

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.util.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ReleaseMaterializationRebuildLocks implements TransactionalFunction<Long> {
  
  @Override
  public Long execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException {
    try {
      // Aux values
      long cnt = 0L;
      List<Long> txnIds = new ArrayList<>();
      long timeoutTime = Instant.now().toEpochMilli() - timeout;

      Connection dbConn = null;
      Statement stmt = null;
      ResultSet rs = null;
      try {
        lockInternal();
        dbConn = getDbConn(Connection.TRANSACTION_READ_COMMITTED);
        stmt = dbConn.createStatement();

        String selectQ = "SELECT \"MRL_TXN_ID\", \"MRL_LAST_HEARTBEAT\" FROM \"MATERIALIZATION_REBUILD_LOCKS\"";
        LOG.debug("Going to execute query <{}>", selectQ);
        rs = stmt.executeQuery(selectQ);
        while(rs.next()) {
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
        }
        if (!txnIds.isEmpty()) {
          String deleteQ = "DELETE FROM \"MATERIALIZATION_REBUILD_LOCKS\" WHERE" +
              " \"MRL_TXN_ID\" IN(" + StringUtils.join(",", txnIds) + ") ";
          LOG.debug("Going to execute update <{}>", deleteQ);
          cnt = stmt.executeUpdate(deleteQ);
        }
        LOG.debug("Going to commit");
        dbConn.commit();
        return cnt;
      } catch (SQLException e) {
        LOG.debug("Going to rollback: ", e);
        rollbackDBConn(dbConn);
        checkRetryable(e, "cleanupMaterializationRebuildLocks");
        throw new MetaException("Unable to clean rebuild locks due to " +
            StringUtils.stringifyException(e));
      } finally {
        close(rs, stmt, dbConn);
        unlockInternal();
      }
    } catch (RetryException e) {
      return cleanupMaterializationRebuildLocks(validTxnList, timeout);
    }  }
}
