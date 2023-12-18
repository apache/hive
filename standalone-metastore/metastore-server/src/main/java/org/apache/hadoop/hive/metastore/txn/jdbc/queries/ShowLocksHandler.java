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
package org.apache.hadoop.hive.metastore.txn.jdbc.queries;

import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.LockTypeComparator;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.txn.entities.LockInfo;
import org.apache.hadoop.hive.metastore.txn.jdbc.QueryHandler;
import org.apache.hadoop.hive.metastore.utils.LockTypeUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.apache.hadoop.hive.metastore.txn.entities.LockInfo.LOCK_ACQUIRED;
import static org.apache.hadoop.hive.metastore.txn.entities.LockInfo.LOCK_WAITING;

public class ShowLocksHandler implements QueryHandler<ShowLocksResponse> {

  private final ShowLocksRequest request;

  public ShowLocksHandler(ShowLocksRequest request) {
    this.request = request;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) throws MetaException {
    return 
        "SELECT \"HL_LOCK_EXT_ID\", \"HL_TXNID\", \"HL_DB\", \"HL_TABLE\", \"HL_PARTITION\", \"HL_LOCK_STATE\", " +
        "\"HL_LOCK_TYPE\", \"HL_LAST_HEARTBEAT\", \"HL_ACQUIRED_AT\", \"HL_USER\", \"HL_HOST\", \"HL_LOCK_INT_ID\"," +
        "\"HL_BLOCKEDBY_EXT_ID\", \"HL_BLOCKEDBY_INT_ID\", \"HL_AGENT_INFO\" FROM \"HIVE_LOCKS\"" +
        "WHERE " +
            "(\"HL_DB\" = :dbName OR :dbName IS NULL) AND " +
            "(\"HL_TABLE\" = :tableName OR :tableName IS NULL) AND " +
            "(\"HL_PARTITION\" = :partition OR :partition IS NULL) AND " +
            "(\"HL_TXNID\" = :txnId OR :txnId IS NULL)";
  }

  @Override
  public SqlParameterSource getQueryParameters() {
    return new MapSqlParameterSource()
        .addValue("dbName", request.getDbname(), Types.VARCHAR)
        .addValue("tableName", request.getTablename(), Types.VARCHAR)
        .addValue("partition", request.getPartname(), Types.VARCHAR)
        .addValue("txnId", request.isSetTxnid() ? request.getTxnid() : null, Types.BIGINT);
  }

  @Override
  public ShowLocksResponse extractData(ResultSet rs) throws SQLException, DataAccessException {
    ShowLocksResponse rsp = new ShowLocksResponse();
    List<ShowLocksResponseElement> elems = new ArrayList<>();
    List<LockInfoExt> sortedList = new ArrayList<>();
    while (rs.next()) {
      ShowLocksResponseElement e = new ShowLocksResponseElement();
      e.setLockid(rs.getLong(1));
      long txnid = rs.getLong(2);
      if (!rs.wasNull()) e.setTxnid(txnid);
      e.setDbname(rs.getString(3));
      e.setTablename(rs.getString(4));
      String partition = rs.getString(5);
      if (partition != null) e.setPartname(partition);
      switch (rs.getString(6).charAt(0)) {
        case LOCK_ACQUIRED:
          e.setState(LockState.ACQUIRED);
          break;
        case LOCK_WAITING:
          e.setState(LockState.WAITING);
          break;
        default:
          throw new SQLException("Unknown lock state " + rs.getString(6).charAt(0));
      }

      char lockChar = rs.getString(7).charAt(0);
      LockType lockType = LockTypeUtil.getLockTypeFromEncoding(lockChar)
          .orElseThrow(() -> new SQLException("Unknown lock type: " + lockChar));
      e.setType(lockType);

      e.setLastheartbeat(rs.getLong(8));
      long acquiredAt = rs.getLong(9);
      if (!rs.wasNull()) e.setAcquiredat(acquiredAt);
      e.setUser(rs.getString(10));
      e.setHostname(rs.getString(11));
      e.setLockIdInternal(rs.getLong(12));
      long id = rs.getLong(13);
      if (!rs.wasNull()) {
        e.setBlockedByExtId(id);
      }
      id = rs.getLong(14);
      if (!rs.wasNull()) {
        e.setBlockedByIntId(id);
      }
      e.setAgentInfo(rs.getString(15));
      sortedList.add(new LockInfoExt(e));
    }
    //this ensures that "SHOW LOCKS" prints the locks in the same order as they are examined
    //by checkLock() - makes diagnostics easier.
    Collections.sort(sortedList, new LockInfoComparator());
    for(LockInfoExt lockInfoExt : sortedList) {
      elems.add(lockInfoExt.e);
    }
    rsp.setLocks(elems);
    return rsp;    
  }

  /**
   * used to sort entries in {@link org.apache.hadoop.hive.metastore.api.ShowLocksResponse}
   */
  private static class LockInfoExt extends LockInfo {
    private final ShowLocksResponseElement e;
    LockInfoExt(ShowLocksResponseElement e) {
      super(e);
      this.e = e;
    }
  }

  private static class LockInfoComparator implements Comparator<LockInfo>, Serializable {
    private final LockTypeComparator lockTypeComparator = new LockTypeComparator();

    public int compare(LockInfo info1, LockInfo info2) {
      // We sort by state (acquired vs waiting) and then by LockType, then by id
      if (info1.getState() == LockState.ACQUIRED &&
          info2.getState() != LockState .ACQUIRED) {
        return -1;
      }
      if (info1.getState() != LockState.ACQUIRED &&
          info2.getState() == LockState .ACQUIRED) {
        return 1;
      }

      int sortByType = lockTypeComparator.compare(info1.getType(), info2.getType());
      if(sortByType != 0) {
        return sortByType;
      }
      if (info1.getExtLockId() < info2.getExtLockId()) {
        return -1;
      } else if (info1.getExtLockId() > info2.getExtLockId()) {
        return 1;
      } else {
        if (info1.getIntLockId() < info2.getIntLockId()) {
          return -1;
        } else if (info1.getTxnId() > info2.getIntLockId()) {
          return 1;
        } else {
          return 0;
        }
      }
    }
  }  
  
}
