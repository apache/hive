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
package org.apache.hadoop.hive.metastore.txn.entities;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.LockTypeUtil;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LockInfo {

  public static final char LOCK_ACQUIRED = 'a';
  public static final char LOCK_WAITING = 'w';
  

  private final long extLockId;
  private final long intLockId;
  //0 means there is no transaction, i.e. it a select statement which is not part of
  //explicit transaction or a IUD statement that is not writing to ACID table
  private final long txnId;
  private final String db;
  private final String table;
  private final String partition;
  private final LockState state;
  private final LockType type;

  // Assumes the result set is set to a valid row
  public LockInfo(ResultSet rs) throws SQLException, MetaException {
    extLockId = rs.getLong("HL_LOCK_EXT_ID"); // can't be null
    intLockId = rs.getLong("HL_LOCK_INT_ID"); // can't be null
    db = rs.getString("HL_DB"); // can't be null
    String t = rs.getString("HL_TABLE");
    table = (rs.wasNull() ? null : t);
    String p = rs.getString("HL_PARTITION");
    partition = (rs.wasNull() ? null : p);
    switch (rs.getString("HL_LOCK_STATE").charAt(0)) {
      case LOCK_WAITING: state = LockState.WAITING; break;
      case LOCK_ACQUIRED: state = LockState.ACQUIRED; break;
      default:
        throw new MetaException("Unknown lock state " + rs.getString("HL_LOCK_STATE").charAt(0));
    }
    char lockChar = rs.getString("HL_LOCK_TYPE").charAt(0);
    type = LockTypeUtil.getLockTypeFromEncoding(lockChar)
        .orElseThrow(() -> new MetaException("Unknown lock type: " + lockChar));
    txnId = rs.getLong("HL_TXNID"); //returns 0 if value is NULL
  }

  public LockInfo(ShowLocksResponseElement e) {
    extLockId = e.getLockid();
    intLockId = e.getLockIdInternal();
    txnId = e.getTxnid();
    db = e.getDbname();
    table = e.getTablename();
    partition = e.getPartname();
    state = e.getState();
    type = e.getType();
  }

  public long getExtLockId() {
    return extLockId;
  }

  public long getIntLockId() {
    return intLockId;
  }

  public long getTxnId() {
    return txnId;
  }

  public String getDb() {
    return db;
  }

  public String getTable() {
    return table;
  }

  public String getPartition() {
    return partition;
  }

  public LockState getState() {
    return state;
  }

  public LockType getType() {
    return type;
  }

  public boolean equals(Object other) {
    if (!(other instanceof LockInfo)) return false;
    LockInfo o = (LockInfo)other;
    // Lock ids are unique across the system.
    return extLockId == o.extLockId && intLockId == o.intLockId;
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder()
        .append(intLockId)
        .append(extLockId)
        .append(txnId)
        .append(db)
        .build();
  }

  @Override
  public String toString() {
    return JavaUtils.lockIdToString(extLockId) + " intLockId:" +
        intLockId + " " + JavaUtils.txnIdToString(txnId)
        + " db:" + db + " table:" + table + " partition:" +
        partition + " state:" + (state == null ? "null" : state.toString())
        + " type:" + (type == null ? "null" : type.toString());
  }
  private boolean isDbLock() {
    return db != null && table == null && partition == null;
  }
  private boolean isTableLock() {
    return db != null && table != null && partition == null;
  }
  private boolean isPartitionLock() {
    return !(isDbLock() || isTableLock());
  }
  
}
