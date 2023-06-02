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
package org.apache.hadoop.hive.metastore.txn.impl.commands;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.DatabaseProduct;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.txn.MetaWrapperException;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedBatchCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.ParameterizedCommand;
import org.apache.hadoop.hive.metastore.utils.LockTypeUtil;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.hadoop.hive.metastore.txn.TxnUtils.getEpochFn;
import static org.apache.hadoop.hive.metastore.txn.entities.LockInfo.LOCK_WAITING;

public class InsertHiveLocksCommand implements ParameterizedBatchCommand {
  
  private final LockRequest lockRequest;
  private final long tempExtLockId;

  public InsertHiveLocksCommand(LockRequest lockRequest, long tempExtLockId) {
    this.lockRequest = lockRequest;
    this.tempExtLockId = tempExtLockId;
  }

  @Override
  public String getParameterizedQueryString(DatabaseProduct databaseProduct) {
    try {
      //language=SQL
      return String.format( 
          "INSERT INTO \"HIVE_LOCKS\" ( " +
          "\"HL_LOCK_EXT_ID\", \"HL_LOCK_INT_ID\", \"HL_TXNID\", \"HL_DB\", \"HL_TABLE\", \"HL_PARTITION\", " +
          "\"HL_LOCK_STATE\", \"HL_LOCK_TYPE\", \"HL_LAST_HEARTBEAT\", \"HL_USER\", \"HL_HOST\", \"HL_AGENT_INFO\") " +
          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, %s, ?, ?, ?)", lockRequest.getTxnid() != 0 ? "0" : getEpochFn(databaseProduct));
    } catch (MetaException e) {
      throw new MetaWrapperException(e);
    }
  }

  @Override
  public List<Object[]> getQueryParameters() {
    List<Object[]> params = new ArrayList<>(lockRequest.getComponentSize());
    long intLockId = 0;
    for (LockComponent lc : lockRequest.getComponent()) {
      String lockType = LockTypeUtil.getEncodingAsStr(lc.getType());
      params.add(new Object[] {tempExtLockId, ++intLockId, lockRequest.getTxnid(), StringUtils.lowerCase(lc.getDbname()),
          StringUtils.lowerCase(lc.getTablename()), TxnUtils.normalizePartitionCase(lc.getPartitionname()),
          Character.toString(LOCK_WAITING), lockType, lockRequest.getUser(), lockRequest.getHostname(), lockRequest.getAgentInfo()});
    }
    return params;
  }

  @Override
  public int[] getParameterTypes() {
    return new int[]{ Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
        Types.CHAR, Types.CHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR};
  }

  @Override
  public Function<Integer, Boolean> resultPolicy() {
    return ParameterizedCommand.EXACTLY_ONE_ROW;
  }
}
