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

package org.apache.hadoop.hive.ql.testutil;

import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;

import java.util.Collections;

import static org.apache.hadoop.hive.metastore.txn.TxnHandler.ConfVars;

public final class TxnStoreHelper {

  private final TxnStore txnHandler;

  private TxnStoreHelper(TxnStore txnHandler) {
    this.txnHandler = txnHandler;
  }

  public static TxnStoreHelper wrap(TxnStore txnHandler) {
    return new TxnStoreHelper(txnHandler);
  }

  /**
   * Allocates a new write ID for the table in the given transaction.
   */
  public long allocateTableWriteId(String dbName, String tblName, long txnId)
      throws TxnAbortedException, NoSuchTxnException, MetaException {
    AllocateTableWriteIdsRequest request = new AllocateTableWriteIdsRequest(dbName, tblName.toLowerCase());
    request.setTxnIds(Collections.singletonList(txnId));

    AllocateTableWriteIdsResponse response = txnHandler.allocateTableWriteIds(request);
    return response.getTxnToWriteIds().getFirst().getWriteId();
  }

  /**
   * Registers the min open write ID for the table in the given transaction.
   */
  public void registerMinOpenWriteId(String dbName, String tblName, long txnId) throws MetaException {
    if (!ConfVars.useMinHistoryWriteId()) {
      return;
    }
    long maxWriteId = txnHandler.getMaxAllocatedTableWriteId(
            new MaxAllocatedTableWriteIdRequest(dbName, tblName.toLowerCase()))
        .getMaxWriteId();

    txnHandler.addWriteIdsToMinHistory(txnId,
        Collections.singletonMap(
            TxnUtils.getFullTableName(dbName, tblName), maxWriteId + 1));
  }
}
