/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.test.concurrent;

import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.iceberg.hive.HiveTxnCoordinator;
import org.apache.iceberg.mr.hive.HiveIcebergOutputCommitter;

public class HiveIcebergStorageHandlerTxnStub extends HiveIcebergStorageHandlerStub {

  @Override
  public HiveIcebergOutputCommitter getOutputCommitter() {
    HiveTxnManager txnManager = SessionState.get().getTxnMgr();

    boolean isExplicitTxnOpen = txnManager.isTxnOpen() && !txnManager.isImplicitTransactionOpen(null);
    int outputCount = SessionStateUtil.getOutputTableCount(conf)
        .orElse(1);

    if (!isExplicitTxnOpen && outputCount < 2) {
      return super.getOutputCommitter();
    }
    txnManager.getOrSetTxnCoordinator(
        HiveTxnCoordinator.class, msClient -> new HiveTxnCoordinatorStub(conf, msClient, isExplicitTxnOpen));
    return new HiveIcebergOutputCommitter();
  }
}
