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

package org.apache.hadoop.hive.ql.lockmgr;

import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.TxnCoordinator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hive ACID transaction coordinator.
 */
public class AcidTxnCoordinator implements TxnCoordinator {

  static final private Logger LOG = LoggerFactory.getLogger(AcidTxnCoordinator.class);

  private final HiveConf conf;
  private final IMetaStoreClient msClient;
  private final String replPolicy;
  private final long txnId;

  public AcidTxnCoordinator(
      HiveConf conf, IMetaStoreClient msClient, String replPolicy, long txnId) {
    this.conf = conf;
    this.msClient = msClient;
    this.replPolicy = replPolicy;
    this.txnId = txnId;
  }

  @Override
  public void commit() throws TException {
    LOG.debug("Committing txn {}", JavaUtils.txnIdToString(txnId));
    CommitTxnRequest commitTxnRequest = new CommitTxnRequest(txnId);
    commitTxnRequest.setExclWriteEnabled(conf.getBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK));
    if (replPolicy != null) {
      commitTxnRequest.setReplPolicy(replPolicy);
      commitTxnRequest.setTxn_type(TxnType.DEFAULT);
    }
    msClient.commitTxn(commitTxnRequest);
  }

  @Override
  public void rollback() throws TException {
    LOG.debug("Rolling back {}", JavaUtils.txnIdToString(txnId));
    if (replPolicy != null) {
      msClient.replRollbackTxn(txnId, replPolicy, TxnType.DEFAULT);
    } else {
      AbortTxnRequest abortTxnRequest = new AbortTxnRequest(txnId);
      abortTxnRequest.setErrorCode(TxnErrorMsg.ABORT_ROLLBACK.getErrorCode());
      msClient.rollbackTxn(abortTxnRequest);
    }
  }

  private boolean isTxnOpen() {
    return txnId > 0;
  }

  @Override
  public boolean hasPendingWork() {
    return isTxnOpen();
  }
}
