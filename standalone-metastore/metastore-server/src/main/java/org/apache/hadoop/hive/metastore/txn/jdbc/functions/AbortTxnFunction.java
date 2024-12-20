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

import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.metastore.txn.entities.TxnStatus;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.txn.jdbc.commands.DeleteReplTxnMapEntryCommand;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.FindTxnStateHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.GetOpenTxnTypeAndLockHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.queries.TargetTxnIdListHandler;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.jdbc.TransactionalFunction;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class AbortTxnFunction implements TransactionalFunction<TxnType> {

  private static final Logger LOG = LoggerFactory.getLogger(AbortTxnFunction.class);

  public AbortTxnFunction(AbortTxnRequest rqst) {
    this.rqst = rqst;
  }

  private final AbortTxnRequest rqst;
  
  @Override
  public TxnType execute(MultiDataSourceJdbcResource jdbcResource) throws MetaException, NoSuchTxnException, TxnAbortedException {
    long txnid = rqst.getTxnid();
    TxnErrorMsg txnErrorMsg = TxnErrorMsg.NONE;
    long sourceTxnId = -1;
    boolean isReplayedReplTxn = TxnType.REPL_CREATED.equals(rqst.getTxn_type());
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && TxnType.DEFAULT.equals(rqst.getTxn_type());
    if (isReplayedReplTxn) {
      assert (rqst.isSetReplPolicy());
      sourceTxnId = rqst.getTxnid();
      List<Long> targetTxnIds = jdbcResource.execute(new TargetTxnIdListHandler(rqst.getReplPolicy(), Collections.singletonList(sourceTxnId)));
      if (targetTxnIds.isEmpty()) {
        // Idempotent case where txn was already closed or abort txn event received without
        // corresponding open txn event.
        LOG.info("Target txn id is missing for source txn id : {} and repl policy {}", sourceTxnId,
            rqst.getReplPolicy());
        return null;
      }
      assert targetTxnIds.size() == 1;
      txnid = targetTxnIds.get(0);
    }

    TxnType txnType = jdbcResource.execute(new GetOpenTxnTypeAndLockHandler(jdbcResource.getSqlGenerator(), txnid));
    if (txnType == null) {
      TxnStatus status = jdbcResource.execute(new FindTxnStateHandler(txnid));
      if (status == TxnStatus.ABORTED) {
        if (isReplayedReplTxn) {
          // in case of replication, idempotent is taken care by getTargetTxnId
          LOG.warn("Invalid state ABORTED for transactions started using replication replay task");
          jdbcResource.execute(new DeleteReplTxnMapEntryCommand(sourceTxnId, rqst.getReplPolicy()));
        }
        LOG.info("abortTxn({}) requested by it is already {}", JavaUtils.txnIdToString(txnid), TxnStatus.ABORTED);
        return null;
      }
      TxnUtils.raiseTxnUnexpectedState(status, txnid);
    }

    if (isReplayedReplTxn) {
      txnErrorMsg = TxnErrorMsg.ABORT_REPLAYED_REPL_TXN;
    } else if (isHiveReplTxn) {
      txnErrorMsg = TxnErrorMsg.ABORT_DEFAULT_REPL_TXN;
    } else if (rqst.isSetErrorCode()) {
      txnErrorMsg = TxnErrorMsg.getTxnErrorMsg(rqst.getErrorCode());
    }

    new AbortTxnsFunction(Collections.singletonList(txnid), false, true,
        isReplayedReplTxn, txnErrorMsg).execute(jdbcResource);

    if (isReplayedReplTxn) {
      jdbcResource.execute(new DeleteReplTxnMapEntryCommand(sourceTxnId, rqst.getReplPolicy()));
    }
    
    return txnType;
  }
}
