/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.TxnInfo;
import org.apache.hadoop.hive.metastore.api.TxnState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class TxnUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TxnUtils.class);

  /**
   * Transform a {@link org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse} to a
   * {@link org.apache.hadoop.hive.common.ValidTxnList}.  This assumes that the caller intends to
   * read the files, and thus treats both open and aborted transactions as invalid.
   * @param txns txn list from the metastore
   * @param currentTxn Current transaction that the user has open.  If this is greater than 0 it
   *                   will be removed from the exceptions list so that the user sees his own
   *                   transaction as valid.
   * @return a valid txn list.
   */
  public static ValidTxnList createValidReadTxnList(GetOpenTxnsResponse txns, long currentTxn) {
    long highWater = txns.getTxn_high_water_mark();
    Set<Long> open = txns.getOpen_txns();
    long[] exceptions = new long[open.size() - (currentTxn > 0 ? 1 : 0)];
    int i = 0;
    for(long txn: open) {
      if (currentTxn > 0 && currentTxn == txn) continue;
      exceptions[i++] = txn;
    }
    return new ValidReadTxnList(exceptions, highWater);
  }

  /**
   * Transform a {@link org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse} to a
   * {@link org.apache.hadoop.hive.common.ValidTxnList}.  This assumes that the caller intends to
   * compact the files, and thus treats only open transactions as invalid.  Additionally any
   * txnId > highestOpenTxnId is also invalid.  This is avoid creating something like
   * delta_17_120 where txnId 80, for example, is still open.
   * @param txns txn list from the metastore
   * @return a valid txn list.
   */
  public static ValidTxnList createValidCompactTxnList(GetOpenTxnsInfoResponse txns) {
    long highWater = txns.getTxn_high_water_mark();
    long minOpenTxn = Long.MAX_VALUE;
    long[] exceptions = new long[txns.getOpen_txnsSize()];
    int i = 0;
    for (TxnInfo txn : txns.getOpen_txns()) {
      if (txn.getState() == TxnState.OPEN) minOpenTxn = Math.min(minOpenTxn, txn.getId());
      exceptions[i++] = txn.getId();
    }
    highWater = minOpenTxn == Long.MAX_VALUE ? highWater : minOpenTxn - 1;
    return new ValidCompactorTxnList(exceptions, -1, highWater);
  }

  /**
   * Get an instance of the TxnStore that is appropriate for this store
   * @param conf configuration
   * @return txn store
   */
  public static TxnStore getTxnStore(HiveConf conf) {
    String className = conf.getVar(HiveConf.ConfVars.METASTORE_TXN_STORE_IMPL);
    try {
      TxnStore handler = ((Class<? extends TxnHandler>) MetaStoreUtils.getClass(
        className)).newInstance();
      handler.setConf(conf);
      return handler;
    } catch (Exception e) {
      LOG.error("Unable to instantiate raw store directly in fastpath mode", e);
      throw new RuntimeException(e);
    }
  }
}
