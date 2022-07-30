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

package org.apache.hadoop.hive.metastore.events;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.TxnType;

import java.util.ArrayList;
import java.util.List;

/**
 * AbortTxnEvent
 * Event generated for roll backing a transaction
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AbortTxnEvent extends ListenerEvent {

  private final Long txnId;
  private final TxnType txnType;
  private final List<String> dbsUpdated;

  public AbortTxnEvent(Long transactionId, IHMSHandler handler) {
    this(transactionId, null, handler, null);
  }

  public AbortTxnEvent(Long transactionId, TxnType txnType) {
    this(transactionId, txnType, null, null);
  }

  /**
   * @param transactionId Unique identification for the transaction that got rolledback.
   * @param txnType type of transaction
   * @param handler handler that is firing the event
   * @param dbsUpdated list of databases that had update events
   */
  public AbortTxnEvent(Long transactionId, TxnType txnType, IHMSHandler handler, List<String> dbsUpdated) {
    super(true, handler);
    this.txnId = transactionId;
    this.txnType = txnType;
    this.dbsUpdated = new ArrayList<String>();
    if (dbsUpdated != null) {
      this.dbsUpdated.addAll(dbsUpdated);;
    }
  }

  /**
   * @return Long txnId
   */
  public Long getTxnId() {
    return txnId;
  }

  /**
   * @return txnType
   */
  public TxnType getTxnType() {
    return txnType;
  }

  /**
   * Returns the list of the db names which might have written anything in this transaction.
   * @return {@link List} of {@link String}
   */
  public List<String> getDbsUpdated() {
    return dbsUpdated;
  }
}
