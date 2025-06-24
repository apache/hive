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

import java.util.List;

/**
 * CommitTxnEvent
 * Event generated for commit transaction operation
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class CommitTxnEvent extends ListenerEvent {

  private final Long txnId;
  private final TxnType txnType;
  private final List<Long> writeId;
  private final List<String> databases;

  public CommitTxnEvent(Long transactionId, IHMSHandler handler) {
    this(transactionId, null, handler, null, null);
  }

  public CommitTxnEvent(Long transactionId, TxnType txnType) {
    this(transactionId, txnType, null, null, null);
  }

  /**
   * @param transactionId Unique identification for the transaction just got committed.
   * @param txnType type of transaction
   * @param handler handler that is firing the event
   * @param databases list of databases for which commit txn event is fired
   * @param writeId write id for transaction
   */
  public CommitTxnEvent(Long transactionId, TxnType txnType, IHMSHandler handler, List<String> databases, List<Long> writeId) {
    super(true, handler);
    this.txnId = transactionId;
    this.txnType = txnType;
    this.writeId = writeId;
    this.databases = databases;
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
     * @return List of write ids for which commit txn event is fired
     */
  public List<Long> getWriteId() {
    return writeId;
  }

    /**
     * @return List of databases for which commit txn event is fired
     */
  public List<String> getDatabases() {
    return databases;
  }
}
