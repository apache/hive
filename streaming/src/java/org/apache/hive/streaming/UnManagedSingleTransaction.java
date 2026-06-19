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

package org.apache.hive.streaming;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Receives a single writeId. Doesn't open connections to the metastore
 * so the commit has to be done externally by the entity that created
 * the writeId.
 */
public class UnManagedSingleTransaction extends AbstractStreamingTransaction {
  private static final Logger LOG = LoggerFactory.getLogger(
      UnManagedSingleTransaction.class.getName());
  private final String username;
  private final HiveStreamingConnection conn;
  private final Set<String> partitions = Sets.newHashSet();

  public UnManagedSingleTransaction(HiveStreamingConnection conn)
      throws StreamingException{
    assert conn.getWriteId() != null;

    this.conn = conn;
    this.username = conn.getUsername();
    this.recordWriter = conn.getRecordWriter();
    this.state = HiveStreamingConnection.TxnState.INACTIVE;

    txnToWriteIds = Lists.newArrayList(new TxnToWriteId(-1,
        conn.getWriteId()));

    boolean success = false;
    try {
      recordWriter.init(conn, txnToWriteIds.get(0).getWriteId(),
          txnToWriteIds.get(0).getWriteId(), conn.getStatementId());
      success = true;
    } finally {
      markDead(success);
    }
  }

  @Override
  public void beginNextTransaction() throws StreamingException {
    beginNextTransactionImpl("No more transactions available in" +
        " next batch for connection: " + conn + " user: " + username);
  }

  @Override
  public void commit(Set<String> partitions, String key, String value)
      throws StreamingException {
    checkIsClosed();
    boolean success = false;
    try {
      commitImpl();
      success = true;
    } finally {
      markDead(success);
    }
  }

  private void commitImpl() throws StreamingException {
    recordWriter.flush();
    List<String> partNames = new ArrayList<>(recordWriter.getPartitions());
    partitions.addAll(partNames);
    state = HiveStreamingConnection.TxnState.PREPARED_FOR_COMMIT;
  }

  @Override
  public void abort() {
    if (isTxnClosed.get()) {
      return;
    }
    state = HiveStreamingConnection.TxnState.ABORTED;
  }

  @Override
  public void close() throws StreamingException {
    if (isClosed()) {
      return;
    }
    isTxnClosed.set(true);
    abort();
    try {
      closeImpl();
    } catch (Exception ex) {
      LOG.error("Fatal error on " + toString() + "; cause " + ex.getMessage(), ex);
      throw new StreamingException("Unable to close", ex);
    }
  }

  private void closeImpl() throws StreamingException {
    state = HiveStreamingConnection.TxnState.INACTIVE;
    recordWriter.close();
  }

  @Override
  public String toString() {
    if (txnToWriteIds == null || txnToWriteIds.isEmpty()) {
      return "{}";
    }
    return "TxnId/WriteIds=[" + txnToWriteIds.get(0).getWriteId()
        + "] on connection = " + conn + "; " + "status=" + state;
  }

  /**
   * @return This class doesn't have a connection to the metastore so it won't
   * create any partition
   */
  @Override
  public Set<String> getPartitions() {
    return partitions;
  }
}
