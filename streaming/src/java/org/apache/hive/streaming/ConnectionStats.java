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

import java.util.concurrent.atomic.LongAdder;

/**
 * Store statistics about streaming connection.
 */
public class ConnectionStats {
  private LongAdder recordsWritten = new LongAdder();
  private LongAdder recordsSize = new LongAdder();
  private LongAdder committedTransactions = new LongAdder();
  private LongAdder abortedTransactions = new LongAdder();
  private LongAdder autoFlushCount = new LongAdder();
  private LongAdder metastoreCalls = new LongAdder();

  public void incrementRecordsWritten() {
    recordsWritten.increment();
  }

  public void incrementCommittedTransactions() {
    committedTransactions.increment();
  }

  public void incrementAbortedTransactions() {
    abortedTransactions.increment();
  }

  public void incrementAutoFlushCount() {
    autoFlushCount.increment();
  }

  public void incrementMetastoreCalls() {
    metastoreCalls.increment();
  }

  public void incrementRecordsSize(long delta) {
    recordsSize.add(delta);
  }

  public long getRecordsWritten() {
    return recordsWritten.longValue();
  }

  public long getRecordsSize() {
    return recordsSize.longValue();
  }

  public long getCommittedTransactions() {
    return committedTransactions.longValue();
  }

  public long getAbortedTransactions() {
    return abortedTransactions.longValue();
  }

  public long getAutoFlushCount() {
    return autoFlushCount.longValue();
  }

  public long getMetastoreCalls() {
    return metastoreCalls.longValue();
  }

  @Override
  public String toString() {
    return "{records-written: " + recordsWritten + ", records-size: "+ recordsSize + ", committed-transactions: " +
      committedTransactions + ", aborted-transactions: " + abortedTransactions + ", auto-flushes: " + autoFlushCount +
      ", metastore-calls: " + metastoreCalls + " }";
  }
}
