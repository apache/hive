/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 *     http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.txn.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.NoMutex;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Periodically cleans out empty aborted and committed txns from the TXNS table.
 * Runs inside Hive Metastore Service.
 */
public class AcidTxnCleanerService implements MetastoreTaskThread {

  private static final Logger LOG = LoggerFactory.getLogger(AcidTxnCleanerService.class);

  private Configuration conf;
  private TxnStore txnHandler;
  private boolean shouldUseMutex = true;

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public long runFrequency(TimeUnit unit) {
    return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.ACID_TXN_CLEANER_INTERVAL, unit);
  }

  @Override
  public void run() {
    TxnStore.MutexAPI mutex = shouldUseMutex ? txnHandler.getMutexAPI() : new NoMutex();
    try (AutoCloseable closeable = mutex.acquireLock(TxnStore.MUTEX_KEY.TxnCleaner.name())) {
      long start = System.currentTimeMillis();
      txnHandler.cleanEmptyAbortedAndCommittedTxns();
      LOG.debug("Txn cleaner service took: {} seconds.", elapsedSince(start));
    } catch (Exception e) {
      LOG.error("Unexpected exception in thread: {}, message: {}", Thread.currentThread().getName(), e.getMessage(), e);
    }
  }

  private long elapsedSince(long start) {
    return (System.currentTimeMillis() - start) / 1000;
  }

  @Override
  public void enforceMutex(boolean enableMutex) {
    this.shouldUseMutex = enableMutex;
  }
}
