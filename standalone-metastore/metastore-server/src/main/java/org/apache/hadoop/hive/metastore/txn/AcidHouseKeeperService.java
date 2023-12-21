/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.txn;

import org.apache.commons.lang3.Functions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.apache.commons.lang3.Functions.FailableRunnable;

/**
 * Performs background tasks for Transaction management in Hive.
 * Runs inside Hive Metastore Service.
 */
public class AcidHouseKeeperService implements MetastoreTaskThread {

  private static final Logger LOG = LoggerFactory.getLogger(AcidHouseKeeperService.class);

  private Configuration conf;
  private TxnStore txnHandler;
  private String serviceName;

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public TxnStore getTxnHandler() {
    return txnHandler;
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
    txnHandler = TxnUtils.getTxnStore(conf);
    this.serviceName = this.getClass().getSimpleName();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public long runFrequency(TimeUnit unit) {
    return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.ACID_HOUSEKEEPER_SERVICE_INTERVAL, unit);
  }

  @Override
  public void run() {
    TxnStore.MutexAPI.LockHandle handle = null;
    try {
      handle = txnHandler.getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.HouseKeeper.name());
      LOG.info("Starting to run {}", serviceName);
      long start = System.currentTimeMillis();
      cleanTheHouse();
      LOG.debug("Total time {} took: {} seconds.", serviceName, elapsedSince(start));
    } catch (Throwable t) {
      LOG.error("Unexpected error in thread: {}, message: {}", Thread.currentThread().getName(), t.getMessage(), t);
    } finally {
      if (handle != null) {
        handle.releaseLocks();
      }
    }
  }

  void cleanTheHouse() {
    performTask(txnHandler::performTimeOuts, "Cleaning timed out txns and locks");
    performTask(txnHandler::performWriteSetGC, "Cleaning obsolete write set entries");
    performTask(txnHandler::cleanTxnToWriteIdTable, "Cleaning obsolete TXN_TO_WRITE_ID entries");
  }

  void performTask(FailableRunnable<MetaException> task, String description) {
    long start = System.currentTimeMillis();
    Functions.run(task);
    LOG.debug("{} took {} seconds.", description, elapsedSince(start));
  }

  private long elapsedSince(long start) {
    return (System.currentTimeMillis() - start) / 1000;
  }
}
