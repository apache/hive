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
package org.apache.hadoop.hive.metastore.txn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Background running thread, periodically updating number of open transactions.
 * Runs inside Hive Metastore Service.
 */
public class AcidOpenTxnsCounterService implements MetastoreTaskThread {
  private static final Logger LOG = LoggerFactory.getLogger(AcidOpenTxnsCounterService.class);

  private Configuration conf;
  private int isAliveCounter = 0;
  private long lastLogTime = 0;
  private TxnStore txnHandler;

  @Override
  public long runFrequency(TimeUnit unit) {
    return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COUNT_OPEN_TXNS_INTERVAL, unit);
  }

  @Override
  public void run() {
    try {
      long startTime = System.currentTimeMillis();
      isAliveCounter++;
      txnHandler.countOpenTxns();
      if (System.currentTimeMillis() - lastLogTime > 60 * 1000) {
        LOG.info("AcidOpenTxnsCounterService ran for " +
            ((System.currentTimeMillis() - startTime) / 1000) +
            " seconds.  isAliveCounter = " + isAliveCounter);
        lastLogTime = System.currentTimeMillis();
      }
    }
    catch(Throwable t) {
      LOG.error("Serious error in {}", Thread.currentThread().getName(), ": {}" + t.getMessage(), t);
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
