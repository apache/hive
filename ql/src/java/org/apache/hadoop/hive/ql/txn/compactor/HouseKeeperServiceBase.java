/**
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
 */package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HouseKeeperService;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.TxnManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class HouseKeeperServiceBase implements HouseKeeperService {
  private static final Logger LOG = LoggerFactory.getLogger(HouseKeeperServiceBase.class);
  private ScheduledExecutorService pool = null;
  protected final AtomicInteger isAliveCounter = new AtomicInteger(Integer.MIN_VALUE);
  protected HiveConf hiveConf;

  @Override
  public void start(HiveConf hiveConf) throws Exception {
    this.hiveConf = hiveConf;
    HiveTxnManager mgr = TxnManagerFactory.getTxnManagerFactory().getTxnManager(hiveConf);
    if(!mgr.supportsAcid()) {
      LOG.info(this.getClass().getName() + " not started since " +
        mgr.getClass().getName()  + " does not support Acid.");
      return;//there are no transactions in this case
    }
    pool = Executors.newScheduledThreadPool(1, new ThreadFactory() {
      private final AtomicInteger threadCounter = new AtomicInteger();
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, HouseKeeperServiceBase.this.getClass().getName() + "-" + threadCounter.getAndIncrement());
      }
    });

    TimeUnit tu = TimeUnit.MILLISECONDS;
    pool.scheduleAtFixedRate(getScheduedAction(hiveConf, isAliveCounter), getStartDelayMs(),
      getIntervalMs(), tu);
    LOG.info("Started " + this.getClass().getName() + " with delay/interval = " + getStartDelayMs() + "/" +
      getIntervalMs() + " " + tu);
  }

  @Override
  public void stop() {
    if(pool != null && !pool.isShutdown()) {
      pool.shutdown();
    }
    pool = null;
  }

  /**
   * This is used for testing only.  Each time the housekeeper runs, counter is incremented by 1.
   * Starts with {@link java.lang.Integer#MIN_VALUE}
   */
  @Override
  public int getIsAliveCounter() {
    return isAliveCounter.get();
  }

  /**
   * Delay in millis before first run of the task of this service.
   */
  protected abstract long getStartDelayMs();
  /**
   * Determines how frequently the service is running its task.
   */
  protected abstract long getIntervalMs();

  /**
   * The actual task implementation.  Must increment the counter on each iteration.
   */
  protected abstract Runnable getScheduedAction(HiveConf hiveConf, AtomicInteger isAliveCounter);
}
