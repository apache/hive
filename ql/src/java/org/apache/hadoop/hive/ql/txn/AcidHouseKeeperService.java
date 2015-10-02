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
 */
package org.apache.hadoop.hive.ql.txn;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HouseKeeperService;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.TxnManagerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Performs background tasks for Transaction management in Hive.
 * Runs inside Hive Metastore Service.
 */
public class AcidHouseKeeperService implements HouseKeeperService {
  private static final Log LOG = LogFactory.getLog(AcidHouseKeeperService.class);
  private ScheduledExecutorService pool = null;
  private AtomicInteger isAliveCounter = new AtomicInteger(Integer.MIN_VALUE);
  @Override
  public void start(HiveConf hiveConf) throws Exception {
    HiveTxnManager mgr = TxnManagerFactory.getTxnManagerFactory().getTxnManager(hiveConf);
    if(!mgr.supportsAcid()) {
      LOG.info(AcidHouseKeeperService.class.getName() + " not started since " +
        mgr.getClass().getName()  + " does not support Acid.");
      return;//there are no transactions in this case
    }
    pool = Executors.newScheduledThreadPool(1, new ThreadFactory() {
      private final AtomicInteger threadCounter = new AtomicInteger();
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "DeadTxnReaper-" + threadCounter.getAndIncrement());
      }
    });
    TimeUnit tu = TimeUnit.MILLISECONDS;
    pool.scheduleAtFixedRate(new TimedoutTxnReaper(hiveConf, this),
      hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_TIMEDOUT_TXN_REAPER_START, tu),
      hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_TIMEDOUT_TXN_REAPER_INTERVAL, tu),
      TimeUnit.MILLISECONDS);
    LOG.info("Started " + this.getClass().getName() + " with delay/interval = " +
      hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_TIMEDOUT_TXN_REAPER_START, tu) + "/" +
      hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_TIMEDOUT_TXN_REAPER_INTERVAL, tu) + " " + tu);
  }
  @Override
  public void stop() {
    if(pool != null && !pool.isShutdown()) {
      pool.shutdown();
    }
    pool = null;
  }
  @Override
  public String getServiceDescription() {
    return "Abort expired transactions";
  }
  private static final class TimedoutTxnReaper implements Runnable {
    private final TxnHandler txnHandler;
    private final AcidHouseKeeperService owner;
    private TimedoutTxnReaper(HiveConf hiveConf, AcidHouseKeeperService owner) {
      txnHandler = new TxnHandler(hiveConf);
      this.owner = owner;
    }
    @Override
    public void run() {
      try {
        long startTime = System.currentTimeMillis();
        txnHandler.performTimeOuts();
        int count = owner.isAliveCounter.incrementAndGet();
        LOG.info("timeout reaper ran for " + (System.currentTimeMillis() - startTime)/1000 + "seconds.  isAliveCounter=" + count);
      }
      catch(Throwable t) {
        LOG.fatal("Serious error in " + Thread.currentThread().getName() + ": " + t.getMessage(), t);
      }
    }
  }

  /**
   * This is used for testing only.  Each time the housekeeper runs, counter is incremented by 1.
   * Starts with {@link java.lang.Integer#MIN_VALUE}
   */
  public int getIsAliveCounter() {
    return isAliveCounter.get();
  }
}
