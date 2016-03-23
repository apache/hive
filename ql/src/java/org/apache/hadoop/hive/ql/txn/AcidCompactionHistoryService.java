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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.ql.txn.compactor.HouseKeeperServiceBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Purges obsolete items from compaction history data
 */
public class AcidCompactionHistoryService extends HouseKeeperServiceBase {
  private static final Logger LOG = LoggerFactory.getLogger(AcidCompactionHistoryService.class);

  @Override
  protected long getStartDelayMs() {
    return 0;
  }
  @Override
  protected long getIntervalMs() {
    return hiveConf.getTimeVar(HiveConf.ConfVars.COMPACTOR_HISTORY_REAPER_INTERVAL, TimeUnit.MILLISECONDS);
  }
  @Override
  protected Runnable getScheduedAction(HiveConf hiveConf, AtomicInteger isAliveCounter) {
    return new ObsoleteEntryReaper(hiveConf, isAliveCounter);
  }

  @Override
  public String getServiceDescription() {
    return "Removes obsolete entries from Compaction History";
  }
  
  private static final class ObsoleteEntryReaper implements Runnable {
    private final TxnStore txnHandler;
    private final AtomicInteger isAliveCounter;
    private ObsoleteEntryReaper(HiveConf hiveConf, AtomicInteger isAliveCounter) {
      txnHandler = TxnUtils.getTxnStore(hiveConf);
      this.isAliveCounter = isAliveCounter;
    }
    
    @Override
    public void run() {
      TxnStore.MutexAPI.LockHandle handle = null;
      try {
        handle = txnHandler.getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.CompactionHistory.name());
        long startTime = System.currentTimeMillis();
        txnHandler.purgeCompactionHistory();
        int count = isAliveCounter.incrementAndGet(); 
        LOG.info("History reaper reaper ran for " + (System.currentTimeMillis() - startTime)/1000 + "seconds.  isAliveCounter=" + count);
      }
      catch(Throwable t) {
        LOG.error("Serious error in {}", Thread.currentThread().getName(), ": {}" + t.getMessage(), t);
      }
      finally {
        if(handle != null) {
          handle.releaseLocks();
        }
      }
    }
  }
}
