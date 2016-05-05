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
 * Periodically cleans WriteSet tracking information used in Transaction management
 */
public class AcidWriteSetService extends HouseKeeperServiceBase {
  private static final Logger LOG = LoggerFactory.getLogger(AcidWriteSetService.class);
  @Override
  protected long getStartDelayMs() {
    return 0;
  }
  @Override
  protected long getIntervalMs() {
    return hiveConf.getTimeVar(HiveConf.ConfVars.WRITE_SET_REAPER_INTERVAL, TimeUnit.MILLISECONDS);
  }
  @Override
  protected Runnable getScheduedAction(HiveConf hiveConf, AtomicInteger isAliveCounter) {
    return new WriteSetReaper(hiveConf, isAliveCounter);
  }
  @Override
  public String getServiceDescription() {
    return "Periodically cleans obsolete WriteSet tracking information used in Transaction management";
  }
  private static final class WriteSetReaper implements Runnable {
    private final TxnStore txnHandler;
    private final AtomicInteger isAliveCounter;
    private WriteSetReaper(HiveConf hiveConf, AtomicInteger isAliveCounter) {
      txnHandler = TxnUtils.getTxnStore(hiveConf);
      this.isAliveCounter = isAliveCounter;
    }
    @Override
    public void run() {
      TxnStore.MutexAPI.LockHandle handle = null;
      try {
        handle = txnHandler.getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.WriteSetCleaner.name());
        long startTime = System.currentTimeMillis();
        txnHandler.performWriteSetGC();
        int count = isAliveCounter.incrementAndGet();
        LOG.info("cleaner ran for " + (System.currentTimeMillis() - startTime)/1000 + "seconds.  isAliveCounter=" + count);
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
