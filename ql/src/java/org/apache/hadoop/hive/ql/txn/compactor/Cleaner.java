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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.txn.TxnCommonUtils;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * A class to clean directories after compactions.  This will run in a separate thread.
 */
public class Cleaner extends MetaStoreCompactorThread {
  static final private String CLASS_NAME = Cleaner.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private long cleanerCheckInterval = 0;

  private ReplChangeManager replChangeManager;

  @Override
  public void init(AtomicBoolean stop, AtomicBoolean looped) throws Exception {
    super.init(stop, looped);
    replChangeManager = ReplChangeManager.getInstance(conf);
  }

  @Override
  public void run() {
    if (cleanerCheckInterval == 0) {
      cleanerCheckInterval = conf.getTimeVar(
          HiveConf.ConfVars.HIVE_COMPACTOR_CLEANER_RUN_INTERVAL, TimeUnit.MILLISECONDS);
    }

    do {
      // This is solely for testing.  It checks if the test has set the looped value to false,
      // and if so remembers that and then sets it to true at the end.  We have to check here
      // first to make sure we go through a complete iteration of the loop before resetting it.
      boolean setLooped = !looped.get();
      TxnStore.MutexAPI.LockHandle handle = null;
      long startedAt = -1;
      // Make sure nothing escapes this run method and kills the metastore at large,
      // so wrap it in a big catch Throwable statement.
      try {
        handle = txnHandler.getMutexAPI().acquireLock(TxnStore.MUTEX_KEY.Cleaner.name());
        startedAt = System.currentTimeMillis();
        long minOpenTxnId = txnHandler.findMinOpenTxnId();
        for(CompactionInfo compactionInfo : txnHandler.findReadyToClean()) {
          clean(compactionInfo, minOpenTxnId);
        }
      } catch (Throwable t) {
        LOG.error("Caught an exception in the main loop of compactor cleaner, " +
            StringUtils.stringifyException(t));
      }
      finally {
        if (handle != null) {
          handle.releaseLocks();
        }
      }
      if (setLooped) {
        looped.set(true);
      }
      // Now, go back to bed until it's time to do this again
      long elapsedTime = System.currentTimeMillis() - startedAt;
      if (elapsedTime >= cleanerCheckInterval || stop.get())  {
        continue;
      } else {
        try {
          Thread.sleep(cleanerCheckInterval - elapsedTime);
        } catch (InterruptedException ie) {
          // What can I do about it?
        }
      }
    } while (!stop.get());
  }

  private void clean(CompactionInfo ci, long minOpenTxnGLB) throws MetaException {
    LOG.info("Starting cleaning for " + ci);
    try {
      Table t = resolveTable(ci);
      if (t == null) {
        // The table was dropped before we got around to cleaning it.
        LOG.info("Unable to find table " + ci.getFullTableName() + ", assuming it was dropped." +
            idWatermark(ci));
        txnHandler.markCleaned(ci);
        return;
      }
      Partition p = null;
      if (ci.partName != null) {
        p = resolvePartition(ci);
        if (p == null) {
          // The partition was dropped before we got around to cleaning it.
          LOG.info("Unable to find partition " + ci.getFullPartitionName() +
              ", assuming it was dropped." + idWatermark(ci));
          txnHandler.markCleaned(ci);
          return;
        }
      }
      StorageDescriptor sd = resolveStorageDescriptor(t, p);
      final String location = sd.getLocation();
      ValidTxnList validTxnList =
          TxnUtils.createValidTxnListForCleaner(txnHandler.getOpenTxns(), minOpenTxnGLB);
      //save it so that getAcidState() sees it
      conf.set(ValidTxnList.VALID_TXNS_KEY, validTxnList.writeToString());
      /**
       * {@code validTxnList} is capped by minOpenTxnGLB so if
       * {@link AcidUtils#getAcidState(Path, Configuration, ValidWriteIdList)} sees a base/delta
       * produced by a compactor, that means every reader that could be active right now see it
       * as well.  That means if this base/delta shadows some earlier base/delta, the it will be
       * used in favor of any files that it shadows.  Thus the shadowed files are safe to delete.
       *
       *
       * The metadata about aborted writeIds (and consequently aborted txn IDs) cannot be deleted
       * above COMPACTION_QUEUE.CQ_HIGHEST_WRITE_ID.
       * See {@link TxnStore#markCleaned(CompactionInfo)} for details.
       * For example given partition P1, txnid:150 starts and sees txnid:149 as open.
       * Say compactor runs in txnid:160, but 149 is still open and P1 has the largest resolved
       * writeId:17.  Compactor will produce base_17_c160.
       * Suppose txnid:149 writes delta_18_18
       * to P1 and aborts.  Compactor can only remove TXN_COMPONENTS entries
       * up to (inclusive) writeId:17 since delta_18_18 may be on disk (and perhaps corrupted) but
       * not visible based on 'validTxnList' capped at minOpenTxn so it will not not be cleaned by
       * {@link #removeFiles(String, ValidWriteIdList, CompactionInfo)} and so we must keep the
       * metadata that says that 18 is aborted.
       * In a slightly different case, whatever txn created delta_18 (and all other txn) may have
       * committed by the time cleaner runs and so cleaner will indeed see delta_18_18 and remove
       * it (since it has nothing but aborted data).  But we can't tell which actually happened
       * in markCleaned() so make sure it doesn't delete meta above CG_CQ_HIGHEST_WRITE_ID.
       *
       * We could perhaps make cleaning of aborted and obsolete and remove all aborted files up
       * to the current Min Open Write Id, this way aborted TXN_COMPONENTS meta can be removed
       * as well up to that point which may be higher than CQ_HIGHEST_WRITE_ID.  This could be
       * useful if there is all of a sudden a flood of aborted txns.  (For another day).
       */
      List<String> tblNames = Collections.singletonList(
          TableName.getDbTable(t.getDbName(), t.getTableName()));
      GetValidWriteIdsRequest rqst = new GetValidWriteIdsRequest(tblNames);
      rqst.setValidTxnList(validTxnList.writeToString());
      GetValidWriteIdsResponse rsp = txnHandler.getValidWriteIds(rqst);
      //we could have no write IDs for a table if it was never written to but
      // since we are in the Cleaner phase of compactions, there must have
      // been some delta/base dirs
      assert rsp != null && rsp.getTblValidWriteIdsSize() == 1;
      //Creating 'reader' list since we are interested in the set of 'obsolete' files
      ValidReaderWriteIdList validWriteIdList =
          TxnCommonUtils.createValidReaderWriteIdList(rsp.getTblValidWriteIds().get(0));

      if (runJobAsSelf(ci.runAs)) {
        removeFiles(location, validWriteIdList, ci);
      } else {
        LOG.info("Cleaning as user " + ci.runAs + " for " + ci.getFullPartitionName());
        UserGroupInformation ugi = UserGroupInformation.createProxyUser(ci.runAs,
            UserGroupInformation.getLoginUser());
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            removeFiles(location, validWriteIdList, ci);
            return null;
          }
        });
        try {
          FileSystem.closeAllForUGI(ugi);
        } catch (IOException exception) {
          LOG.error("Could not clean up file-system handles for UGI: " + ugi + " for " +
              ci.getFullPartitionName() + idWatermark(ci), exception);
        }
      }
      txnHandler.markCleaned(ci);
    } catch (Exception e) {
      LOG.error("Caught exception when cleaning, unable to complete cleaning of " + ci + " " +
          StringUtils.stringifyException(e));
      txnHandler.markFailed(ci);
    }
  }
  private static String idWatermark(CompactionInfo ci) {
    return " id=" + ci.id;
  }
  private void removeFiles(String location, ValidWriteIdList writeIdList, CompactionInfo ci)
          throws IOException, NoSuchObjectException {
    Path locPath = new Path(location);
    AcidUtils.Directory dir = AcidUtils.getAcidState(locPath, conf, writeIdList);
    List<FileStatus> obsoleteDirs = dir.getObsolete();
    /**
     * add anything in 'dir'  that only has data from aborted transactions - no one should be
     * trying to read anything in that dir (except getAcidState() that only reads the name of
     * this dir itself)
     * So this may run ahead of {@link CompactionInfo#highestWriteId} but it's ok (suppose there
     * are no active txns when cleaner runs).  The key is to not delete metadata about aborted
     * txns with write IDs > {@link CompactionInfo#highestWriteId}.
     * See {@link TxnStore#markCleaned(CompactionInfo)}
     */
    obsoleteDirs.addAll(dir.getAbortedDirectories());
    List<Path> filesToDelete = new ArrayList<>(obsoleteDirs.size());
    StringBuilder extraDebugInfo = new StringBuilder("[");
    for (FileStatus stat : obsoleteDirs) {
      filesToDelete.add(stat.getPath());
      extraDebugInfo.append(stat.getPath().getName()).append(",");
      if(!FileUtils.isPathWithinSubtree(stat.getPath(), locPath)) {
        LOG.info(idWatermark(ci) + " found unexpected file: " + stat.getPath());
      }
    }
    extraDebugInfo.setCharAt(extraDebugInfo.length() - 1, ']');
    LOG.info(idWatermark(ci) + " About to remove " + filesToDelete.size() +
         " obsolete directories from " + location + ". " + extraDebugInfo.toString());
    if (filesToDelete.size() < 1) {
      LOG.warn("Hmm, nothing to delete in the cleaner for directory " + location +
          ", that hardly seems right.");
      return;
    }

    FileSystem fs = filesToDelete.get(0).getFileSystem(conf);
    Database db = rs.getDatabase(getDefaultCatalog(conf), ci.dbname);
    Boolean isSourceOfRepl = ReplChangeManager.isSourceOfReplication(db);

    for (Path dead : filesToDelete) {
      LOG.debug("Going to delete path " + dead.toString());
      if (isSourceOfRepl) {
        replChangeManager.recycle(dead, ReplChangeManager.RecycleType.MOVE, true);
      }
      fs.delete(dead, true);
    }
  }
}
