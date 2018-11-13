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

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
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
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
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
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * A class to clean directories after compactions.  This will run in a separate thread.
 */
public class Cleaner extends CompactorThread {
  static final private String CLASS_NAME = Cleaner.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);
  private long cleanerCheckInterval = 0;

  private ReplChangeManager replChangeManager;
  // List of compactions to clean.
  private Map<Long, Set<Long>> compactId2LockMap = new HashMap<>();
  private Map<Long, CompactionInfo> compactId2CompactInfoMap = new HashMap<>();

  @Override
  public void init(AtomicBoolean stop, AtomicBoolean looped) throws MetaException {
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
        // First look for all the compactions that are waiting to be cleaned.  If we have not
        // seen an entry before, look for all the locks held on that table or partition and
        // record them.  We will then only clean the partition once all of those locks have been
        // released.  This way we avoid removing the files while they are in use,
        // while at the same time avoiding starving the cleaner as new readers come along.
        // This works because we know that any reader who comes along after the worker thread has
        // done the compaction will read the more up to date version of the data (either in a
        // newer delta or in a newer base).
        List<CompactionInfo> toClean = txnHandler.findReadyToClean();
        {
          /**
           * Since there may be more than 1 instance of Cleaner running we may have state info
           * for items which were cleaned by instances.  Here we remove them.
           *
           * In the long run if we add end_time to compaction_queue, then we can check that
           * hive_locks.acquired_at > compaction_queue.end_time + safety_buffer in which case
           * we know the lock owner is reading files created by this compaction or later.
           * The advantage is that we don't have to store the locks.
           */
          Set<Long> currentToCleanSet = new HashSet<>();
          for (CompactionInfo ci : toClean) {
            currentToCleanSet.add(ci.id);
          }
          Set<Long> cleanPerformedByOthers = new HashSet<>();
          for (long id : compactId2CompactInfoMap.keySet()) {
            if (!currentToCleanSet.contains(id)) {
              cleanPerformedByOthers.add(id);
            }
          }
          for (long id : cleanPerformedByOthers) {
            compactId2CompactInfoMap.remove(id);
            compactId2LockMap.remove(id);
          }
        }
        if (toClean.size() > 0 || compactId2LockMap.size() > 0) {
          ShowLocksResponse locksResponse = txnHandler.showLocks(new ShowLocksRequest());
          if(LOG.isDebugEnabled()) {
            dumpLockState(locksResponse);
          }
          for (CompactionInfo ci : toClean) {
            // Check to see if we have seen this request before.  If so, ignore it.  If not,
            // add it to our queue.
            if (!compactId2LockMap.containsKey(ci.id)) {
              compactId2LockMap.put(ci.id, findRelatedLocks(ci, locksResponse));
              compactId2CompactInfoMap.put(ci.id, ci);
            }
          }

          // Now, for each entry in the queue, see if all of the associated locks are clear so we
          // can clean
          Set<Long> currentLocks = buildCurrentLockSet(locksResponse);
          List<Long> expiredLocks = new ArrayList<Long>();
          List<Long> compactionsCleaned = new ArrayList<Long>();
          try {
            for (Map.Entry<Long, Set<Long>> queueEntry : compactId2LockMap.entrySet()) {
              boolean sawLock = false;
              for (Long lockId : queueEntry.getValue()) {
                if (currentLocks.contains(lockId)) {
                  sawLock = true;
                  break;
                } else {
                  expiredLocks.add(lockId);
                }
              }

              if (!sawLock) {
                // Remember to remove this when we're out of the loop,
                // we can't do it in the loop or we'll get a concurrent modification exception.
                compactionsCleaned.add(queueEntry.getKey());
                //Future thought: this may be expensive so consider having a thread pool run in parallel
                clean(compactId2CompactInfoMap.get(queueEntry.getKey()));
              } else {
                // Remove the locks we didn't see so we don't look for them again next time
                for (Long lockId : expiredLocks) {
                  queueEntry.getValue().remove(lockId);
                }
                LOG.info("Skipping cleaning of " +
                    idWatermark(compactId2CompactInfoMap.get(queueEntry.getKey())) +
                    " due to reader present: " + queueEntry.getValue());
              }
            }
          } finally {
            if (compactionsCleaned.size() > 0) {
              for (Long compactId : compactionsCleaned) {
                compactId2LockMap.remove(compactId);
                compactId2CompactInfoMap.remove(compactId);
              }
            }
          }
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

  private Set<Long> findRelatedLocks(CompactionInfo ci, ShowLocksResponse locksResponse) {
    Set<Long> relatedLocks = new HashSet<Long>();
    for (ShowLocksResponseElement lock : locksResponse.getLocks()) {
      /**
       * Hive QL is not case sensitive wrt db/table/column names
       * Partition names get
       * normalized (as far as I can tell) by lower casing column name but not partition value.
       * {@link org.apache.hadoop.hive.metastore.Warehouse#makePartName(List, List, String)}
       * {@link org.apache.hadoop.hive.ql.parse.DDLSemanticAnalyzer#getPartSpec(ASTNode)}
       * Since user input may start out in any case, compare here case-insensitive for db/table
       * but leave partition name as is.
       */
      if (ci.dbname.equalsIgnoreCase(lock.getDbname())) {
        if ((ci.tableName == null && lock.getTablename() == null) ||
            (ci.tableName != null && ci.tableName.equalsIgnoreCase(lock.getTablename()))) {
          if ((ci.partName == null && lock.getPartname() == null) ||
              (ci.partName != null && ci.partName.equals(lock.getPartname()))) {
            relatedLocks.add(lock.getLockid());
          }
        }
      }
    }

    return relatedLocks;
  }

  private Set<Long> buildCurrentLockSet(ShowLocksResponse locksResponse) {
    Set<Long> currentLocks = new HashSet<Long>(locksResponse.getLocks().size());
    for (ShowLocksResponseElement lock : locksResponse.getLocks()) {
      currentLocks.add(lock.getLockid());
    }
    return currentLocks;
  }

  private void clean(CompactionInfo ci) throws MetaException {
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

      /**
       * Each Compaction only compacts as far as the highest txn id such that all txns below it
       * are resolved (i.e. not opened).  This is what "highestWriteId" tracks.  This is only tracked
       * since Hive 1.3.0/2.0 - thus may be 0.  See ValidCompactorWriteIdList and uses for more info.
       *
       * We only want to clean up to the highestWriteId - otherwise we risk deleting deltas from
       * under an active reader.
       *
       * Suppose we have deltas D2 D3 for table T, i.e. the last compaction created D3 so now there is a 
       * clean request for D2.  
       * Cleaner checks existing locks and finds none.
       * Between that check and removeFiles() a query starts (it will be reading D3) and another compaction
       * completes which creates D4.
       * Now removeFiles() (more specifically AcidUtils.getAcidState()) will declare D3 to be obsolete
       * unless ValidWriteIdList is "capped" at highestWriteId.
       */
      final ValidWriteIdList validWriteIdList = (ci.highestWriteId > 0)
          ? new ValidReaderWriteIdList(ci.getFullTableName(), new long[0], new BitSet(),
          ci.highestWriteId)
          : new ValidReaderWriteIdList();

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
    List<Path> filesToDelete = new ArrayList<Path>(obsoleteDirs.size());
    StringBuilder extraDebugInfo = new StringBuilder("[");
    for (FileStatus stat : obsoleteDirs) {
      filesToDelete.add(stat.getPath());
      extraDebugInfo.append(stat.getPath().getName()).append(",");
      if(!FileUtils.isPathWithinSubtree(stat.getPath(), locPath)) {
        LOG.info(idWatermark(ci) + " found unexpected file: " + stat.getPath());
      }
    }
    extraDebugInfo.setCharAt(extraDebugInfo.length() - 1, ']');
    List<Long> compactIds = new ArrayList<>(compactId2CompactInfoMap.keySet());
    Collections.sort(compactIds);
    extraDebugInfo.append("compactId2CompactInfoMap.keySet(").append(compactIds).append(")");
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
  private static class LockComparator implements Comparator<ShowLocksResponseElement> {
    //sort ascending by resource, nulls first
    @Override
    public int compare(ShowLocksResponseElement o1, ShowLocksResponseElement o2) {
      if(o1 == o2) {
        return 0;
      }
      if(o1 == null) {
        return -1;
      }
      if(o2 == null) {
        return 1;
      }
      int v = o1.getDbname().compareToIgnoreCase(o2.getDbname());
      if(v != 0) {
        return v;
      }
      if(o1.getTablename() == null) {
        return -1;
      }
      if(o2.getTablename() == null) {
        return 1;
      }
      v = o1.getTablename().compareToIgnoreCase(o2.getTablename());
      if(v != 0) {
        return v;
      }
      if(o1.getPartname() == null) {
        return -1;
      }
      if(o2.getPartname() == null) {
        return 1;
      }
      v = o1.getPartname().compareToIgnoreCase(o2.getPartname());
      if(v != 0) {
        return v;
      }
      //if still equal, compare by lock ids
      v = Long.compare(o1.getLockid(), o2.getLockid());
      if(v != 0) {
        return v;
      }
      return Long.compare(o1.getLockIdInternal(), o2.getLockIdInternal());

    }
  }
  private void dumpLockState(ShowLocksResponse slr) {
    Iterator<ShowLocksResponseElement> l = slr.getLocksIterator();
    List<ShowLocksResponseElement> sortedList = new ArrayList<>();
    while(l.hasNext()) {
      sortedList.add(l.next());
    }
    //sort for readability
    sortedList.sort(new LockComparator());
    LOG.info("dumping locks");
    for(ShowLocksResponseElement lock : sortedList) {
      LOG.info(lock.toString());
    }
  }
}
