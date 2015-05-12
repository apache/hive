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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A class to clean directories after compactions.  This will run in a separate thread.
 */
public class Cleaner extends CompactorThread {
  static final private String CLASS_NAME = Cleaner.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

  private long cleanerCheckInterval = 0;

  // List of compactions to clean.
  private Map<Long, Set<Long>> compactId2LockMap = new HashMap<Long, Set<Long>>();
  private Map<Long, CompactionInfo> compactId2CompactInfoMap = new HashMap<Long, CompactionInfo>();

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
      long startedAt = System.currentTimeMillis();
      // Make sure nothing escapes this run method and kills the metastore at large,
      // so wrap it in a big catch Throwable statement.
      try {

        // First look for all the compactions that are waiting to be cleaned.  If we have not
        // seen an entry before, look for all the locks held on that table or partition and
        // record them.  We will then only clean the partition once all of those locks have been
        // released.  This way we avoid removing the files while they are in use,
        // while at the same time avoiding starving the cleaner as new readers come along.
        // This works because we know that any reader who comes along after the worker thread has
        // done the compaction will read the more up to date version of the data (either in a
        // newer delta or in a newer base).
        List<CompactionInfo> toClean = txnHandler.findReadyToClean();
        if (toClean.size() > 0 || compactId2LockMap.size() > 0) {
          ShowLocksResponse locksResponse = txnHandler.showLocks(new ShowLocksRequest());

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
                clean(compactId2CompactInfoMap.get(queueEntry.getKey()));
              } else {
                // Remove the locks we didn't see so we don't look for them again next time
                for (Long lockId : expiredLocks) {
                  queueEntry.getValue().remove(lockId);
                }
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
      if (ci.dbname.equals(lock.getDbname())) {
        if ((ci.tableName == null && lock.getTablename() == null) ||
            (ci.tableName != null && ci.tableName.equals(lock.getTablename()))) {
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
    LOG.info("Starting cleaning for " + ci.getFullPartitionName());
    try {
      Table t = resolveTable(ci);
      if (t == null) {
        // The table was dropped before we got around to cleaning it.
        LOG.info("Unable to find table " + ci.getFullTableName() + ", assuming it was dropped");
        return;
      }
      Partition p = null;
      if (ci.partName != null) {
        p = resolvePartition(ci);
        if (p == null) {
          // The partition was dropped before we got around to cleaning it.
          LOG.info("Unable to find partition " + ci.getFullPartitionName() +
              ", assuming it was dropped");
          return;
        }
      }
      StorageDescriptor sd = resolveStorageDescriptor(t, p);
      final String location = sd.getLocation();

      // Create a bogus validTxnList with a high water mark set to MAX_LONG and no open
      // transactions.  This assures that all deltas are treated as valid and all we return are
      // obsolete files.
      final ValidTxnList txnList = new ValidReadTxnList();

      if (runJobAsSelf(ci.runAs)) {
        removeFiles(location, txnList);
      } else {
        LOG.info("Cleaning as user " + ci.runAs);
        UserGroupInformation ugi = UserGroupInformation.createProxyUser(ci.runAs,
            UserGroupInformation.getLoginUser());
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            removeFiles(location, txnList);
            return null;
          }
        });
      }

    } catch (Exception e) {
      LOG.error("Caught exception when cleaning, unable to complete cleaning " +
          StringUtils.stringifyException(e));
    } finally {
      // We need to clean this out one way or another.
      txnHandler.markCleaned(ci);
    }
  }

  private void removeFiles(String location, ValidTxnList txnList) throws IOException {
    AcidUtils.Directory dir = AcidUtils.getAcidState(new Path(location), conf, txnList);
    List<FileStatus> obsoleteDirs = dir.getObsolete();
    List<Path> filesToDelete = new ArrayList<Path>(obsoleteDirs.size());
    for (FileStatus stat : obsoleteDirs) {
      filesToDelete.add(stat.getPath());
    }
    if (filesToDelete.size() < 1) {
      LOG.warn("Hmm, nothing to delete in the cleaner for directory " + location +
          ", that hardly seems right.");
      return;
    }
    FileSystem fs = filesToDelete.get(0).getFileSystem(conf);

    for (Path dead : filesToDelete) {
      LOG.debug("Doing to delete path " + dead.toString());
      fs.delete(dead, true);
    }
  }

}
