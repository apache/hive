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
import org.apache.hadoop.hive.common.ValidTxnListImpl;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * A class to clean directories after compactions.  This will run in a separate thread.
 */
public class Cleaner extends CompactorThread {
  static final private String CLASS_NAME = Cleaner.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

  private long cleanerCheckInterval = 5000;

  @Override
  public void run() {
    // Make sure nothing escapes this run method and kills the metastore at large,
    // so wrap it in a big catch Throwable statement.
    do {
      try {
        long startedAt = System.currentTimeMillis();

        // Now look for new entries ready to be cleaned.
        List<CompactionInfo> toClean = txnHandler.findReadyToClean();
        for (CompactionInfo ci : toClean) {
          LockComponent comp = null;
          comp = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, ci.dbname);
          comp.setTablename(ci.tableName);
          if (ci.partName != null)  comp.setPartitionname(ci.partName);
          List<LockComponent> components = new ArrayList<LockComponent>(1);
          components.add(comp);
          LockRequest rqst = new LockRequest(components, System.getProperty("user.name"),
              Worker.hostname());
          LockResponse rsp = txnHandler.lockNoWait(rqst);
          try {
            if (rsp.getState() == LockState.ACQUIRED) {
              clean(ci);
            }
          } finally {
            if (rsp.getState() == LockState.ACQUIRED) {
              txnHandler.unlock(new UnlockRequest(rsp.getLockid()));
            }
          }
        }

        // Now, go back to bed until it's time to do this again
        long elapsedTime = System.currentTimeMillis() - startedAt;
        if (elapsedTime >= cleanerCheckInterval || stop.boolVal)  continue;
        else Thread.sleep(cleanerCheckInterval - elapsedTime);
      } catch (Throwable t) {
        LOG.error("Caught an exception in the main loop of compactor cleaner, " +
            StringUtils.stringifyException(t));
      }
    } while (!stop.boolVal);
  }

  private void clean(CompactionInfo ci) throws MetaException {
    LOG.info("Starting cleaning for " + ci.getFullPartitionName());
    try {
      StorageDescriptor sd = resolveStorageDescriptor(resolveTable(ci), resolvePartition(ci));
      final String location = sd.getLocation();

      // Create a bogus validTxnList with a high water mark set to MAX_LONG and no open
      // transactions.  This assures that all deltas are treated as valid and all we return are
      // obsolete files.
      final ValidTxnList txnList = new ValidTxnListImpl();

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
