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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.common.util.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * A runnable class which takes in cleaningRequestHandler and cleaning request and deletes the files
 * according to the cleaning request.
 */
public class FSRemover {
  private static final Logger LOG = LoggerFactory.getLogger(FSRemover.class);
  private final HiveConf conf;
  private final ReplChangeManager replChangeManager;
  private final MetadataCache metadataCache;

  public FSRemover(HiveConf conf, ReplChangeManager replChangeManager, MetadataCache metadataCache) {
    this.conf = conf;
    this.replChangeManager = replChangeManager;
    this.metadataCache = metadataCache;
  }

  public List<Path> clean(CleanupRequest cr) throws MetaException {
    Ref<List<Path>> removedFiles = Ref.from(new ArrayList<>());
    try {
      Callable<List<Path>> cleanUpTask;
      cleanUpTask = () -> removeFiles(cr);

      if (CompactorUtil.runJobAsSelf(cr.runAs())) {
        removedFiles.value = cleanUpTask.call();
      } else {
        LOG.info("Cleaning as user {} for {}", cr.runAs(), cr.getFullPartitionName());
        UserGroupInformation ugi = UserGroupInformation.createProxyUser(cr.runAs(),
                UserGroupInformation.getLoginUser());
        try {
          ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
            removedFiles.value = cleanUpTask.call();
            return null;
          });
        } finally {
          try {
            FileSystem.closeAllForUGI(ugi);
          } catch (IOException exception) {
            LOG.error("Could not clean up file-system handles for UGI: {} for {}",
                    ugi, cr.getFullPartitionName(), exception);
          }
        }
      }
    } catch (Exception ex) {
      LOG.error("Caught exception when cleaning, unable to complete cleaning of {} due to {}", cr,
              StringUtils.stringifyException(ex));
    }
    return removedFiles.value;
  }

  /**
   * @param cr Cleaning request
   * @return List of deleted files if any files were removed
   */
  private List<Path> removeFiles(CleanupRequest cr)
          throws MetaException, IOException {
    List<Path> deleted = new ArrayList<>();
    if (cr.getObsoleteDirs().isEmpty()) {
      return deleted;
    }
    LOG.info("About to remove {} obsolete directories from {}. {}", cr.getObsoleteDirs().size(),
            cr.getLocation(), CompactorUtil.getDebugInfo(cr.getObsoleteDirs()));
    boolean needCmRecycle;
    try {
      Database db = metadataCache.computeIfAbsent(cr.getDbName(),
              () -> CompactorUtil.resolveDatabase(conf, cr.getDbName()));
      needCmRecycle = ReplChangeManager.isSourceOfReplication(db);
    } catch (NoSuchObjectException ex) {
      // can not drop a database which is a source of replication
      needCmRecycle = false;
    } catch (RuntimeException ex) {
      if (ex.getCause() instanceof NoSuchObjectException) {
        // can not drop a database which is a source of replication
        needCmRecycle = false;
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      throw new MetaException(ex.getMessage());
    }
    FileSystem fs = new Path(cr.getLocation()).getFileSystem(conf);
    for (Path dead : cr.getObsoleteDirs()) {
      LOG.debug("Going to delete path: {}", dead);
      if (needCmRecycle) {
        replChangeManager.recycle(dead, ReplChangeManager.RecycleType.MOVE, cr.isPurge());
      }
      if (FileUtils.moveToTrash(fs, dead, conf, cr.isPurge())) {
        deleted.add(dead);
      }
    }
    return deleted;
  }
}
