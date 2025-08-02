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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshotException;
import org.apache.hadoop.hive.metastore.api.MetaException;

public class HiveMetaStoreFsImpl implements MetaStoreFS {

  public static final Logger LOG = LoggerFactory
      .getLogger("hive.metastore.hivemetastoreFsimpl");

  @Override
  public boolean deleteDir(FileSystem fs, Path f, boolean ifPurge, Configuration conf)
      throws MetaException {
    try {
      if (!fs.exists(f)) {
        LOG.warn("The path to delete does not exist: " + f);
        return true;
      }
      if (!ifPurge && FileUtils.moveToTrash(fs, f, conf)) {
        return true;
      }
      try {
        // for whatever failure reason including that trash has lower encryption zone
        // retry with force delete
        fs.delete(f, true);
      } catch (RemoteException | SnapshotException se) {
        // If this is snapshot exception or the cause is snapshot replication from HDFS, could be the case where the
        // snapshots were created by replication, so in that case attempt to delete the replication related snapshots,
        // if the exists and then re attempt delete.
        if (se instanceof SnapshotException || se.getCause() instanceof SnapshotException || se.getMessage()
            .contains("Snapshot"))
          deleteReplRelatedSnapshots(fs, f);
        // retry delete after attempting to delete replication related snapshots
        fs.delete(f, true);
      }
    } catch (Exception e) {
      MetaStoreUtils.throwMetaException(e);
    }
    return true;
  }

  /**
   * Attempts to delete the replication related snapshots
   * @param fs the filesystem
   * @param path path where the snapshots are supposed to exists.
   */
  private static void deleteReplRelatedSnapshots(FileSystem fs, Path path) {
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      // List the snapshot directory.
      FileStatus[] listing = fs.listStatus(new Path(path, ".snapshot"));
      for (FileStatus elem : listing) {
        // if the snapshot name has replication related suffix, then delete that snapshot.
        if (elem.getPath().getName().endsWith("replOld") || elem.getPath().getName().endsWith("replNew")) {
          dfs.deleteSnapshot(path, elem.getPath().getName());
        }
      }
    } catch (Exception ioe) {
      // Ignore since this method is used as part of purge which actually ignores all exception, if the directory can
      // not be deleted, so preserve the same behaviour.
      LOG.warn("Couldn't clean up replication related snapshots", ioe);
    }
  }
}
