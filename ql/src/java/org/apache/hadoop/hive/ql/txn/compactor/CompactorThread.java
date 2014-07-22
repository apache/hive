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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RawStoreProxy;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.CompactionTxnHandler;
import org.apache.hadoop.hive.metastore.txn.TxnHandler;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Superclass for all threads in the compactor.
 */
abstract class CompactorThread extends Thread implements MetaStoreThread {
  static final private String CLASS_NAME = CompactorThread.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

  protected HiveConf conf;
  protected CompactionTxnHandler txnHandler;
  protected RawStore rs;
  protected int threadId;
  protected BooleanPointer stop;

  @Override
  public void setHiveConf(HiveConf conf) {
    this.conf = conf;
  }

  @Override
  public void setThreadId(int threadId) {
    this.threadId = threadId;

  }

  @Override
  public void init(BooleanPointer stop) throws MetaException {
    this.stop = stop;
    setPriority(MIN_PRIORITY);
    setDaemon(true); // this means the process will exit without waiting for this thread

    // Get our own instance of the transaction handler
    txnHandler = new CompactionTxnHandler(conf);

    // Get our own connection to the database so we can get table and partition information.
    rs = RawStoreProxy.getProxy(conf, conf,
        conf.getVar(HiveConf.ConfVars.METASTORE_RAW_STORE_IMPL), threadId);
  }

  /**
   * Find the table being compacted
   * @param ci compaction info returned from the compaction queue
   * @return metastore table
   * @throws org.apache.hadoop.hive.metastore.api.MetaException if the table cannot be found.
   */
  protected Table resolveTable(CompactionInfo ci) throws MetaException {
    try {
      return rs.getTable(ci.dbname, ci.tableName);
    } catch (MetaException e) {
      LOG.error("Unable to find table " + ci.getFullTableName() + ", " + e.getMessage());
      throw e;
    }
  }

  /**
   * Get the partition being compacted.
   * @param ci compaction info returned from the compaction queue
   * @return metastore partition, or null if there is not partition in this compaction info
   * @throws Exception if underlying calls throw, or if the partition name resolves to more than
   * one partition.
   */
  protected Partition resolvePartition(CompactionInfo ci) throws Exception {
    Partition p = null;
    if (ci.partName != null) {
      List<String> names = new ArrayList<String>(1);
      names.add(ci.partName);
      List<Partition> parts = null;
      try {
        parts = rs.getPartitionsByNames(ci.dbname, ci.tableName, names);
      } catch (Exception e) {
        LOG.error("Unable to find partition " + ci.getFullPartitionName() + ", " + e.getMessage());
        throw e;
      }
      if (parts.size() != 1) {
        LOG.error(ci.getFullPartitionName() + " does not refer to a single partition");
        throw new MetaException("Too many partitions");
      }
      return parts.get(0);
    } else {
      return null;
    }
  }

  /**
   * Get the storage descriptor for a compaction.
   * @param t table from {@link #resolveTable(org.apache.hadoop.hive.metastore.txn.CompactionInfo)}
   * @param p table from {@link #resolvePartition(org.apache.hadoop.hive.metastore.txn.CompactionInfo)}
   * @return metastore storage descriptor.
   */
  protected StorageDescriptor resolveStorageDescriptor(Table t, Partition p) {
    return (p == null) ? t.getSd() : p.getSd();
  }

  /**
   * Determine which user to run an operation as, based on the owner of the directory to be
   * compacted.  It is asserted that either the user running the hive metastore or the table
   * owner must be able to stat the directory and determine the owner.
   * @param location directory that will be read or written to.
   * @param t metastore table object
   * @return username of the owner of the location.
   * @throws java.io.IOException if neither the hive metastore user nor the table owner can stat
   * the location.
   */
  protected String findUserToRunAs(String location, Table t) throws IOException,
      InterruptedException {
    LOG.debug("Determining who to run the job as.");
    final Path p = new Path(location);
    final FileSystem fs = p.getFileSystem(conf);
    try {
      FileStatus stat = fs.getFileStatus(p);
      LOG.debug("Running job as " + stat.getOwner());
      return stat.getOwner();
    } catch (AccessControlException e) {
      // TODO not sure this is the right exception
      LOG.debug("Unable to stat file as current user, trying as table owner");

      // Now, try it as the table owner and see if we get better luck.
      final List<String> wrapper = new ArrayList<String>(1);
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(t.getOwner(),
          UserGroupInformation.getLoginUser());
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          FileStatus stat = fs.getFileStatus(p);
          wrapper.add(stat.getOwner());
          return null;
        }
      });

      if (wrapper.size() == 1) {
        LOG.debug("Running job as " + wrapper.get(0));
        return wrapper.get(0);
      }
    }
    LOG.error("Unable to stat file as either current user or table owner, giving up");
    throw new IOException("Unable to stat file");
  }

  /**
   * Determine whether to run this job as the current user or whether we need a doAs to switch
   * users.
   * @param owner of the directory we will be working in, as determined by
   * {@link #findUserToRunAs(String, org.apache.hadoop.hive.metastore.api.Table)}
   * @return true if the job should run as the current user, false if a doAs is needed.
   */
  protected boolean runJobAsSelf(String owner) {
    return (owner.equals(System.getProperty("user.name")));
  }

  protected String tableName(Table t) {
    return t.getDbName() + "." + t.getTableName();
  }
}
