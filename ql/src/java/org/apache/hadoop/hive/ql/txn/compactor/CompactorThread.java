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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.RawStoreProxy;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

/**
 * Superclass for all threads in the compactor.
 */
abstract class CompactorThread extends Thread implements MetaStoreThread {
  static final private String CLASS_NAME = CompactorThread.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  protected HiveConf conf;
  protected TxnStore txnHandler;
  protected RawStore rs;
  protected int threadId;
  protected AtomicBoolean stop;
  protected AtomicBoolean looped;

  @Override
  public void setConf(Configuration configuration) {
    // TODO MS-SPLIT for now, keep a copy of HiveConf around as we need to call other methods with
    // it. This should be changed to Configuration once everything that this calls that requires
    // HiveConf is moved to the standalone metastore.
    conf = (configuration instanceof HiveConf) ? (HiveConf)configuration :
        new HiveConf(configuration, HiveConf.class);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setThreadId(int threadId) {
    this.threadId = threadId;
  }

  @Override
  public void init(AtomicBoolean stop, AtomicBoolean looped) throws MetaException {
    this.stop = stop;
    this.looped = looped;
    setPriority(MIN_PRIORITY);
    setDaemon(true); // this means the process will exit without waiting for this thread

    // Get our own instance of the transaction handler
    txnHandler = TxnUtils.getTxnStore(conf);

    // Get our own connection to the database so we can get table and partition information.
    rs = RawStoreProxy.getProxy(conf, conf,
        MetastoreConf.getVar(conf, MetastoreConf.ConfVars.RAW_STORE_IMPL), threadId);
  }

  /**
   * Find the table being compacted
   * @param ci compaction info returned from the compaction queue
   * @return metastore table
   * @throws org.apache.hadoop.hive.metastore.api.MetaException if the table cannot be found.
   */
  protected Table resolveTable(CompactionInfo ci) throws MetaException {
    try {
      return rs.getTable(getDefaultCatalog(conf), ci.dbname, ci.tableName);
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
    if (ci.partName != null) {
      List<Partition> parts;
      try {
        parts = rs.getPartitionsByNames(getDefaultCatalog(conf), ci.dbname, ci.tableName,
            Collections.singletonList(ci.partName));
        if (parts == null || parts.size() == 0) {
          // The partition got dropped before we went looking for it.
          return null;
        }
      } catch (Exception e) {
        LOG.error("Unable to find partition " + ci.getFullPartitionName() + ", " + e.getMessage());
        throw e;
      }
      if (parts.size() != 1) {
        LOG.error(ci.getFullPartitionName() + " does not refer to a single partition. " + parts);
        throw new MetaException("Too many partitions for : " + ci.getFullPartitionName());
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
      final List<String> wrapper = new ArrayList<>(1);
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(t.getOwner(),
          UserGroupInformation.getLoginUser());
      ugi.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // need to use a new filesystem object here to have the correct ugi
          FileSystem proxyFs = p.getFileSystem(conf);
          FileStatus stat = proxyFs.getFileStatus(p);
          wrapper.add(stat.getOwner());
          return null;
        }
      });
      try {
        FileSystem.closeAllForUGI(ugi);
      } catch (IOException exception) {
        LOG.error("Could not clean up file-system handles for UGI: " + ugi, exception);
      }

      if (wrapper.size() == 1) {
        LOG.debug("Running job as " + wrapper.get(0));
        return wrapper.get(0);
      }
    }
    LOG.error("Unable to stat file " + p + " as either current user(" + UserGroupInformation.getLoginUser() +
      ") or table owner(" + t.getOwner() + "), giving up");
    throw new IOException("Unable to stat file: " + p);
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
    return Warehouse.getQualifiedName(t);
  }
}
