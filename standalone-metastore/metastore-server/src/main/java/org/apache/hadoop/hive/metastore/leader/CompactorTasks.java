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

package org.apache.hadoop.hive.metastore.leader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class CompactorTasks implements LeaderElection.LeadershipStateListener {

  private final Configuration configuration;
  private final boolean runOnlyWorker;

  // each MetaStoreThread runs as a thread
  private Map<MetaStoreThread, AtomicBoolean> metastoreThreadsMap;

  public CompactorTasks(Configuration configuration, boolean runOnlyWorker) {
    // recreate a new configuration
    this.configuration = new Configuration(requireNonNull(configuration,
        "configuration is null"));
    this.runOnlyWorker = runOnlyWorker;
  }

  // Copied from HiveMetaStore
  private MetaStoreThread instantiateThread(String classname) throws Exception {
    Object o = JavaUtils.newInstance(Class.forName(classname));
    if (MetaStoreThread.class.isAssignableFrom(o.getClass())) {
      return (MetaStoreThread) o;
    } else {
      String s = classname + " is not an instance of MetaStoreThread.";
      HiveMetaStore.LOG.error(s);
      throw new IOException(s);
    }
  }

  public List<MetaStoreThread> getCompactorThreads() throws Exception {
    List<MetaStoreThread> compactors = new ArrayList<>();
    if (!runOnlyWorker) {
      if (MetastoreConf.getBoolVar(configuration, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON)) {
        MetaStoreThread initiator = instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Initiator");
        compactors.add(initiator);
      }
      if (MetastoreConf.getBoolVar(configuration, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON)) {
        MetaStoreThread cleaner = instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Cleaner");
        compactors.add(cleaner);
      }
    } else {
      boolean runInMetastore = MetastoreConf.getVar(configuration,
          MetastoreConf.ConfVars.HIVE_METASTORE_RUNWORKER_IN).equals("metastore");
      if (runInMetastore) {
        HiveMetaStore.LOG.warn("Running compaction workers on HMS side is not suggested because compaction pools are not supported in HMS " +
            "(HIVE-26443). Consider removing the hive.metastore.runworker.in configuration setting, as it will be " +
            "comletely removed in future releases.");
        int numWorkers = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS);
        for (int i = 0; i < numWorkers; i++) {
          MetaStoreThread worker = instantiateThread("org.apache.hadoop.hive.ql.txn.compactor.Worker");
          compactors.add(worker);
        }
      }
    }
    return compactors;
  }

  private void logCompactionParameters() {
    if (!runOnlyWorker) {
      HiveMetaStore.LOG.info("Compaction HMS parameters:");
      HiveMetaStore.LOG.info("metastore.compactor.initiator.on = {}",
          MetastoreConf.getBoolVar(configuration, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON));
      HiveMetaStore.LOG.info("metastore.compactor.cleaner.on = {}",
          MetastoreConf.getBoolVar(configuration, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON));
      HiveMetaStore.LOG.info("metastore.compactor.worker.threads = {}",
          MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS));
      HiveMetaStore.LOG.info("hive.metastore.runworker.in = {}",
          MetastoreConf.getVar(configuration, MetastoreConf.ConfVars.HIVE_METASTORE_RUNWORKER_IN));
      HiveMetaStore.LOG.info("metastore.compactor.history.retention.attempted = {}",
          MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_DID_NOT_INITIATE));
      HiveMetaStore.LOG.info("metastore.compactor.history.retention.failed = {}",
          MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_FAILED));
      HiveMetaStore.LOG.info("metastore.compactor.history.retention.succeeded = {}",
          MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.COMPACTOR_HISTORY_RETENTION_SUCCEEDED));
      HiveMetaStore.LOG.info("metastore.compactor.initiator.failed.compacts.threshold = {}",
          MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_FAILED_THRESHOLD));
      HiveMetaStore.LOG.info("metastore.compactor.enable.stats.compression",
          MetastoreConf.getBoolVar(configuration, MetastoreConf.ConfVars.COMPACTOR_MINOR_STATS_COMPRESSION));

      if (!MetastoreConf.getBoolVar(configuration, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON)) {
        HiveMetaStore.LOG.warn("Compactor Initiator is turned Off. Automatic compaction will not be triggered.");
      }
      if (!MetastoreConf.getBoolVar(configuration, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON)) {
        HiveMetaStore.LOG.warn("Compactor Cleaner is turned Off. Automatic compaction cleaner will not be triggered.");
      }

    } else {
      int numThreads = MetastoreConf.getIntVar(configuration, MetastoreConf.ConfVars.COMPACTOR_WORKER_THREADS);
      if (numThreads < 1) {
        HiveMetaStore.LOG.warn("Invalid number of Compactor Worker threads({}) on HMS", numThreads);
      }
    }
  }

  @Override
  public void takeLeadership(LeaderElection election) throws Exception {
    if (metastoreThreadsMap != null) {
      throw new IllegalStateException("There should be no running tasks before taking the leadership!");
    }

    logCompactionParameters();
    metastoreThreadsMap = new IdentityHashMap<>();
    List<MetaStoreThread> metaStoreThreads = getCompactorThreads();
    for (MetaStoreThread thread : metaStoreThreads) {
      AtomicBoolean flag = new AtomicBoolean();
      thread.setConf(configuration);
      thread.init(flag);
      metastoreThreadsMap.put(thread, flag);
      HiveMetaStore.LOG.info("Starting metastore thread of type " + thread.getClass().getName());
      thread.start();
    }
  }

  @Override
  public void lossLeadership(LeaderElection election) {
    if (metastoreThreadsMap != null) {
      metastoreThreadsMap.forEach((thread, flag) -> {
        flag.set(true);
        if (thread instanceof Thread) {
          ((Thread)thread).interrupt();
        }
        HiveMetaStore.LOG.info("Stopped the Compaction task: {}.", thread.getClass().getName());
      });
      metastoreThreadsMap = null;
    }
  }

}
