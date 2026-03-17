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

import com.cronutils.utils.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.service.CompactionHouseKeeperService;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class HouseKeepingTasks implements LeaderElection.LeadershipStateListener {

  private final Configuration configuration;

  // shut down pool when new leader is selected
  private ScheduledExecutorService metastoreTaskThreadPool;

  private boolean runOnlyRemoteTasks;

  private List<MetastoreTaskThread> runningTasks;

  public HouseKeepingTasks(Configuration configuration, boolean runOnlyRemoteTasks) {
    this.configuration = new Configuration(requireNonNull(configuration,
        "configuration is null"));
    this.runOnlyRemoteTasks = runOnlyRemoteTasks;
  }

  /**
   * invoke setConf(Configuration conf) before running
   */
  public List<MetastoreTaskThread> getRemoteOnlyTasks() throws Exception {
    List<MetastoreTaskThread> remoteOnlyTasks = new ArrayList<>();
    if(!MetastoreConf.getBoolVar(configuration,
        MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_THREADS_ON)) {
      return remoteOnlyTasks;
    }
    boolean isCompactorEnabled = MetastoreConf.getBoolVar(configuration, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON)
            || MetastoreConf.getBoolVar(configuration, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON);

    Collection<String> taskNames =
        MetastoreConf.getStringCollection(configuration, MetastoreConf.ConfVars.TASK_THREADS_REMOTE_ONLY);
    for (String taskName : taskNames) {
      if (CompactionHouseKeeperService.class.getName().equals(taskName) && !isCompactorEnabled) {
        continue;
      }
      MetastoreTaskThread task =
          JavaUtils.newInstance(JavaUtils.getClass(taskName, MetastoreTaskThread.class));
      remoteOnlyTasks.add(task);
    }
    return remoteOnlyTasks;
  }

  // Copied from HiveMetaStore
  public List<MetastoreTaskThread> getAlwaysTasks() throws Exception {
    List<MetastoreTaskThread> alwaysTasks = new ArrayList<>();
    Collection<String> taskNames =
        MetastoreConf.getStringCollection(configuration, MetastoreConf.ConfVars.TASK_THREADS_ALWAYS);
    for (String taskName : taskNames) {
      MetastoreTaskThread task =
          JavaUtils.newInstance(JavaUtils.getClass(taskName, MetastoreTaskThread.class));
      alwaysTasks.add(task);
    }
    return alwaysTasks;
  }

  @Override
  public void takeLeadership(LeaderElection election) throws Exception {
    if (metastoreTaskThreadPool != null) {
      throw new IllegalStateException("There should be no running tasks before taking the leadership!");
    }
    runningTasks = new ArrayList<>();
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("Metastore Scheduled Worker(" + election.getName() + ") %d").build();
    final List<MetastoreTaskThread> tasks;
    if (!runOnlyRemoteTasks) {
      tasks = new ArrayList<>(getAlwaysTasks());
    } else {
      tasks = new ArrayList<>(getRemoteOnlyTasks());
    }
    int poolSize = Math.min(MetastoreConf.getIntVar(configuration,
        MetastoreConf.ConfVars.THREAD_POOL_SIZE), tasks.size());
    metastoreTaskThreadPool = Executors.newScheduledThreadPool(poolSize, threadFactory);
    for (MetastoreTaskThread task : tasks) {
      task.setConf(configuration);
      task.enforceMutex(election.enforceMutex());
      long freq = task.runFrequency(TimeUnit.MILLISECONDS);
      if (freq > 0) {
        runningTasks.add(task);
        metastoreTaskThreadPool.scheduleAtFixedRate(task, freq, freq, TimeUnit.MILLISECONDS);
      }
    }

    runningTasks.forEach(task -> {
      HiveMetaStore.LOG.info("Scheduling for " + task.getClass().getCanonicalName() + " service.");
    });
  }

  @Override
  public void lossLeadership(LeaderElection election) throws Exception {
    if (metastoreTaskThreadPool != null) {
      metastoreTaskThreadPool.shutdown();
      metastoreTaskThreadPool = null;
    }

    if (runningTasks != null && !runningTasks.isEmpty()) {
      runningTasks.forEach(task -> {
        HiveMetaStore.LOG.info("Stopped the Housekeeping task: {}", task.getClass().getCanonicalName());
      });
      runningTasks.clear();
    }
  }

  @VisibleForTesting
  public ScheduledExecutorService getExecutorService() {
    return metastoreTaskThreadPool;
  }
}
