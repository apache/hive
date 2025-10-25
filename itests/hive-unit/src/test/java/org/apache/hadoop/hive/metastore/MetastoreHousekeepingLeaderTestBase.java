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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.leader.LeaderElection;
import org.apache.hadoop.hive.metastore.leader.LeaderElectionContext;
import org.apache.hadoop.hive.metastore.leader.LeaseLeaderElection;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.ql.stats.StatsUpdaterThread;
import org.apache.hadoop.hive.ql.txn.compactor.Cleaner;
import org.apache.hadoop.hive.ql.txn.compactor.Initiator;
import org.apache.hadoop.hive.ql.txn.compactor.Worker;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Base class for HMS leader config testing.
 */
abstract class MetastoreHousekeepingLeaderTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreHousekeepingLeaderTestBase.class);
  private static HiveMetaStoreClient client;
  protected Configuration conf;
  private static Warehouse warehouse;
  private static boolean isServerStarted = false;
  private static int port;
  private static MiniDFSCluster miniDFS;
  // Threads using ThreadPool will start after the configured interval. Start them right away
  private static final long REMOTE_TASKS_INTERVAL = 1;
  static final String METASTORE_THREAD_TASK_FREQ_CONF = "metastore.leader.test.task.freq";

  static Map<String, Boolean> threadNames = new HashMap<>();
  static Map<Class<? extends Thread>, Boolean> threadClasses = new HashMap<>();

  void setup(final String leaderHostName, Configuration configuration) throws Exception {
    this.conf = configuration;
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_BIND_HOST, "localhost");
    MetastoreConf.setVar(conf, ConfVars.METASTORE_HOUSEKEEPING_LEADER_ELECTION,
        leaderHostName != null ? "host" : "lock");
    if (leaderHostName != null) {
      MetastoreConf.setVar(conf, ConfVars.METASTORE_HOUSEKEEPING_LEADER_HOSTNAME, leaderHostName);
    }
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, true);

    addHouseKeepingThreadConfigs();

    warehouse = new Warehouse(conf);

    if (isServerStarted) {
      Assert.assertNotNull("Unable to connect to the MetaStore server", client);
      return;
    }
    // Start the metastore and wait for the background threads
    port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(),
            conf, false, false,  true, true);
    System.out.println("Starting MetaStore Server on port " + port);
    isServerStarted = true;

    // If the client connects the metastore service has started. This is used as a signal to
    // start tests.
    client = createClient();
  }

  private HiveMetaStoreClient createClient() throws Exception {
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    MetastoreConf.setBoolVar(conf, ConfVars.EXECUTE_SET_UGI, false);
    return new HiveMetaStoreClient(conf);
  }

  private void addHouseKeepingThreadConfigs() throws Exception {
    conf.setTimeDuration(METASTORE_THREAD_TASK_FREQ_CONF, REMOTE_TASKS_INTERVAL,
                          TimeUnit.MILLISECONDS);
    addStatsUpdaterThreadConfigs();
    addReplChangeManagerConfigs();
    addCompactorConfigs();
    long numTasks = addRemoteOnlyTasksConfigs();
    numTasks = numTasks + addAlwaysTasksConfigs();
    MetastoreConf.setLongVar(conf, ConfVars.THREAD_POOL_SIZE, numTasks);
  }
  private void addStatsUpdaterThreadConfigs() {
    MetastoreConf.setLongVar(conf, ConfVars.STATS_AUTO_UPDATE_WORKER_COUNT, 1);
    MetastoreConf.setVar(conf, ConfVars.STATS_AUTO_UPDATE, "all");
    threadClasses.put(StatsUpdaterThread.class, false);
    threadNames.put(StatsUpdaterThread.WORKER_NAME_PREFIX, false);
  }

  private void addReplChangeManagerConfigs() throws Exception {
    miniDFS = new MiniDFSCluster.Builder(new Configuration()).numDataNodes(1).format(true).build();
    MetastoreConf.setBoolVar(conf, ConfVars.REPLCMENABLED, true);
    String cmroot = "hdfs://" + miniDFS.getNameNode().getHostAndPort() + "/cmroot";
    MetastoreConf.setVar(conf, ConfVars.REPLCMDIR, cmroot);
    MetastoreConf.setVar(conf, ConfVars.REPLCMFALLBACKNONENCRYPTEDDIR, cmroot);
    threadNames.put(ReplChangeManager.CM_THREAD_NAME_PREFIX,  false);
  }

  private void addCompactorConfigs() {
    MetastoreConf.setBoolVar(conf, ConfVars.COMPACTOR_INITIATOR_ON, true);
    MetastoreConf.setBoolVar(conf, ConfVars.COMPACTOR_CLEANER_ON, true);
    MetastoreConf.setLongVar(conf, ConfVars.COMPACTOR_WORKER_THREADS, 1);
    threadClasses.put(Initiator.class, false);
    threadClasses.put(Cleaner.class, false);
  }

  private long addRemoteOnlyTasksConfigs() {
    String remoteTaskClassPaths =
            RemoteMetastoreTaskThreadTestImpl1.class.getCanonicalName() + "," +
                    RemoteMetastoreTaskThreadTestImpl2.class.getCanonicalName();

    MetastoreConf.setBoolVar(conf, ConfVars.METASTORE_HOUSEKEEPING_THREADS_ON, true);
    MetastoreConf.setVar(conf, ConfVars.TASK_THREADS_REMOTE_ONLY, remoteTaskClassPaths);

    threadNames.put(RemoteMetastoreTaskThreadTestImpl1.TASK_NAME, false);
    threadNames.put(RemoteMetastoreTaskThreadTestImpl2.TASK_NAME, false);

    return 2;
  }

  private long addAlwaysTasksConfigs() {
    String alwaysTaskClassPaths = MetastoreTaskThreadAlwaysTestImpl.class.getCanonicalName();
    MetastoreConf.setVar(conf, ConfVars.TASK_THREADS_ALWAYS, alwaysTaskClassPaths);
    threadNames.put(MetastoreTaskThreadAlwaysTestImpl.TASK_NAME, false);
    return 1;
  }

  private static String getAllThreadsAsString() {
    Map<Thread, StackTraceElement[]> threadStacks = Thread.getAllStackTraces();
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Thread, StackTraceElement[]> entry : threadStacks.entrySet()) {
      Thread t = entry.getKey();
      sb.append(System.lineSeparator());
      sb.append("Name: ").append(t.getName()).append(" State: ").append(t.getState())
              .append(" Class name: ").append(t.getClass().getCanonicalName());
    }
    return sb.toString();
  }

  void searchHousekeepingThreads() throws Exception {
    resetThreadStatus();
    // Client has been created so the metastore has started serving and started the background threads
    LOG.info(getAllThreadsAsString());

    // Check if all the housekeeping threads have been started.
    Set<Thread> threads = Thread.getAllStackTraces().keySet();
    for (Thread thread : threads) {
      // All house keeping threads should be alive.
      if (!thread.isAlive()) {
        continue;
      }

      // Account for the threads identifiable by their classes.
      if (threadClasses.get(thread.getClass()) != null) {
        threadClasses.put(thread.getClass(), true);
      }

      // Account for the threads identifiable by their names
      String threadName = thread.getName();
      if (threadName == null) {
        continue;
      }

      if (threadName.startsWith(StatsUpdaterThread.WORKER_NAME_PREFIX)) {
        threadNames.put(StatsUpdaterThread.WORKER_NAME_PREFIX, true);
      } else if (threadName.startsWith(ReplChangeManager.CM_THREAD_NAME_PREFIX)) {
        threadNames.put(ReplChangeManager.CM_THREAD_NAME_PREFIX, true);
      } else if (threadNames.get(threadName) != null) {
        threadNames.put(threadName, true);
      }
    }
  }

  private void resetThreadStatus() {
    threadNames.forEach((name, status) -> threadNames.put(name, false));
    threadClasses.forEach((thread, status) -> threadClasses.put(thread, false));
  }

  static class CombinedLeaderElector implements AutoCloseable {
    List<Pair<TableName, LeaderElection<TableName>>> elections = new ArrayList<>();
    private final Configuration configuration;
    private String name;

    CombinedLeaderElector(Configuration conf) throws IOException {
      this.configuration = conf;
      for (LeaderElectionContext.TTYPE type : LeaderElectionContext.TTYPE.values()) {
        TableName table = type.getTableName();
        elections.add(Pair.of(table, new LeaseLeaderElection()));
      }
    }

    public void tryBeLeader() throws Exception {
      int i = 0;
      for (Pair<TableName, LeaderElection<TableName>> election : elections) {
        LeaderElection<TableName> le = election.getRight();
        le.setName(name + "-" + i++);
        le.tryBeLeader(configuration, election.getLeft());
      }
    }

    public boolean isLeader() {
      boolean isLeader = true;
      for (Pair<TableName, LeaderElection<TableName>> election : elections) {
        isLeader &= election.getRight().isLeader();
      }
      return isLeader;
    }

    public void setName(String name) {
      this.name = name;
    }

    @Override
    public void close() throws Exception {
      for (Pair<TableName, LeaderElection<TableName>> election : elections) {
        election.getRight().close();
      }
    }
  }

  static class ReleaseAndRequireLease extends LeaseLeaderElection {
    private static CountDownLatch latch;
    private final Configuration configuration;
    private final boolean needRenewLease;
    private TableName tableName;

    public static void setMonitor(CountDownLatch latch) {
      ReleaseAndRequireLease.latch = latch;
    }
    public static void reset() {
      ReleaseAndRequireLease.latch = null;
    }

    public ReleaseAndRequireLease(Configuration conf, boolean needRenewLease) throws IOException {
      super();
      this.configuration = conf;
      this.needRenewLease = needRenewLease;
    }

    @Override
    public void setName(String name) {
      super.setName(name);
      LeaderElectionContext.TTYPE type = null;
      for (LeaderElectionContext.TTYPE value : LeaderElectionContext.TTYPE.values()) {
        if (value.getName().equalsIgnoreCase(name)) {
          type = value;
          break;
        }
      }
      if (type == null) {
        // This shouldn't happen at all
        throw new AssertionError("Unknown elector name: " + name);
      }
      this.tableName = type.getTableName();
    }

    @Override
    protected void notifyListener() {
      super.notifyListener();
      if (isLeader) {
        if (!needRenewLease) {
          super.shutdownWatcher();
          // In our tests, the time spent on notifying the listener might be greater than the lease timeout,
          // which makes the leader loss the leadership quickly after wake up, and kill all housekeeping services.
          // Make sure the leader is still valid while notifying the listener, and switch to ReleaseAndRequireWatcher
          // after all listeners finish their work.
          heartbeater = new ReleaseAndRequireWatcher(configuration, tableName);
          heartbeater.startWatch();
        }
      } else {
        try {
          // This is the last one get notified, sleep some time to make sure all other
          // services have been stopped before return
          Thread.sleep(12000);
        } catch (InterruptedException ignore) {
        }
      }
      if (latch != null) {
        latch.countDown();
      }
    }

    // For testing purpose only, lock would become timeout and then acquire it again
    private class ReleaseAndRequireWatcher extends LeaseWatcher {
      long timeout;
      public ReleaseAndRequireWatcher(Configuration conf,
          TableName tableName) {
        super(conf, tableName);
        timeout = MetastoreConf.getTimeVar(conf,
            MetastoreConf.ConfVars.TXN_TIMEOUT, TimeUnit.MILLISECONDS) + 3000;
        setName("ReleaseAndRequireWatcher-" + ((name != null) ? name + "-" : "") + ID.incrementAndGet());
      }

      @Override
      public void beforeRun() {
        try {
          Thread.sleep(timeout);
        } catch (InterruptedException e) {
          // ignore this
        }
      }

      @Override
      public void runInternal() {
        shutDown();
        // The timeout lock should be cleaned,
        // sleep some time to let others take the chance to become the leader
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          // ignore
        }
        // Acquire the lock again
        conf = new Configuration(conf);
        reclaim();
      }
    }
  }

  public void checkHouseKeepingThreadExistence(boolean isLeader) throws Exception {
    searchHousekeepingThreads();

    // Verify existence of threads
    for (Map.Entry<String, Boolean> entry : threadNames.entrySet()) {
      if (entry.getValue()) {
        LOG.info("Found thread with name {}", entry.getKey());
      } else {
        LOG.info("No thread found with name {}", entry.getKey());
      }
      if (isLeader) {
        Assert.assertTrue("No thread with name " + entry.getKey() + " found.", entry.getValue());
      } else {
        Assert.assertFalse("Thread with name " + entry.getKey() + " found.", entry.getValue());
      }
    }

    for (Map.Entry<Class<? extends Thread>, Boolean> entry : threadClasses.entrySet()) {
      if (isLeader) {
        if (entry.getValue()) {
          LOG.info("Found thread for {}", entry.getKey().getSimpleName());
        }
        Assert.assertTrue("No thread found for class " + entry.getKey().getSimpleName(), entry.getValue());
      } else {
        // A non-leader HMS will still run the configured number of Compaction worker threads.
        if (entry.getKey() == Worker.class) {
          if (entry.getValue()) {
            LOG.info("Thread found for " + entry.getKey().getSimpleName());
          }
        } else {
          if (!entry.getValue()) {
            LOG.info("No thread found for " + entry.getKey().getSimpleName());
          }
          Assert.assertFalse("Thread found for class " + entry.getKey().getSimpleName(),
              entry.getValue());
        }
      }
    }
  }
}

