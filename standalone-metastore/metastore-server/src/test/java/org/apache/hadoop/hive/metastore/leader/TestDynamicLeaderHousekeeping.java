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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.leader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreThread;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.service.CompactionHouseKeeperService;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Category(MetastoreUnitTest.class)
public class TestDynamicLeaderHousekeeping {

  private HouseKeepingTasks houseKeepingTasks;
  private CompactorTasks compactorTasks;
  private static LeaseLeaderElection dynamicElection;

  @BeforeClass
  public static void setUp() throws Exception {
    dynamicElection = new LeaseLeaderElection();
    dynamicElection.setName("housekeeping test");
  }

  @After
  public void tearDown() throws Exception {
    if (houseKeepingTasks != null) {
      houseKeepingTasks.lossLeadership(dynamicElection);
      houseKeepingTasks = null;
    }
    compactorTasks = null;
  }

  @Test
  public void remoteTasksSkippedOnStaticLeader() throws Exception {
    Configuration conf = baseConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_THREADS_ON, false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TASK_THREADS_REMOTE_ONLY,
        NoOpMetastoreTaskThread.class.getName());

    houseKeepingTasks = new HouseKeepingTasks(conf, true);
    Assert.assertTrue(houseKeepingTasks.getRemoteOnlyTasks().isEmpty());
  }

  @Test
  public void remoteTasksScheduledOnDynamicLeader() throws Exception {
    Configuration conf = baseConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_THREADS_ON, false);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, false);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.TASK_THREADS_REMOTE_ONLY,
        NoOpMetastoreTaskThread.class.getName() + "," + CompactionHouseKeeperService.class.getName());
    MetastoreConf.setLongVar(conf, MetastoreConf.ConfVars.THREAD_POOL_SIZE, 1);

    houseKeepingTasks = new HouseKeepingTasks(conf, true);
    StaticLeaderElection staticElection = new StaticLeaderElection();
    houseKeepingTasks.takeLeadership(staticElection);
    List<MetastoreTaskThread> remoteOnlyTasks = houseKeepingTasks.getRemoteOnlyTasks();
    Assert.assertTrue(remoteOnlyTasks.isEmpty());
    Assert.assertFalse(houseKeepingTasks.getExecutorService().isShutdown());
    houseKeepingTasks.lossLeadership(staticElection);
    Assert.assertNull(houseKeepingTasks.getExecutorService());

    houseKeepingTasks.takeLeadership(dynamicElection);
    remoteOnlyTasks = houseKeepingTasks.getRemoteOnlyTasks();
    Assert.assertEquals(2, remoteOnlyTasks.size());
    Assert.assertFalse(houseKeepingTasks.getExecutorService().isShutdown());
  }

  @Test
  public void compactorOnDynamicLeader() throws Exception {
    Configuration conf = baseConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_INITIATOR_ON, false);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.COMPACTOR_CLEANER_ON, false);

    compactorTasks = new CompactorTasks(conf);
    StaticLeaderElection staticElection = new StaticLeaderElection();
    compactorTasks.takeLeadership(staticElection);
    Map<MetaStoreThread, AtomicBoolean> metastoreThreadsMap = compactorTasks.getMetastoreThreadsMap();
    Assert.assertTrue(metastoreThreadsMap.isEmpty());
    compactorTasks.lossLeadership(staticElection);
    metastoreThreadsMap = compactorTasks.getMetastoreThreadsMap();
    Assert.assertNull(metastoreThreadsMap);

    ClassNotFoundException cnfe = Assert.assertThrows(ClassNotFoundException.class,
        () -> compactorTasks.takeLeadership(dynamicElection));
    // Error in starting the Initiator
    Assert.assertTrue(cnfe.getMessage().contains("org.apache.hadoop.hive.ql.txn.compactor.Initiator"));
  }

  private static Configuration baseConf() {
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METASTORE_SUPPORT_ACID, true);
    return conf;
  }

  public static class NoOpMetastoreTaskThread implements MetastoreTaskThread {
    @Override
    public void setConf(Configuration conf) {
    }

    @Override
    public Configuration getConf() {
      return null;
    }

    @Override
    public void run() {
    }

    @Override
    public long runFrequency(TimeUnit unit) {
      return unit.convert(1, TimeUnit.HOURS);
    }
  }
}
