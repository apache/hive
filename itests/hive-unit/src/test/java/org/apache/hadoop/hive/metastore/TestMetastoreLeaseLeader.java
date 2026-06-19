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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.leader.LeaderElectionContext;
import org.apache.hadoop.hive.metastore.leader.LeaderElectionFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestMetastoreLeaseLeader extends MetastoreHousekeepingLeaderTestBase {

  CombinedLeaderElector elector;

  @Before
  public void setUp() throws Exception {
    Configuration configuration = MetastoreConf.newMetastoreConf();
    MetastoreConf.setTimeVar(configuration, MetastoreConf.ConfVars.TXN_TIMEOUT, 3, TimeUnit.SECONDS);
    MetastoreConf.setTimeVar(configuration, MetastoreConf.ConfVars.LOCK_SLEEP_BETWEEN_RETRIES, 200, TimeUnit.MILLISECONDS);
    MetastoreConf.setLongVar(configuration, MetastoreConf.ConfVars.HMS_HANDLER_ATTEMPTS, 3);
    MetastoreConf.setTimeVar(configuration, MetastoreConf.ConfVars.HMS_HANDLER_INTERVAL, 100, TimeUnit.MILLISECONDS);
    configuration.set("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    LeaderElectionFactory.addElectionCreator(LeaderElectionFactory.Method.LOCK, conf -> new ReleaseAndRequireLease(conf, false));
    setup(null, configuration);

    Configuration conf = new Configuration(configuration);
    elector = new CombinedLeaderElector(conf);
    elector.setName("TestMetastoreLeaseLeader");
    elector.tryBeLeader();
  }

  @Test
  public void testHouseKeepingThreads() throws Exception {
    int size = LeaderElectionContext.TTYPE.values().length;
    CountDownLatch latch = new CountDownLatch(size);
    MetastoreHousekeepingLeaderTestBase.ReleaseAndRequireLease.setMonitor(latch);
    // hms is the leader now
    checkHouseKeepingThreadExistence(true);
    assertFalse(elector.isLeader());
    latch.await();
    // the lease of hms is timeout, the elector becomes leader now
    assertTrue(elector.isLeader());
    // hms should shut down all housekeeping tasks
    checkHouseKeepingThreadExistence(false);

    latch = new CountDownLatch(size);
    MetastoreHousekeepingLeaderTestBase.ReleaseAndRequireLease.setMonitor(latch);
    elector.close();
    latch.await();
    // hms becomes leader again
    checkHouseKeepingThreadExistence(true);
  }

  @After
  public void afterTest() {
    MetastoreHousekeepingLeaderTestBase.ReleaseAndRequireLease.reset();
  }

}
