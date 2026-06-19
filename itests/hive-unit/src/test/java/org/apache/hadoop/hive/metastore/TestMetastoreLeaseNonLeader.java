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
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TestMetastoreLeaseNonLeader extends MetastoreHousekeepingLeaderTestBase {

  CombinedLeaderElector elector;

  @Before
  public void setUp() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);
    elector = new CombinedLeaderElector(conf);
    elector.setName("TestMetastoreLeaseNonLeader");
    elector.tryBeLeader();
    assertTrue("The elector should hold the lease now", elector.isLeader());
    // start the non-leader hms now
    Configuration configuration = new Configuration(conf);
    MetastoreConf.setTimeVar(configuration, MetastoreConf.ConfVars.LOCK_SLEEP_BETWEEN_RETRIES, 100, TimeUnit.MILLISECONDS);
    LeaderElectionFactory.addElectionCreator(LeaderElectionFactory.Method.LOCK,  c -> new ReleaseAndRequireLease(c, true));
    setup(null, configuration);
  }

  @Test
  public void testHouseKeepingThreads() throws Exception {
    checkHouseKeepingThreadExistence(false);
    // elector releases the lease
    CountDownLatch latch = new CountDownLatch(LeaderElectionContext.TTYPE.values().length);
    MetastoreHousekeepingLeaderTestBase.ReleaseAndRequireLease.setMonitor(latch);
    elector.close();
    latch.await();
    // housing threads are here now as the hms wins the election
    checkHouseKeepingThreadExistence(true);
  }

  @After
  public void afterTest() {
    MetastoreHousekeepingLeaderTestBase.ReleaseAndRequireLease.reset();
  }

}
