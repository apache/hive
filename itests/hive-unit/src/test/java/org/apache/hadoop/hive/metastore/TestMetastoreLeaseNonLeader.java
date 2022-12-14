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
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.leader.LeaderElection;
import org.apache.hadoop.hive.metastore.leader.LeaderElectionContext;
import org.apache.hadoop.hive.metastore.leader.LeaseLeaderElection;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TestMetastoreLeaseNonLeader {

  LeaderElection election;

  TestMetastoreHousekeepingLeader hms;

  @Before
  public void setUp() throws Exception {
    Configuration conf = MetastoreConf.newMetastoreConf();
    TestTxnDbUtil.setConfValues(conf);
    TestTxnDbUtil.prepDb(conf);
    election = new LeaseLeaderElection();
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_ELECTION, "lock");
    TableName tableName = (TableName) LeaderElectionContext.getLeaderMutex(conf,
        LeaderElectionContext.TTYPE.HOUSEKEEPING, null);
    election.tryBeLeader(conf, tableName);
    assertTrue("The elector should hold the lease now", election.isLeader());
    // start the non-leader hms now
    hms = new TestMetastoreHousekeepingLeader();
    MetastoreConf.setTimeVar(hms.conf, MetastoreConf.ConfVars.LOCK_SLEEP_BETWEEN_RETRIES, 1, TimeUnit.SECONDS);
    hms.conf.setBoolean(LeaderElectionContext.LEADER_IN_TEST, true);
    hms.internalSetup("", false);
  }

  @Test
  public void testHouseKeepingThreads() throws Exception {
    try {
      hms.testHouseKeepingThreadExistence();
      throw new IllegalStateException("HMS shouldn't start any housekeeping tasks");
    } catch (AssertionError e) {
      // expected
    }
    // elector releases the lease
    election.close();
    Thread.sleep(10 * 1000);
    // housing threads are here now as the hms wins the election
    hms.testHouseKeepingThreadExistence();
  }

}
