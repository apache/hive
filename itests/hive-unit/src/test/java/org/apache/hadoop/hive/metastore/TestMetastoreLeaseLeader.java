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
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestMetastoreLeaseLeader {

  LeaderElection<TableName> election;

  TestMetastoreHousekeepingLeader hms;

  @Before
  public void setUp() throws Exception {
    Configuration configuration = MetastoreConf.newMetastoreConf();
    hms = new TestMetastoreHousekeepingLeader();
    MetastoreConf.setTimeVar(configuration, MetastoreConf.ConfVars.TXN_TIMEOUT, 1, TimeUnit.SECONDS);
    MetastoreConf.setTimeVar(configuration, MetastoreConf.ConfVars.LOCK_SLEEP_BETWEEN_RETRIES, 200, TimeUnit.MILLISECONDS);
    configuration.setBoolean(LeaseLeaderElection.METASTORE_RENEW_LEASE, false);
    configuration.setBoolean(LeaderElectionContext.LEADER_IN_TEST, true);
    configuration.set("hive.txn.manager", "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
    hms.internalSetup(null, configuration);

    Configuration conf = new Configuration(configuration);
    conf.setBoolean(LeaseLeaderElection.METASTORE_RENEW_LEASE, true);
    election = new LeaseLeaderElection();
    election.setName("TestMetastoreLeaseLeader");
    TableName tableName = (TableName) LeaderElectionContext.getLeaderMutex(conf,
        LeaderElectionContext.TTYPE.HOUSEKEEPING, null);
    election.tryBeLeader(conf, tableName);
  }

  @Test
  public void testHouseKeepingThreads() throws Exception {
    // hms is the leader now
    hms.testHouseKeepingThreadExistence();
    assertFalse(election.isLeader());
    Thread.sleep(15 * 1000);
    // the lease of hms is timeout, election becomes leader now
    assertTrue(election.isLeader());
    try {
      // hms should shutdown all housekeeping tasks
      hms.testHouseKeepingThreadExistence();
      throw new IllegalStateException("HMS should shutdown all housekeeping tasks");
    } catch (AssertionError e) {
      // expected
    }

    election.close();
    Thread.sleep(15 * 1000);
    // hms becomes leader again
    hms.testHouseKeepingThreadExistence();
  }

}
