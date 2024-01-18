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
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestLeaderElection {

  @Test
  public void testConfigLeaderElection() throws Exception {
    LeaderElection election = new StaticLeaderElection();
    String leaderHost = "host1.work";
    Configuration configuration = MetastoreConf.newMetastoreConf();
    election.tryBeLeader(configuration, leaderHost);
    // all given hosts should be leader
    assertTrue(election.isLeader());
    MetastoreConf.setVar(configuration,
        MetastoreConf.ConfVars.METASTORE_HOUSEKEEPING_LEADER_HOSTNAME, leaderHost);
    election.tryBeLeader(configuration, leaderHost);
    assertTrue(election.isLeader());
    // another host
    election.tryBeLeader(configuration, "host2.work");
    assertFalse(election.isLeader());
  }

  static class TestLeaderListener implements LeaderElection.LeadershipStateListener {
    AtomicBoolean flag;
    TestLeaderListener(AtomicBoolean flag) {
      this.flag = flag;
    }
    @Override
    public void takeLeadership(LeaderElection election) throws Exception {
      synchronized (flag) {
        flag.set(true);
        flag.notifyAll();
      }
    }

    @Override
    public void lossLeadership(LeaderElection election) throws Exception {
      synchronized (flag) {
        flag.set(false);
        flag.notifyAll();
      }
    }
  }

  @Test
  public void testLeaseLeaderElection() throws Exception {
    Configuration configuration = MetastoreConf.newMetastoreConf();
    MetastoreConf.setTimeVar(configuration,
        MetastoreConf.ConfVars.TXN_TIMEOUT, 3, TimeUnit.SECONDS);
    MetastoreConf.setTimeVar(configuration,
        MetastoreConf.ConfVars.LOCK_SLEEP_BETWEEN_RETRIES, 1, TimeUnit.SECONDS);
    MetastoreConf.setBoolVar(configuration, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    TestTxnDbUtil.setConfValues(configuration);
    TestTxnDbUtil.prepDb(configuration);
    TxnStore txnStore = TxnUtils.getTxnStore(configuration);

    configuration.setBoolean(LeaseLeaderElection.METASTORE_RENEW_LEASE, false);
    TableName mutex = new TableName("hive", "default", "leader_lease_ms");
    LeaseLeaderElection instance1 = new LeaseLeaderElection();
    AtomicBoolean flag1 = new AtomicBoolean(false);
    instance1.addStateListener(new TestLeaderListener(flag1));
    instance1.tryBeLeader(configuration, mutex);
    // elect1 as a leader now
    assertTrue(flag1.get() && instance1.isLeader());

    configuration.setBoolean(LeaseLeaderElection.METASTORE_RENEW_LEASE, true);
    LeaseLeaderElection instance2 = new LeaseLeaderElection();
    AtomicBoolean flag2 = new AtomicBoolean(false);
    instance2.addStateListener(new TestLeaderListener(flag2));
    instance2.tryBeLeader(configuration, mutex);
    // instance2 should not be leader as elect1 holds the lease
    assertFalse(flag2.get() || instance2.isLeader());

    ExecutorService service = Executors.newFixedThreadPool(4);
    wait(service, flag1, flag2);
    // now instance1 lease is timeout, the instance2 should be leader now
    assertTrue(instance2.isLeader() && flag2.get());
    assertFalse(flag1.get() || instance1.isLeader());
    assertTrue(flag2.get() && instance2.isLeader());

    // remove leader's lease (instance2)
    long lockId2 = instance2.getLockId();
    txnStore.unlock(new UnlockRequest(lockId2));
    wait(service, flag1, flag2);
    assertFalse(flag2.get() || instance2.isLeader());
    assertTrue(lockId2 > 0);
    assertFalse(instance2.getLockId() == lockId2);

    // remove leader's lease(instance1)
    long lockId1 = instance1.getLockId();
    txnStore.unlock(new UnlockRequest(lockId1));
    wait(service, flag1, flag2);
    assertFalse(lockId1 == instance1.getLockId());
    assertTrue(lockId1 > 0);

    for (int i = 0; i < 10; i++) {
      assertFalse(flag1.get() || instance1.isLeader());
      assertTrue(flag2.get() && instance2.isLeader());
      Thread.sleep(1 * 1000);
    }
  }

  private void wait(ExecutorService service, Object... obj) throws Exception {
    Future[] fs = new Future[obj.length];
    for (int i = 0; i < obj.length; i++) {
      Object monitor = obj[i];
      fs[i] = service.submit(() -> {
        try {
          synchronized (monitor) {
            monitor.wait();
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      });
    }
    for (Future f : fs) {
      f.get();
    }
  }

}
