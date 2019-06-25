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
package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.registry.AMRecord;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestTezExternalSessionsRegistryClient {

  @Test
  public void testBinPackComputeComparator() {
    PriorityQueue<AMRecord> amRecords =
      new PriorityQueue<>(new TezExternalSessionsRegistryClient.AMRecordBinPackingComputeComparator());
    // compute-1
    AMRecord c1Am0 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-1-0.compute-10000-abcd.svc.cluster.local", "192.168.1.1", 2222, "abcd", "compute-1");
    AMRecord c1Am1 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-1-1.compute-10000-abcd.svc.cluster.local", "192.168.1.2", 2222, "abcd", "compute-1");
    AMRecord c1Am2 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-1-2.compute-10000-abcd.svc.cluster.local", "192.168.1.3", 2222, "abcd", "compute-1");
    amRecords.add(c1Am0);
    assertEquals(amRecords.peek(), c1Am0);
    amRecords.add(c1Am1);
    assertEquals(amRecords.peek(), c1Am0);
    amRecords.add(c1Am2);
    assertEquals(amRecords.peek(), c1Am0);
    List<AMRecord> expected = Lists.newArrayList(c1Am0, c1Am1, c1Am2);
    List<AMRecord> got = Lists.newArrayList();
    int size = amRecords.size();
    for (int i = 0; i < size; i++) {
      got.add(amRecords.poll());
    }
    assertEquals(expected, got);

    // add the AMs back to amRecords since we poll'ed
    amRecords.add(c1Am0);
    amRecords.add(c1Am1);
    amRecords.add(c1Am2);

    // compute-0
    AMRecord c0Am0 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-0-0.compute-10000-abcd.svc.cluster.local", "192.168.1.1", 2222, "abcd", "compute-0");
    AMRecord c0Am1 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-0-1.compute-10000-abcd.svc.cluster.local", "192.168.1.2", 2222, "abcd", "compute-0");
    AMRecord c0Am2 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-0-2.compute-10000-abcd.svc.cluster.local", "192.168.1.3", 2222, "abcd", "compute-0");
    amRecords.add(c0Am1);
    assertEquals(amRecords.peek(), c0Am1);
    amRecords.add(c0Am2);
    assertEquals(amRecords.peek(), c0Am1);
    amRecords.add(c0Am0);
    assertEquals(amRecords.peek(), c0Am0);
    expected = Lists.newArrayList(c0Am0, c0Am1, c0Am2, c1Am0, c1Am1, c1Am2);
    got = Lists.newArrayList();
    size = amRecords.size();
    for (int i = 0; i < size; i++) {
      got.add(amRecords.poll());
    }
    assertEquals(expected, got);
  }

  @Test
  public void testRoundRobinComputeComparator() {
    PriorityQueue<AMRecord> amRecords =
      new PriorityQueue<>(new TezExternalSessionsRegistryClient.AMRecordRoundRobinComputeComparator());
    // compute-1
    AMRecord c1Am0 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-1-0.compute-10000-abcd.svc.cluster.local", "192.168.1.1", 2222, "abcd", "compute-1");
    AMRecord c1Am1 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-1-1.compute-10000-abcd.svc.cluster.local", "192.168.1.2", 2222, "abcd", "compute-1");
    AMRecord c1Am2 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-1-2.compute-10000-abcd.svc.cluster.local", "192.168.1.3", 2222, "abcd", "compute-1");
    amRecords.add(c1Am0);
    assertEquals(amRecords.peek(), c1Am0);
    amRecords.add(c1Am1);
    assertEquals(amRecords.peek(), c1Am0);
    amRecords.add(c1Am2);
    assertEquals(amRecords.peek(), c1Am0);
    List<AMRecord> expected = Lists.newArrayList(c1Am0, c1Am1, c1Am2);
    List<AMRecord> got = Lists.newArrayList();
    int size = amRecords.size();
    for (int i = 0; i < size; i++) {
      got.add(amRecords.poll());
    }
    assertEquals(expected, got);

    // add the AMs back to amRecords since we poll'ed
    amRecords.add(c1Am0);
    amRecords.add(c1Am1);
    amRecords.add(c1Am2);

    // compute-0
    AMRecord c0Am0 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-0-0.compute-10000-abcd.svc.cluster.local", "192.168.1.1", 2222, "abcd", "compute-0");
    AMRecord c0Am1 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-0-1.compute-10000-abcd.svc.cluster.local", "192.168.1.2", 2222, "abcd", "compute-0");
    AMRecord c0Am2 = new AMRecord(ApplicationId.newInstance(System.currentTimeMillis(), 0),
      "query-coordinator-0-2.compute-10000-abcd.svc.cluster.local", "192.168.1.3", 2222, "abcd", "compute-0");
    amRecords.add(c0Am1);
    assertEquals(amRecords.peek(), c1Am0);
    amRecords.add(c0Am2);
    assertEquals(amRecords.peek(), c1Am0);
    amRecords.add(c0Am0);
    assertEquals(amRecords.peek(), c0Am0);
    expected = Lists.newArrayList(c0Am0, c1Am0, c0Am1, c1Am1, c0Am2, c1Am2);
    got = Lists.newArrayList();
    size = amRecords.size();
    for (int i = 0; i < size; i++) {
      got.add(amRecords.poll());
    }
    assertEquals(expected, got);
  }

  @Test
  public void testRandomComputeComparator() {
    PriorityQueue<AMRecord> amRecords =
      new PriorityQueue<>(new TezExternalSessionsRegistryClient.AMRecordRandomComputeComparator());
    // compute-1
    AMRecord c1Am0 = new AMRecord(ApplicationId.newInstance(0, 0),
      "query-coordinator-1-0.compute-10000-abcd.svc.cluster.local", "192.168.1.1", 2222, "1", "compute-1");
    AMRecord c1Am1 = new AMRecord(ApplicationId.newInstance(0, 1),
      "query-coordinator-1-1.compute-10000-abcd.svc.cluster.local", "192.168.1.2", 2222, "2", "compute-1");
    AMRecord c1Am2 = new AMRecord(ApplicationId.newInstance(0, 2),
      "query-coordinator-1-2.compute-10000-abcd.svc.cluster.local", "192.168.1.3", 2222, "3", "compute-1");
    amRecords.add(c1Am0);
    amRecords.add(c1Am1);
    amRecords.add(c1Am2);
    AMRecord c0Am0 = new AMRecord(ApplicationId.newInstance(1, 0),
      "query-coordinator-0-0.compute-10000-abcd.svc.cluster.local", "192.168.1.1", 2222, "4", "compute-0");
    AMRecord c0Am1 = new AMRecord(ApplicationId.newInstance(1, 0),
      "query-coordinator-0-1.compute-10000-abcd.svc.cluster.local", "192.168.1.2", 2222, "5", "compute-0");
    AMRecord c0Am2 = new AMRecord(ApplicationId.newInstance(1, 0),
      "query-coordinator-0-2.compute-10000-abcd.svc.cluster.local", "192.168.1.3", 2222, "6", "compute-0");
    amRecords.add(c0Am1);
    amRecords.add(c0Am2);
    amRecords.add(c0Am0);
    List<AMRecord> expected = Lists.newArrayList(c0Am0, c1Am2, c1Am1, c1Am0, c0Am2, c0Am1);
    List<AMRecord> got = Lists.newArrayList();
    int size = amRecords.size();
    for (int i = 0; i < size; i++) {
      got.add(amRecords.poll());
    }
    assertEquals(expected, got);
  }

  @Test
  public void testDummyExternalSessionsRegistry() throws MetaException {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_REGISTRY_CLASS,
      DummyExternalSessionsRegistry.class.getName());
    conf.set("tez.am.registry.namespace", "dummy");
    ExternalSessionsRegistry externalSessionsRegistry = ExternalSessionsRegistry.getClient(conf);
    assertTrue(externalSessionsRegistry instanceof DummyExternalSessionsRegistry);
  }

  @Test
  public void testTezExternalSessionsRegistry() throws MetaException {
    HiveConf conf = new HiveConf();
    conf.set("tez.am.zookeeper.quorum", "test-quorum");
    conf.set("tez.am.registry.namespace", "tez");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_REGISTRY_CLASS,
      TezExternalSessionsRegistryClient.class.getName());
    ExternalSessionsRegistry externalSessionsRegistry = ExternalSessionsRegistry.getClient(conf);
    assertTrue(externalSessionsRegistry instanceof TezExternalSessionsRegistryClient);
  }
}
