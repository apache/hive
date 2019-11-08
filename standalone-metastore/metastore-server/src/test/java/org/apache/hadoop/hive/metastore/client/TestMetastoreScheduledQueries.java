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
package org.apache.hadoop.hive.metastore.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.jdo.JDOObjectNotFoundException;
import javax.jdo.PersistenceManager;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.ObjectStoreTestHook;
import org.apache.hadoop.hive.metastore.PersistenceManagerProvider;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.QueryState;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequestType;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.apache.hadoop.hive.metastore.model.MScheduledExecution;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Tests handling of scheduled queries related calls to the metastore.
 *
 * Checks wether expected state changes are being done to the HMS database.
 */
@RunWith(Parameterized.class)
@Category(MetastoreUnitTest.class)
public class TestMetastoreScheduledQueries extends MetaStoreClientTest {
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  public TestMetastoreScheduledQueries(String name, AbstractMetaStoreService metaStore) throws Exception {
    metaStore.getConf().set("scheduled.queries.progress.timeout", "3");
    this.metaStore = metaStore;
  }

  @Before
  public void setUp() throws Exception {
    client = metaStore.getClient();

  }

  @After
  public void tearDown() throws Exception {
    try {
      client.close();
    } catch (Exception e) {
      // HIVE-19729: Shallow the exceptions based on the discussion in the Jira
    }
    client = null;
  }

  @Test(expected = NoSuchObjectException.class)
  public void testNonExistent() throws Exception {
    client.getScheduledQuery(new ScheduledQueryKey("nonExistent", "x"));
  }

  @Test
  public void testCreate() throws Exception {

    ScheduledQuery schq = createScheduledQuery(createKey("create", "c1"));
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.CREATE);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);

    ScheduledQuery schq2 = client.getScheduledQuery(new ScheduledQueryKey("create", "c1"));

    // next execution is set by remote
    schq.setNextExecution(schq2.getNextExecution());
    assertEquals(schq2, schq);
  }

  @Test(expected = InvalidInputException.class)
  public void testCreateWithInvalidSchedule() throws Exception {
    ScheduledQuery schq = createScheduledQuery(createKey("createInvalidSch", "c1"));
    schq.setSchedule("asd asd");
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.CREATE);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);
  }

  @Test(expected = AlreadyExistsException.class)
  public void testDuplicateCreate() throws Exception {
    ScheduledQuery schq = createScheduledQuery(createKey("duplicate", "c1"));
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.CREATE);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);
    client.scheduledQueryMaintenance(r);

  }

  @Test
  public void testUpdate() throws Exception {
    ScheduledQuery schq = createScheduledQuery(createKey("update", "ns1"));
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.CREATE);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);

    r.setType(ScheduledQueryMaintenanceRequestType.ALTER);
    ScheduledQuery schq2 = createScheduledQuery2(createKey("update", "ns1"));
    schq2.getScheduleKey().setClusterNamespace("ns1");
    r.setScheduledQuery(schq2);
    client.scheduledQueryMaintenance(r);

    ScheduledQuery schq3 = client.getScheduledQuery(new ScheduledQueryKey("update", "ns1"));

    // next execution is set by remote
    schq2.setNextExecution(schq3.getNextExecution());
    assertEquals(schq2, schq3);
  }

  @Test
  public void testNormalDelete() throws Exception {
    ScheduledQuery schq = createScheduledQuery(createKey("q1", "nsdel"));
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.CREATE);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);
    r.setType(ScheduledQueryMaintenanceRequestType.DROP);
    client.scheduledQueryMaintenance(r);
  }

  @Test
  public void testNormalDeleteWithExec() throws Exception {
    String testCaseNS = "delwithexec";
    // insert
    ScheduledQuery schq = createScheduledQuery(createKey("del2", testCaseNS));
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.CREATE);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);

    // wait 2 sec to have the query exection
    Thread.sleep(2000);

    // invoke poll to create a dependent execution
    ScheduledQueryPollRequest pollRequest=new ScheduledQueryPollRequest(testCaseNS);
    client.scheduledQueryPoll(pollRequest);

    // delete scheduled query
    r.setType(ScheduledQueryMaintenanceRequestType.DROP);
    client.scheduledQueryMaintenance(r);

  }

  @Test(expected = NoSuchObjectException.class)
  public void testDeleteNonExistent() throws Exception {
    ScheduledQuery schq = createScheduledQuery(createKey("nonexistent", "nsdel"));
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.DROP);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);
  }

  @Test
  public void testExclusivePoll() throws Exception {
    try {
      ObjectStoreTestHook.instance = new ObjectStoreTestHook() {

        @Override
        public void scheduledQueryPoll() {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      };
      ScheduledQuery schq = createScheduledQuery(new ScheduledQueryKey("q1", "exclusive"));
      ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
      r.setType(ScheduledQueryMaintenanceRequestType.CREATE);
      r.setScheduledQuery(schq);
      client.scheduledQueryMaintenance(r);
      // wait 1 sec for next execution
      Thread.sleep(1000);

      ExecutorService pool = Executors.newCachedThreadPool();
      Future<ScheduledQueryPollResponse> f1 = pool.submit(new AsyncPollCall("exclusive"));
      Future<ScheduledQueryPollResponse> f2 = pool.submit(new AsyncPollCall("exclusive"));

      ScheduledQueryPollResponse resp1 = f1.get();
      ScheduledQueryPollResponse resp2 = f2.get();

      assertTrue(resp1.isSetQuery() ^ resp2.isSetQuery());

      pool.shutdown();
    } finally {
      ObjectStoreTestHook.instance = null;
    }

  }

  class AsyncPollCall implements Callable<ScheduledQueryPollResponse> {

    private String ns;

    AsyncPollCall(String string) {
      ns = string;
    }

    @Override
    public ScheduledQueryPollResponse call() throws Exception {
      IMetaStoreClient client1 = null;
      try {
        client1 = metaStore.getClient();
        ScheduledQueryPollRequest request = new ScheduledQueryPollRequest();
        request.setClusterNamespace(ns);
        ScheduledQueryPollResponse pollResult = null;
        pollResult = client1.scheduledQueryPoll(request);
        return pollResult;
      } catch (TException e) {
        throw new RuntimeException(e);
      } finally {
        if (client1 != null) {
          client1.close();
        }
      }

    }

  }

  @Test
  public void testPoll() throws Exception {
    ScheduledQuery schq = createScheduledQuery(new ScheduledQueryKey("q1", "polltest"));
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.CREATE);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);

    schq.setScheduleKey(new ScheduledQueryKey("q1", "polltestOther"));
    client.scheduledQueryMaintenance(r);

    // disabled queries are not considered
    schq.setScheduleKey(new ScheduledQueryKey("q2disabled", "polltest"));
    schq.setEnabled(false);
    client.scheduledQueryMaintenance(r);

    // do some poll requests; and wait for q1's execution
    ScheduledQueryPollRequest request = new ScheduledQueryPollRequest();
    request.setClusterNamespace("polltest");
    ScheduledQueryPollResponse pollResult = null;
    // wait for poll to hit
    for (int i = 0; i < 30; i++) {
      pollResult = client.scheduledQueryPoll(request);
      if (pollResult.isSetQuery()) {
        break;
      }
      Thread.sleep(100);
    }
    assertTrue(pollResult.isSetQuery());
    assertTrue(pollResult.isSetScheduleKey());
    assertTrue(pollResult.isSetExecutionId());
    // after reading the only scheduled query; there are no more queries to run (for 1 sec)
    ScheduledQueryPollResponse pollResult2 = client.scheduledQueryPoll(request);
    assertTrue(!pollResult2.isSetQuery());

    try (PersistenceManager pm = PersistenceManagerProvider.getPersistenceManager()) {
      MScheduledExecution q = pm.getObjectById(MScheduledExecution.class, pollResult.getExecutionId());
      assertNotNull(q);
      assertEquals(QueryState.INITED, q.getState());
      assertTrue(q.getStartTime() <= getEpochSeconds());
      assertTrue(q.getStartTime() >= getEpochSeconds() - 1);
      assertTrue(q.getEndTime() == null);
      assertTrue(q.getLastUpdateTime() <= getEpochSeconds());
      assertTrue(q.getLastUpdateTime() >= getEpochSeconds() - 1);
    }
    // wait 1 sec
    Thread.sleep(1000);

    ScheduledQueryProgressInfo info;
    info = new ScheduledQueryProgressInfo(
        pollResult.getExecutionId(), QueryState.EXECUTING, "executor-query-id");
    client.scheduledQueryProgress(info);

    try (PersistenceManager pm = PersistenceManagerProvider.getPersistenceManager()) {
      MScheduledExecution q = pm.getObjectById(MScheduledExecution.class, pollResult.getExecutionId());
      assertEquals(QueryState.EXECUTING, q.getState());
      assertEquals("executor-query-id", q.getExecutorQueryId());
      assertTrue(q.getLastUpdateTime() <= getEpochSeconds());
      assertTrue(q.getLastUpdateTime() >= getEpochSeconds() - 1);
    }

    // wait 1 sec
    Thread.sleep(1000);

    info = new ScheduledQueryProgressInfo(
        pollResult.getExecutionId(), QueryState.ERRORED, "executor-query-id");
    //    info.set
    client.scheduledQueryProgress(info);

    try (PersistenceManager pm = PersistenceManagerProvider.getPersistenceManager()) {
      MScheduledExecution q = pm.getObjectById(MScheduledExecution.class, pollResult.getExecutionId());
      assertEquals(QueryState.ERRORED, q.getState());
      assertEquals("executor-query-id", q.getExecutorQueryId());
      assertNull(q.getLastUpdateTime());
      assertTrue(q.getEndTime() <= getEpochSeconds());
      assertTrue(q.getEndTime() >= getEpochSeconds() - 1);
    }

    // clustername is taken into account; this should be empty
    request.setClusterNamespace("polltestSomethingElse");
    pollResult = client.scheduledQueryPoll(request);
    assertFalse(pollResult.isSetQuery());
  }

  @Test
  public void testCleanup() throws Exception {
    String namespace = "cleanup";
    ObjectStore objStore = new ObjectStore();
    objStore.setConf(metaStore.getConf());
    objStore.deleteScheduledExecutions(0);

    ScheduledQuery schq = createScheduledQuery(new ScheduledQueryKey("q1", namespace));
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.CREATE);
    r.setScheduledQuery(schq);
    objStore.scheduledQueryMaintenance(r);

    Thread.sleep(1000);
    ScheduledQueryPollRequest request = new ScheduledQueryPollRequest(namespace);
    ScheduledQueryPollResponse pollResult = objStore.scheduledQueryPoll(request);
    // will add q1 as a query being executed

    try (PersistenceManager pm = PersistenceManagerProvider.getPersistenceManager()) {
      MScheduledExecution q = pm.getObjectById(MScheduledExecution.class, pollResult.getExecutionId());
      assertEquals(QueryState.INITED, q.getState());
    }

    Thread.sleep(1000);
    objStore.deleteScheduledExecutions(0);

    try (PersistenceManager pm = PersistenceManagerProvider.getPersistenceManager()) {
      try {
        pm.getObjectById(MScheduledExecution.class, pollResult.getExecutionId());
        fail("The execution is expected to be deleted at this point...");
      }catch(JDOObjectNotFoundException e) {
        // expected
      }
    }
  }

  @Test
  public void testOutdatedCleanup() throws Exception {
    String namespace = "outdatedcleanup";
    ObjectStore objStore = new ObjectStore();
    objStore.setConf(metaStore.getConf());
    objStore.deleteScheduledExecutions(0);

    ScheduledQuery schq = createScheduledQuery(new ScheduledQueryKey("q1", namespace));
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.CREATE);
    r.setScheduledQuery(schq);
    objStore.scheduledQueryMaintenance(r);

    Thread.sleep(1000);
    ScheduledQueryPollRequest request = new ScheduledQueryPollRequest(namespace);
    ScheduledQueryPollResponse pollResult = objStore.scheduledQueryPoll(request);
    // will add q1 as a query being executed

    Thread.sleep(1000);
    objStore.markScheduledExecutionsTimedOut(0);

    try (PersistenceManager pm = PersistenceManagerProvider.getPersistenceManager()) {
      MScheduledExecution execution = pm.getObjectById(MScheduledExecution.class, pollResult.getExecutionId());
      assertEquals(QueryState.TIMED_OUT, execution.getState());
    }
  }

  private int getEpochSeconds() {
    return (int) (System.currentTimeMillis() / 1000);
  }

  private ScheduledQuery createScheduledQuery(ScheduledQueryKey key) {
    ScheduledQuery schq = new ScheduledQuery();
    schq.setScheduleKey(key);
    schq.setEnabled(true);
    schq.setSchedule("* * * * * ? *");
    schq.setUser("user");
    schq.setQuery("select 1");
    return schq;
  }

  private ScheduledQueryKey createKey(String name, String string) {
    ScheduledQueryKey ret = new ScheduledQueryKey();
    ret.setScheduleName(name);
    ret.setClusterNamespace(string);
    return ret;
  }

  private ScheduledQuery createScheduledQuery2(ScheduledQueryKey key) {
    ScheduledQuery schq = new ScheduledQuery();
    schq.setScheduleKey(key);
    schq.setEnabled(true);
    schq.setSchedule("* * * 22 * ? *");
    schq.setUser("user22");
    schq.setQuery("select 12");
    return schq;
  }

}
