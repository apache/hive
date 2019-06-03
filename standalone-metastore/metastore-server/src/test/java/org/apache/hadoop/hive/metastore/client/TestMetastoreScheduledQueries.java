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
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequestType;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category(MetastoreUnitTest.class)
public class TestMetastoreScheduledQueries extends MetaStoreClientTest {
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  public TestMetastoreScheduledQueries(String name, AbstractMetaStoreService metaStore) throws Exception {
    metaStore.getConf().set("scheduled.queries.cron.syntax", "QUARTZ");
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

    ScheduledQuery schq = createScheduledQuery("create");
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.INSERT);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);

    ScheduledQuery schq2 = client.getScheduledQuery(new ScheduledQueryKey("create", "c1"));

    // next execution is set by remote
    schq.setNextExecution(schq2.getNextExecution());
    assertEquals(schq2, schq);
  }

  @Test(expected = InvalidInputException.class)
  public void testCreateWithInvalidSchedule() throws Exception {
    ScheduledQuery schq = createScheduledQuery("createInvalidSch");
    schq.setSchedule("asd asd");
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.INSERT);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);
  }

  @Test(expected = AlreadyExistsException.class)
  public void testDuplicateCreate() throws Exception {
    ScheduledQuery schq = createScheduledQuery("duplicate");
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.INSERT);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);
    client.scheduledQueryMaintenance(r);

  }

  @Test
  public void testUpdate() throws Exception {
    ScheduledQuery schq = createScheduledQuery("update");
    schq.getScheduleKey().setClusterNamespace("ns1");
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.INSERT);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);

    r.setType(ScheduledQueryMaintenanceRequestType.UPDATE);
    ScheduledQuery schq2 = createScheduledQuery2("update");
    schq2.getScheduleKey().setClusterNamespace("ns1");
    r.setScheduledQuery(schq2);
    client.scheduledQueryMaintenance(r);

    ScheduledQuery schq3 = client.getScheduledQuery(new ScheduledQueryKey("update", "c1"));

    // next execution is set by remote
    schq2.setNextExecution(schq3.getNextExecution());
    assertEquals(schq2, schq3);
  }

  @Test(expected = NoSuchObjectException.class)
  public void testDeleteNonExistent() throws Exception {
    ScheduledQuery schq = createScheduledQuery("delnonexist");
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.DELETE);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);
  }

  @Test
  public void testPoll() throws Exception {
    ScheduledQuery schq = createScheduledQuery(new ScheduledQueryKey("q1", "polltest"));
    ScheduledQueryMaintenanceRequest r = new ScheduledQueryMaintenanceRequest();
    r.setType(ScheduledQueryMaintenanceRequestType.INSERT);
    r.setScheduledQuery(schq);
    client.scheduledQueryMaintenance(r);

    schq.setScheduleKey(new ScheduledQueryKey("q1", "polltestOther"));
    client.scheduledQueryMaintenance(r);

    ScheduledQueryPollRequest request = new ScheduledQueryPollRequest();
    request.setClusterNamespace("polltest");

    ScheduledQueryPollResponse pollResult = null;
    for (int i = 0; i < 10; i++) {
      pollResult = client.scheduledQueryPoll(request);
      if (pollResult.isSetQuery()) {
        break;
      }
      System.out.println(pollResult);
      Thread.sleep(1000);
    }
    assertTrue(pollResult.isSetQuery());
    assertTrue(pollResult.isSetScheduleKey());

    request.setClusterNamespace("polltestSomethingElse");
    pollResult = client.scheduledQueryPoll(request);
    assertFalse(pollResult.isSetQuery());

    //    assertTrue(pollResult.isSetExecutionId());

  }

  private ScheduledQuery createScheduledQuery(String name) {
    return createScheduledQuery(createKey(name, "c1"));
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

  private ScheduledQuery createScheduledQuery2(String name) {
    ScheduledQuery schq = new ScheduledQuery();
    schq.setScheduleKey(createKey(name, "c222"));
    schq.setEnabled(true);
    schq.setSchedule("* * * 22 * * *");
    schq.setUser("user22");
    schq.setQuery("select 12");
    return schq;
  }

}
