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

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.GetReplicationMetricsRequest;
import org.apache.hadoop.hive.metastore.api.ReplicationMetricList;
import org.apache.hadoop.hive.metastore.api.ReplicationMetrics;
import org.apache.hadoop.hive.metastore.minihms.AbstractMetaStoreService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * TestReplicationMetrics for testing replication metrics.
 * persistence to HMS
 */
@RunWith(Parameterized.class)
@Category(MetastoreUnitTest.class)
public class TestReplicationMetrics extends MetaStoreClientTest {
  private final AbstractMetaStoreService metaStore;
  private IMetaStoreClient client;

  public TestReplicationMetrics(String name, AbstractMetaStoreService metaStore) throws Exception {
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

  @Test
  public void testAddMetrics() throws Exception {
    ObjectStore objStore = new ObjectStore();
    objStore.setConf(metaStore.getConf());
    objStore.deleteReplicationMetrics(0);
    ReplicationMetricList replicationMetricList = new ReplicationMetricList();
    List<ReplicationMetrics> replicationMetrics = new ArrayList<>();
    replicationMetrics.add(createReplicationMetric("repl1", 1L));
    replicationMetrics.add(createReplicationMetric("repl1", 2L));
    replicationMetricList.setReplicationMetricList(replicationMetrics);
    objStore.addReplicationMetrics(replicationMetricList);
    Thread.sleep(1000);
    replicationMetrics = new ArrayList<>();
    replicationMetrics.add(createReplicationMetric("repl2", 3L));
    replicationMetrics.add(createReplicationMetric("repl2", 4L));
    replicationMetricList.setReplicationMetricList(replicationMetrics);
    objStore.addReplicationMetrics(replicationMetricList);
    Thread.sleep(1000);

    GetReplicationMetricsRequest getReplicationMetricsRequest = new GetReplicationMetricsRequest();
    getReplicationMetricsRequest.setPolicy("repl1");
    ReplicationMetricList actualList = client.getReplicationMetrics(getReplicationMetricsRequest);
    assertEquals(2, actualList.getReplicationMetricListSize());
    List<ReplicationMetrics> actualMetrics = actualList.getReplicationMetricList();
    //Ordering should be descending
    ReplicationMetrics actualMetric0 = actualMetrics.get(0);
    assertEquals("repl1", actualMetric0.getPolicy());
    assertEquals(2L, actualMetric0.getScheduledExecutionId());
    assertEquals(1, actualMetric0.getDumpExecutionId());
    assertEquals("metadata", actualMetric0.getMetadata());
    assertEquals("progress", actualMetric0.getProgress());
    ReplicationMetrics actualMetric1 = actualMetrics.get(1);
    assertEquals("repl1", actualMetric1.getPolicy());
    assertEquals(1L, actualMetric1.getScheduledExecutionId());
    assertEquals(1, actualMetric1.getDumpExecutionId());
    assertEquals("metadata", actualMetric1.getMetadata());
    assertEquals("progress", actualMetric1.getProgress());

    getReplicationMetricsRequest = new GetReplicationMetricsRequest();
    getReplicationMetricsRequest.setPolicy("repl2");
    actualList = client.getReplicationMetrics(getReplicationMetricsRequest);
    assertEquals(2, actualList.getReplicationMetricListSize());
    actualMetrics = actualList.getReplicationMetricList();
    //Ordering should be descending
    actualMetric0 = actualMetrics.get(0);
    assertEquals("repl2", actualMetric0.getPolicy());
    assertEquals(4L, actualMetric0.getScheduledExecutionId());
    assertEquals(1, actualMetric0.getDumpExecutionId());
    assertEquals("metadata", actualMetric0.getMetadata());
    assertEquals("progress", actualMetric0.getProgress());
    actualMetric1 = actualMetrics.get(1);
    assertEquals("repl2", actualMetric1.getPolicy());
    assertEquals(3L, actualMetric1.getScheduledExecutionId());
    assertEquals(1, actualMetric1.getDumpExecutionId());
    assertEquals("metadata", actualMetric1.getMetadata());
    assertEquals("progress", actualMetric1.getProgress());
  }

  @Test
  public void testUpdateMetrics() throws Exception {
    ObjectStore objStore = new ObjectStore();
    objStore.setConf(metaStore.getConf());
    objStore.deleteReplicationMetrics(0);
    ReplicationMetricList replicationMetricList = new ReplicationMetricList();
    List<ReplicationMetrics> replicationMetrics = new ArrayList<>();
    replicationMetrics.add(createReplicationMetric("repl1", 1L));
    replicationMetrics.add(createReplicationMetric("repl1", 2L));
    replicationMetricList.setReplicationMetricList(replicationMetrics);
    objStore.addReplicationMetrics(replicationMetricList);
    Thread.sleep(1000);
    replicationMetrics = new ArrayList<>();
    replicationMetrics.add(createReplicationMetric("repl2", 3L));
    replicationMetrics.add(createReplicationMetric("repl2", 4L));
    replicationMetricList.setReplicationMetricList(replicationMetrics);
    objStore.addReplicationMetrics(replicationMetricList);
    Thread.sleep(1000);
    replicationMetrics = new ArrayList<>();
    replicationMetrics.add(updateReplicationMetric("repl2", 3L, "progress1"));
    replicationMetricList.setReplicationMetricList(replicationMetrics);
    objStore.addReplicationMetrics(replicationMetricList);
    Thread.sleep(1000);

    GetReplicationMetricsRequest getReplicationMetricsRequest = new GetReplicationMetricsRequest();
    getReplicationMetricsRequest.setPolicy("repl1");
    ReplicationMetricList actualList = client.getReplicationMetrics(getReplicationMetricsRequest);
    assertEquals(2, actualList.getReplicationMetricListSize());
    List<ReplicationMetrics> actualMetrics = actualList.getReplicationMetricList();
    //Ordering should be descending
    ReplicationMetrics actualMetric0 = actualMetrics.get(0);
    assertEquals("repl1", actualMetric0.getPolicy());
    assertEquals(2L, actualMetric0.getScheduledExecutionId());
    assertEquals(1, actualMetric0.getDumpExecutionId());
    assertEquals("metadata", actualMetric0.getMetadata());
    assertEquals("progress", actualMetric0.getProgress());

    ReplicationMetrics actualMetric1 = actualMetrics.get(1);
    assertEquals("repl1", actualMetric1.getPolicy());
    assertEquals(1L, actualMetric1.getScheduledExecutionId());
    assertEquals(1, actualMetric1.getDumpExecutionId());
    assertEquals("metadata", actualMetric1.getMetadata());
    assertEquals("progress", actualMetric1.getProgress());

    getReplicationMetricsRequest = new GetReplicationMetricsRequest();
    getReplicationMetricsRequest.setPolicy("repl2");
    actualList = client.getReplicationMetrics(getReplicationMetricsRequest);
    assertEquals(2, actualList.getReplicationMetricListSize());
    actualMetrics = actualList.getReplicationMetricList();
    //Ordering should be descending
    actualMetric0 = actualMetrics.get(0);
    assertEquals("repl2", actualMetric0.getPolicy());
    assertEquals(4L, actualMetric0.getScheduledExecutionId());
    assertEquals(1, actualMetric0.getDumpExecutionId());
    assertEquals("metadata", actualMetric0.getMetadata());
    assertEquals("progress", actualMetric0.getProgress());

    actualMetric1 = actualMetrics.get(1);
    assertEquals("repl2", actualMetric1.getPolicy());
    assertEquals(3L, actualMetric1.getScheduledExecutionId());
    assertEquals(1, actualMetric1.getDumpExecutionId());
    assertEquals("metadata", actualMetric1.getMetadata());
    assertEquals("progress1", actualMetric1.getProgress());
  }

  @Test
  public void testGetMetricsByScheduleId() throws Exception {
    ObjectStore objStore = new ObjectStore();
    objStore.setConf(metaStore.getConf());
    objStore.deleteReplicationMetrics(0);
    ReplicationMetricList replicationMetricList = new ReplicationMetricList();
    List<ReplicationMetrics> replicationMetrics = new ArrayList<>();
    replicationMetrics.add(createReplicationMetric("repl1", 1L));
    replicationMetrics.add(createReplicationMetric("repl1", 2L));
    replicationMetricList.setReplicationMetricList(replicationMetrics);
    objStore.addReplicationMetrics(replicationMetricList);
    Thread.sleep(1000);
    replicationMetrics = new ArrayList<>();
    replicationMetrics.add(createReplicationMetric("repl2", 3L));
    replicationMetrics.add(createReplicationMetric("repl2", 4L));
    replicationMetricList.setReplicationMetricList(replicationMetrics);
    objStore.addReplicationMetrics(replicationMetricList);
    Thread.sleep(1000);

    GetReplicationMetricsRequest getReplicationMetricsRequest = new GetReplicationMetricsRequest();
    getReplicationMetricsRequest.setScheduledExecutionId(1L);
    ReplicationMetricList actualList = client.getReplicationMetrics(getReplicationMetricsRequest);
    assertEquals(1, actualList.getReplicationMetricListSize());
    List<ReplicationMetrics> actualMetrics = actualList.getReplicationMetricList();
    //Ordering should be descending
    ReplicationMetrics actualMetric0 = actualMetrics.get(0);
    assertEquals("repl1", actualMetric0.getPolicy());
    assertEquals(1L, actualMetric0.getScheduledExecutionId());
    assertEquals(1, actualMetric0.getDumpExecutionId());
    assertEquals("metadata", actualMetric0.getMetadata());
    assertEquals("progress", actualMetric0.getProgress());

    //Update progress
    replicationMetrics = new ArrayList<>();
    replicationMetrics.add(updateReplicationMetric("repl1", 1L, "progress1"));
    replicationMetricList.setReplicationMetricList(replicationMetrics);
    objStore.addReplicationMetrics(replicationMetricList);
    Thread.sleep(1000);

    //get the metrics again
    getReplicationMetricsRequest = new GetReplicationMetricsRequest();
    getReplicationMetricsRequest.setScheduledExecutionId(1L);
    actualList = client.getReplicationMetrics(getReplicationMetricsRequest);
    assertEquals(1, actualList.getReplicationMetricListSize());
    actualMetrics = actualList.getReplicationMetricList();
    //Ordering should be descending
    actualMetric0 = actualMetrics.get(0);
    assertEquals("repl1", actualMetric0.getPolicy());
    assertEquals(1L, actualMetric0.getScheduledExecutionId());
    assertEquals(1, actualMetric0.getDumpExecutionId());
    assertEquals("metadata", actualMetric0.getMetadata());
    assertEquals("progress1", actualMetric0.getProgress());

  }

  @Test
  public void testDeleteMetrics() throws Exception {
    ObjectStore objStore = new ObjectStore();
    objStore.setConf(metaStore.getConf());
    objStore.deleteReplicationMetrics(0);
    ReplicationMetricList replicationMetricList = new ReplicationMetricList();
    List<ReplicationMetrics> replicationMetrics = new ArrayList<>();
    replicationMetrics.add(createReplicationMetric("repl1", 1L));
    replicationMetrics.add(createReplicationMetric("repl1", 2L));
    replicationMetricList.setReplicationMetricList(replicationMetrics);
    objStore.addReplicationMetrics(replicationMetricList);
    Thread.sleep(2000);
    replicationMetrics = new ArrayList<>();
    replicationMetrics.add(createReplicationMetric("repl1", 3L));
    replicationMetricList.setReplicationMetricList(replicationMetrics);
    objStore.addReplicationMetrics(replicationMetricList);
    Thread.sleep(500);

    GetReplicationMetricsRequest getReplicationMetricsRequest = new GetReplicationMetricsRequest();
    getReplicationMetricsRequest.setPolicy("repl1");
    ReplicationMetricList actualList = client.getReplicationMetrics(getReplicationMetricsRequest);
    assertEquals(3, actualList.getReplicationMetricListSize());
    //delete older metrics
    objStore.deleteReplicationMetrics(2);

    getReplicationMetricsRequest = new GetReplicationMetricsRequest();
    getReplicationMetricsRequest.setPolicy("repl1");
    actualList = client.getReplicationMetrics(getReplicationMetricsRequest);
    assertEquals(1, actualList.getReplicationMetricListSize());
    List<ReplicationMetrics> actualMetrics = actualList.getReplicationMetricList();
    ReplicationMetrics actualMetric0 = actualMetrics.get(0);
    assertEquals("repl1", actualMetric0.getPolicy());
    assertEquals(3L, actualMetric0.getScheduledExecutionId());
    assertEquals(1, actualMetric0.getDumpExecutionId());
    assertEquals("metadata", actualMetric0.getMetadata());
    assertEquals("progress", actualMetric0.getProgress());

  }

  private ReplicationMetrics createReplicationMetric(String policyName, Long scheduleId) {
    ReplicationMetrics replicationMetrics = new ReplicationMetrics();
    replicationMetrics.setPolicy(policyName);
    replicationMetrics.setScheduledExecutionId(scheduleId);
    replicationMetrics.setDumpExecutionId(1);
    replicationMetrics.setMetadata("metadata");
    replicationMetrics.setProgress("progress");
    return replicationMetrics;
  }

  private ReplicationMetrics updateReplicationMetric(String policyName, Long scheduleId, String progress) {
    ReplicationMetrics replicationMetrics = new ReplicationMetrics();
    replicationMetrics.setPolicy(policyName);
    replicationMetrics.setScheduledExecutionId(scheduleId);
    replicationMetrics.setProgress(progress);
    return replicationMetrics;
  }

}
