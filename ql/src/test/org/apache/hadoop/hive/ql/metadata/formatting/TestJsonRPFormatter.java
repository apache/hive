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

package org.apache.hadoop.hive.ql.metadata.formatting;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMPoolTrigger;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlanStatus;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.show.formatter.JsonShowResourcePlanFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Test class for json resource plan formatter.
 */
public class TestJsonRPFormatter {
  private final JsonShowResourcePlanFormatter formatter = new JsonShowResourcePlanFormatter();

  private ByteArrayOutputStream bos;
  private DataOutputStream out;

  @Before
  public void setup() {
    bos = new ByteArrayOutputStream();
    out = new DataOutputStream(bos);
  }

  @After
  public void teardown() throws Exception {
    out.close();
    bos.close();
  }

  private WMFullResourcePlan createRP(String name, Integer parallelism, String defaultPoolPath) {
    WMResourcePlan rp = new WMResourcePlan(name);
    rp.setStatus(WMResourcePlanStatus.ACTIVE);
    if (parallelism != null) {
      rp.setQueryParallelism(parallelism);
    }
    if (defaultPoolPath != null) {
      rp.setDefaultPoolPath(defaultPoolPath);
    }
    WMFullResourcePlan fullRp = new WMFullResourcePlan(rp, new ArrayList<>());
    return fullRp;
  }

  private void addPool(WMFullResourcePlan fullRp, String poolName, double allocFraction,
      int parallelism, String policy) {
    WMPool pool = new WMPool(fullRp.getPlan().getName(), poolName);
    pool.setAllocFraction(allocFraction);
    pool.setQueryParallelism(parallelism);
    if (policy != null) {
      pool.setSchedulingPolicy(policy);
    }
    fullRp.addToPools(pool);
  }

  private void addTrigger(WMFullResourcePlan fullRp, String triggerName, String action,
      String expr, String poolName) {
    WMTrigger trigger = new WMTrigger(fullRp.getPlan().getName(), triggerName);
    trigger.setActionExpression(action);
    trigger.setTriggerExpression(expr);
    fullRp.addToTriggers(trigger);

    WMPoolTrigger pool2Trigger = new WMPoolTrigger(poolName, triggerName);
    fullRp.addToPoolTriggers(pool2Trigger);
  }

  private void addMapping(WMFullResourcePlan fullRp, String type, String name, String poolName) {
    WMMapping mapping = new WMMapping(fullRp.getPlan().getName(), type, name);
    mapping.setPoolPath(poolName);
    fullRp.addToMappings(mapping);
  }

  @Test
  public void testJsonEmptyRPFormatter() throws Exception {
    WMFullResourcePlan fullRp = createRP("test_rp_1", null, null);
    formatter.showFullResourcePlan(out, fullRp);
    out.flush();

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonTree = objectMapper.readTree(bos.toByteArray());

    assertNotNull(jsonTree);
    assertTrue(jsonTree.isObject());
    assertEquals("test_rp_1", jsonTree.get("name").asText());
    assertTrue(jsonTree.get("parallelism").isNull());
    assertTrue(jsonTree.get("defaultPool").isNull());
    assertTrue(jsonTree.get("pools").isArray());
    assertEquals(0, jsonTree.get("pools").size());
  }

  @Test
  public void testJsonRPFormatter() throws Exception {
    WMFullResourcePlan fullRp = createRP("test_rp_2", 10, "def");
    addPool(fullRp, "pool1", 0.3, 3, "fair");
    addTrigger(fullRp, "trigger1", "KILL", "BYTES > 2", "pool1");
    addPool(fullRp, "pool2", 0.7, 7, "fcfs");
    addMapping(fullRp, "user", "foo", "pool2");
    addMapping(fullRp, "user", "bar", "pool2");
    formatter.showFullResourcePlan(out, fullRp);
    out.flush();

    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode jsonTree = objectMapper.readTree(bos.toByteArray());

    assertNotNull(jsonTree);
    assertTrue(jsonTree.isObject());
    assertEquals("test_rp_2", jsonTree.get("name").asText());
    assertEquals(10, jsonTree.get("parallelism").asInt());
    assertEquals("def", jsonTree.get("defaultPool").asText());
    assertTrue(jsonTree.get("pools").isArray());
    assertEquals(2, jsonTree.get("pools").size());

    JsonNode pool2 = jsonTree.get("pools").get(0);
    assertEquals("pool2", pool2.get("name").asText());
    assertEquals("fcfs", pool2.get("schedulingPolicy").asText());
    assertEquals(7, pool2.get("parallelism").asInt());
    assertEquals(0.7, pool2.get("allocFraction").asDouble(), 0.00001);
    assertTrue(pool2.get("triggers").isArray());
    assertEquals(0, pool2.get("triggers").size());
    assertTrue(pool2.get("mappings").isArray());
    JsonNode type0 = pool2.get("mappings").get(0);
    assertEquals("user", type0.get("type").asText());
    assertTrue(type0.get("values").isArray());
    assertEquals(2, type0.get("values").size());
    HashSet<String> vals = new HashSet<>();
    for (int i = 0; i < type0.get("values").size(); ++i) {
      vals.add(type0.get("values").get(i).asText());
    }
    assertTrue(vals.contains("foo"));
    assertTrue(vals.contains("bar"));

    JsonNode pool1 = jsonTree.get("pools").get(1);
    assertEquals("pool1", pool1.get("name").asText());
    assertEquals("fair", pool1.get("schedulingPolicy").asText());
    assertEquals(3, pool1.get("parallelism").asInt());
    assertEquals(0.3, pool1.get("allocFraction").asDouble(), 0.00001);
    assertTrue(pool1.get("triggers").isArray());
    assertEquals(1, pool1.get("triggers").size());

    JsonNode trigger1 = pool1.get("triggers").get(0);
    assertEquals("trigger1", trigger1.get("name").asText());
    assertEquals("KILL", trigger1.get("action").asText());
    assertEquals("BYTES > 2", trigger1.get("trigger").asText());
  }
}
